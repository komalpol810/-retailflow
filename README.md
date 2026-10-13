# RetailFlow

A data lakehouse and orchestration pipeline built on the Olist Brazilian e-commerce dataset. It demonstrates a full modern data engineering stack: SQL dimensional modeling, a Spark-based medallion architecture, production-style Airflow orchestration, and a cloud deployment on Azure (ADLS Gen2, Databricks, Data Factory, Synapse serverless SQL).

## Table of contents

- [What this demonstrates](#what-this-demonstrates)
- [What was run vs. what was studied](#what-was-run-vs-what-was-studied)
- [Dataset](#dataset)
- [Architecture](#architecture)
- [Data model](#data-model)
- [Tech stack](#tech-stack)
- [Project phases](#project-phases)
- [Repository structure](#repository-structure)
- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [Running it](#running-it)
- [Verifying correctness](#verifying-correctness)
- [Monitoring and alerting](#monitoring-and-alerting)
- [Cloud deployment (Azure)](#cloud-deployment-azure)
- [Incremental processing with Delta Lake (Phase 6)](#incremental-processing-with-delta-lake-phase-6)
- [Cost control](#cost-control)
- [Design decisions and lessons learned](#design-decisions-and-lessons-learned)
- [Troubleshooting](#troubleshooting)
- [Future work](#future-work)

## What this demonstrates

- Designing a dimensional (star schema) data model from a raw relational dataset
- Building a medallion (Bronze, Silver, Gold) lakehouse with PySpark and Delta Lake
- Implementing Slowly Changing Dimension (SCD Type 2) history tracking
- Orchestrating a multi-stage pipeline with Apache Airflow: scheduling, retries with backoff, and Slack failure alerting
- Running the stack containerized with Docker, including a custom Airflow image
- Migrating the lakehouse to Azure: ADLS Gen2 storage, Databricks transformations, Azure Data Factory scheduling, and Synapse serverless SQL views over Gold Delta files
- Designing incremental processing (MERGE, SCD2 merge, Change Data Feed, OPTIMIZE/ZORDER/VACUUM) as a replacement for full refresh
- Controlling cloud cost: budget alerts, terminated clusters, stopped triggers, and full teardown after the build
- Debugging real cross-environment issues (hardcoded paths, network instability, credential and access-mode errors)

## What was run vs. what was studied

| Phase | Status |
|---|---|
| 1 to 4 (local build, Postgres, Spark medallion, Airflow) | Built and run locally |
| 5 (Azure migration) | Built and run on Azure, verified against the local counts, then torn down |
| 6 (incremental processing) | Designed and documented as Databricks PySpark; **not executed**, because the Databricks trial was used up |

Phase 6 code is included as a reference implementation. No timings are claimed for it.

## Dataset

[Olist Brazilian E-Commerce Public Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce): real, anonymized order data from a Brazilian marketplace, spanning customers, orders, order items, products, sellers, payments, reviews, and geolocation, across multiple raw CSV files.

## Architecture

### Local

```
Raw CSVs (Olist dataset)
        |
        v
+-------------------+
|   Bronze layer    |  Raw ingestion, append-only, schema-on-read
+-------------------+
        |
        v
+-------------------+
|   Silver layer    |  Cleaned, validated, deduplicated, typed
+-------------------+
        |
        v
+-------------------+
|   Gold layer      |  dim_customer (SCD2), dim_product, dim_seller,
|                   |  dim_date, fct_order_items
+-------------------+
```

### Azure (Phase 5)

```
Raw CSVs --> ADLS Gen2 (bronze / silver / gold containers)
                  |
                  |  Databricks notebooks (PySpark + Delta)
                  |  triggered daily by Azure Data Factory
                  v
            Gold Delta files in ADLS Gen2
                  |
                  v
   Synapse serverless SQL: 7 views over the Gold Delta files
   (no cluster running, no second copy of the data, pay per data scanned)
```

The local runs do a full refresh: Bronze is append-only, so each run wipes `lakehouse/{bronze,silver,gold}` and re-ingests. Phase 6 describes how to replace this with incremental logic.

## Data model

The local Gold layer is a star schema:

| Table | Type | Notes |
|---|---|---|
| `dim_customer` | Dimension | SCD Type 2: effective-dated rows and a current-record flag |
| `dim_product` | Dimension | Product category, dimensions, etc. |
| `dim_seller` | Dimension | Seller location and identifiers |
| `dim_date` | Dimension | Standard date dimension |
| `fct_order_items` | Fact | Grain: one row per order item; foreign keys to the dimensions, plus price and freight measures |

The Azure Gold layer is exposed through 7 Synapse views, including a `dim_geography` dimension. The fact table's `customer_geography_key` joins to `dim_geography.geography_key` (a role-playing key, so the fact column name differs from the dimension's key name).

Phase 2 built this schema relationally in PostgreSQL first; Phase 3 rebuilt it as a Spark lakehouse.

## Tech stack

| Layer | Tool |
|---|---|
| Language | Python |
| Distributed processing | PySpark 4.2.0 (local), Databricks (cloud) |
| Table format | Delta Lake (delta-spark 4.4.0) |
| Relational modeling | PostgreSQL 16 |
| Orchestration (local) | Apache Airflow 3.3.1 (CeleryExecutor) |
| Orchestration (cloud) | Azure Data Factory |
| Cloud storage | Azure Data Lake Storage Gen2 |
| Cloud SQL serving | Azure Synapse serverless SQL pool |
| Alerting | Slack (Incoming Webhooks) |
| Containerization | Docker / Docker Compose |
| Runtime | OpenJDK 17, Ubuntu |

## Project phases

**Phase 1: Foundations.** Explored the raw Olist dataset and set up the local environment.

**Phase 2: Relational modeling.** Designed and built the star schema in PostgreSQL (`pg_dataeng`, via `docker-compose.yml`).

**Phase 3: Lakehouse rebuild.** Rebuilt the model as a PySpark medallion architecture, chained end to end by `run_pipeline.py`. Verified against known-good counts:
- `fct_order_items`: 112,650 rows
- `dim_customer` (SCD2): 96,355 total rows, 96,096 current, 252 changed customers

**Phase 4: Orchestration.** Rebuilt the pipeline as an Airflow DAG (`retailflow_gold_pipeline`) in a separate Dockerized deployment with a custom image matching the core project's PySpark, Delta and Java versions. Added a daily schedule (`0 6 * * *`, `catchup=False`), retries with backoff (2 retries, 5-minute delay), and Slack alerting on failure through an Airflow Variable (no secrets in code).

**Phase 5: Cloud migration (Azure).** Ported the pipeline to Azure. See [Cloud deployment](#cloud-deployment-azure).

**Phase 6: Incremental processing.** Designed the move from full refresh to incremental Delta logic. See [Incremental processing](#incremental-processing-with-delta-lake-phase-6).

## Repository structure

```
retailflow/
├── data/                        # Raw Olist CSVs (gitignored)
├── spark/                       # Local PySpark ETL scripts
│   ├── 01_bronze_ingest.py
│   ├── 02_silver_transform.py
│   ├── 03_gold_dimensions.py
│   ├── 04_gold_dim_customer_scd2.py
│   └── 05_gold_fct_order_items.py
├── databricks/
│   └── notebooks/               # 00_clean_bronze and 01 to 05, exported as .py
├── azure/
│   ├── adf/                     # Data Factory pipeline and trigger JSON
│   └── synapse/                 # Serverless SQL queries and view definitions
├── docs/
│   └── screenshots/             # Evidence from the Azure run
├── lakehouse/                   # Generated Delta tables (gitignored)
├── run_pipeline.py              # Chains the 5 local Spark scripts
├── docker-compose.yml           # Postgres (pg_dataeng) for Phase 2
├── requirements.txt
└── venv/                        # Python virtual environment (gitignored)

airflow-retailflow/              # Separate Airflow deployment (sibling directory)
├── dags/
│   └── retailflow_pipeline.py   # retailflow_gold_pipeline DAG
├── Dockerfile                   # apache/airflow:3.3.1 + Java 17 + PySpark + Delta
└── docker-compose.yaml          # Airflow services; mounts ~/retailflow at /opt/retailflow
```

## Prerequisites

- Ubuntu (or another Linux distro) with Docker and Docker Compose
- Python 3.x with `venv`
- About 4 GB of free RAM for Spark plus Airflow's Celery stack
- A Slack workspace (optional, for failure alerts)
- An Azure subscription (optional, only to reproduce Phase 5)

## Setup

**1. Core project: Postgres and Python environment**

```bash
cd ~/retailflow
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
docker compose up -d
```

**2. Airflow deployment (separate folder, separate compose file)**

```bash
mkdir -p ~/airflow-retailflow/{dags,logs,plugins,config}
cd ~/airflow-retailflow
curl -LfO https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml
echo "AIRFLOW_UID=$(id -u)" > .env
# edit docker-compose.yaml: comment out the plain `image:` line, uncomment
# `build: .`, and add a volume mount for ~/retailflow:/opt/retailflow
docker compose up airflow-init
docker compose up -d
```

The UI is at http://localhost:8080 (default login `airflow` / `airflow` unless changed).

**3. Slack alerting (optional).** See [Monitoring and alerting](#monitoring-and-alerting).

## Running it

Core pipeline (host, manual run):

```bash
cd ~/retailflow
source venv/bin/activate
python run_pipeline.py
```

Orchestrated with Airflow:

```bash
cd ~/airflow-retailflow
docker compose up -d
# DAG retailflow_gold_pipeline runs daily at 6 AM, or trigger it manually:
docker exec -it airflow-retailflow-airflow-apiserver-1 \
  airflow dags trigger retailflow_gold_pipeline
```

## Verifying correctness

After a run, the Gold layer should match:

| Table | Expected count |
|---|---|
| `fct_order_items` | 112,650 |
| `dim_customer` (total rows, SCD2) | 96,355 |
| `dim_customer` (current rows only) | 96,096 |
| `dim_customer` (changed customers) | 252 |

On Azure, all 7 Synapse view row counts were checked against the Databricks counts and matched exactly. A revenue-by-state query (`azure/synapse/revenue_by_state.sql`) ranked SP first with 47,433 items sold, followed by RJ, MG, RS and PR.

## Monitoring and alerting

The Airflow DAG posts a Slack message when a task fails after its retries are exhausted:

1. Create a Slack app at api.slack.com/apps, enable Incoming Webhooks, and add a webhook to a channel.
2. Store the webhook URL as an Airflow Variable (never hardcoded):
   ```bash
   docker exec -it airflow-retailflow-airflow-apiserver-1 \
     airflow variables set slack_webhook_url "https://hooks.slack.com/services/..."
   ```
3. The DAG's `on_failure_callback` reads the Variable and posts the DAG name, failed task, run timestamp and a link to the task logs.

Retry policy: 2 retries with a 5-minute delay before the callback fires.

## Cloud deployment (Azure)

Phase 5 moved the pipeline to Azure.

| Component | Role |
|---|---|
| ADLS Gen2 | Storage for the Bronze, Silver and Gold layers |
| Databricks | Runs the transformation notebooks (`00_clean_bronze`, `01` to `05`) in PySpark with Delta |
| Azure Data Factory | Runs the pipeline on a daily 6 AM trigger (`tr_daily_6am`) |
| Synapse serverless SQL | Serves the Gold Delta files as 7 SQL views |
| Databricks secret scope | Holds the storage account key, read through `dbutils.secrets.get(...)` so no key is in a notebook |

**Why Synapse serverless.** Databricks transforms the data and storage holds the Gold files. Serverless SQL exposes them as views with no cluster running and no second copy of the data, billed per data scanned. A dedicated pool is provisioned and billed hourly, so it suits heavy, constant dashboard workloads instead.

**Authentication.** The notebooks authenticate to storage with the account key from a secret scope. A production setup would use a service principal or managed identity with RBAC on the storage account instead.

**Roadmap note.** Microsoft is steering new analytics projects toward Microsoft Fabric. This project used the Synapse and Databricks combination because it is still widely deployed.

## Incremental processing with Delta Lake (Phase 6)

> Design and reference code, not executed (see [What was run vs. what was studied](#what-was-run-vs-what-was-studied)).

| Concept | What it does here |
|---|---|
| Delta internals | `_delta_log` records every commit; `DESCRIBE HISTORY` and `VERSION AS OF` give audit and time travel |
| `MERGE INTO` | Upserts on the business key, replacing the full wipe and reload |
| SCD Type 2 merge | A two-copy staging trick closes the old row and inserts the new one in one merge |
| Change Data Feed | Downstream jobs read only the rows that changed |
| Late-arriving data | Merging on the business key makes reloads idempotent |
| `OPTIMIZE` / `ZORDER` / `VACUUM` | Fixes the small-file problem, enables data skipping, and removes old files (default 7-day retention) |

To validate it, the plan is to time a full refresh against an incremental run on the same data and record both numbers. This has not been done, so no performance claim is made.

## Cost control

- Azure usage ran on a Pay-As-You-Go subscription, with a budget alert configured; spend stayed at roughly $20.
- The Databricks cluster was terminated and the ADF trigger stopped after the build.
- Code, notebooks, pipeline JSON and SQL were exported to this repository, then the Azure resource groups were deleted.
- Phases 7 onward run locally at no cost.

## Design decisions and lessons learned

- **Relative paths over absolute paths.** Scripts hardcoded host paths, which broke once the project was mounted at `/opt/retailflow` in a container. All paths are now relative to the project root.
- **Secrets stay out of code.** The Slack webhook lives in an Airflow Variable; the storage key lives in a Databricks secret scope; `.env` is gitignored and was never committed.
- **Retry, then alert.** `on_failure_callback` fires only after retries are exhausted, so one transient blip does not raise a false alarm.
- **Full refresh first, incremental second.** Full refresh is simple to verify against known counts; Phase 6 designs the incremental replacement.
- **Separate Airflow deployment.** `~/airflow-retailflow/` is kept apart from `~/retailflow/docker-compose.yml` to avoid running commands against the wrong compose file.
- **Role-playing dimension key.** `customer_geography_key` in the fact table joins to `dim_geography.geography_key`; the first Synapse query failed on the wrong column name until this was corrected.
- **Serverless over dedicated for serving.** Chosen for cost: nothing runs between queries.
- **Tear down after building.** Cloud resources were deleted once evidence was captured, keeping the project at near-zero ongoing cost.

## Troubleshooting

Real issues hit during development:

- **Large Docker image pulls timing out on home WiFi.** Small HTTPS requests worked, but the roughly 1.5 GB Airflow image pull kept resetting. Switching to a mobile hotspot for the first pull fixed it; once cached, WiFi is fine.
- **DAG appears "missing".** Airflow 3 DAGs start paused and can be buried among built-in examples. Search in the UI or run `airflow dags unpause retailflow_gold_pipeline` inside the apiserver container.
- **File-not-found inside the container but fine on the host.** Almost always a hardcoded absolute path.
- **Slack alert is slow to appear.** With `retries=2` and a 5-minute delay it can take about 10 minutes; set `retries=0` temporarily when testing.
- **Callback output missing from the task log.** In Airflow 3's split-component setup it lands in the worker container logs:
  ```bash
  docker logs airflow-retailflow-airflow-worker-1 --since 15m | grep -i "slack\|callback"
  ```
- **Databricks: single-user cluster check failed.** The cluster's assigned identity was a different user. Fixed by editing the cluster and setting Dedicated access to your own account.
- **Databricks: `fs.azure.account.key` invalid.** The notebook was missing its storage login cell.
- **Databricks: token endpoint "not found".** A service-principal template was filled with one secret reused for all three values. The project actually authenticates with the storage account key from the secret scope.
- **Synapse: `Invalid column name 'geography_key'`.** The fact table column is `customer_geography_key`.

## Future work

- **Phase 7:** data quality checks (Great Expectations), unit tests (pytest), and CI with GitHub Actions
- **Phase 8:** streaming ingestion (local)
- **Phase 9:** dashboard, Spark tuning, and final documentation
- Run the Phase 6 incremental logic and record measured full-refresh vs incremental timings
- Evaluate Microsoft Fabric as the cloud target
