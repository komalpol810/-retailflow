# RetailFlow

A data lakehouse and orchestration pipeline built on the Olist Brazilian e-commerce dataset. It demonstrates a full modern data engineering stack: SQL dimensional modeling, a Spark-based medallion architecture, production-style Airflow orchestration, a cloud deployment on Azure (ADLS Gen2, Databricks, Data Factory, Synapse serverless SQL), and incremental processing patterns with Delta Lake.

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
- Implementing and testing incremental Delta Lake patterns: MERGE, a single-statement SCD2 merge, Change Data Feed, handling of late-arriving (out-of-order) dimension changes, and OPTIMIZE / ZORDER / VACUUM
- Controlling cloud cost: budget alerts, terminated clusters, stopped triggers, and full teardown after the build
- Debugging real cross-environment issues (hardcoded paths, network instability, credential and access-mode errors, SQL-on-path limits in Delta)

## What was run vs. what was studied

| Phase | Status |
|---|---|
| 1 to 4 (local build, Postgres, Spark medallion, Airflow) | Built and run locally |
| 5 (Azure migration) | Built and run on Azure, verified against the local counts, then torn down |
| 6 (incremental processing) | Built and run locally with PySpark and Delta Lake, as standalone demo scripts. The Databricks trial was used up, so this phase did not run on Databricks. Not yet wired into `run_pipeline.py` or Airflow |

The Phase 6 results below are row counts, file counts and version histories from real runs. No timings are claimed: a measured full refresh vs incremental comparison has not been done.

## Dataset

Olist Brazilian E-Commerce Public Dataset: real, anonymized order data from a Brazilian marketplace, spanning customers, orders, order items, products, sellers, payments, reviews, and geolocation, across multiple raw CSV files.

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

The local pipeline runs do a full refresh: Bronze is append-only, so each run wipes `lakehouse/{bronze,silver,gold}` and re-ingests. Phase 6 demonstrates the incremental logic that could replace this.

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

**Phase 6: Incremental processing.** Built and tested incremental Delta Lake patterns locally: MERGE, SCD2 merge, Change Data Feed, late-arriving data, and file maintenance (OPTIMIZE / ZORDER / VACUUM). See [Incremental processing](#incremental-processing-with-delta-lake-phase-6).

## Repository structure

```
retailflow/
├── data/                        # Raw Olist CSVs (gitignored)
├── spark/                       # Local PySpark ETL scripts and Phase 6 demos
│   ├── 01_bronze_ingest.py
│   ├── 02_silver_transform.py
│   ├── 03_gold_dimensions.py
│   ├── 04_gold_dim_customer_scd2.py
│   ├── 05_gold_fct_order_items.py
│   ├── 06_merge_demo.py                    # MERGE INTO, duplicate-key failure, de-duplication
│   ├── 07_inspect_dim_customer.py          # Inspect dim_customer before the SCD2 merge
│   ├── 07_scd2_merge_demo.py               # One-statement SCD2 merge, idempotent re-run
│   ├── 08_change_data_feed_demo.py         # Change Data Feed
│   ├── 09_late_arriving_demo.py            # Out-of-order SCD2 change: naive vs fixed logic
│   ├── 10_optimize_zorder_vacuum_demo.py   # Small files, OPTIMIZE, ZORDER, VACUUM
│   └── 10b_time_travel_after_vacuum.py     # Time travel after VACUUM
├── databricks/
│   └── notebooks/               # 00_clean_bronze and 01 to 05, exported as .py
├── azure/
│   ├── adf/                     # Data Factory pipeline and trigger JSON
│   └── synapse/                 # Serverless SQL queries and view definitions
├── docs/
│   └── screenshots/             # Evidence from the Azure run
├── lakehouse/                   # Generated Delta tables (gitignored)
├── scratch/                     # Generated Delta tables for the Phase 6 demos
├── run_pipeline.py              # Chains the 5 local Spark scripts (full refresh)
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

`run_pipeline.py` includes a guard (`check_lakehouse_is_clean`) that checks the lakehouse is empty before ingesting. Bronze is append-only, so running the pipeline on top of existing data multiplies the rows.

Phase 6 demos (each is a standalone script, run from the project root):

```bash
python spark/07_scd2_merge_demo.py
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

Built and run locally with PySpark and delta-spark 4.4.0 after the Databricks trial credits ran out. The demos are standalone scripts in `spark/`; the production-style pipeline (`run_pipeline.py`) is still a full refresh.

| Milestone | What was built | Measured result |
|---|---|---|
| Delta internals | UPDATE, DELETE and RESTORE on the fact table, read back through the transaction log | Each operation created a new table version; row count went 112,650, then 112,267, then back to 112,650 after RESTORE |
| MERGE INTO | Upsert on the business key (`06_merge_demo.py`) | A batch containing a duplicate key made the merge fail with nothing committed. After de-duplicating the batch, 2 rows were inserted and 3 updated (99,443 rows) |
| SCD Type 2 merge | One atomic MERGE that closes the old row and inserts the new one (`07_scd2_merge_demo.py`) | Re-running the same batch changed nothing; every customer kept exactly one current row |
| Change Data Feed | Row-level change records for downstream consumers (`08_change_data_feed_demo.py`) | 2 update preimages, 2 update postimages, 1 delete and 1 insert, as expected |
| Late-arriving data | An older change arriving after a newer one (`09_late_arriving_demo.py`) | The naive logic produced an inverted date range; the fixed logic produced a clean history and was idempotent (details below) |
| OPTIMIZE / ZORDER / VACUUM | Small-file compaction, data skipping and file cleanup (`10_...`, `10b_...`) | 100 files compacted to 34, then ZORDER to 24; a lookup opened 34 of 34 files before ZORDER and 1 of 24 after; VACUUM deleted 133 files (details below) |

### How the SCD2 merge works

Each customer who moved goes into the MERGE source twice. One copy carries the real customer key and matches the current row, which closes it (sets `valid_to` and `is_current = false`). The other copy carries a NULL key, which never matches, so it falls into the insert branch and becomes the new current row. Both actions happen in a single atomic commit. Idempotency comes from comparing a hash of the tracked columns (`row_hash`) against the current row: a batch that changes nothing produces no action.

### Late-arriving data

The first SCD2 logic assumed changes arrive in time order. The test picked a customer with two versions (Cajamar, then Sao Paulo from 2018-12-01) and sent a change dated 2018-08-20, older than the current version.

- **Naive logic** (compare with the current row only): the late address became the current row, and the real current row was closed with a `valid_to` earlier than its own `valid_from`. A health check over the table reported 1 inverted date range and 2 rows not linked to the previous version.
- **Fixed logic:** find the version that was valid at the change's date and split it in two. The first piece ends at the change date; the second starts there and inherits the old end date and current flag. Result: three rows, each ending where the next begins, with one current row. The health check reported 0 problems, and re-running the same batch did nothing.
- **Known gap:** fact rows for orders placed after the late change date still point at the old dimension key. A real pipeline would need to re-point them.

### File maintenance

The demo table was written as 100 small files (112,650 rows). The maximum file size was lowered to 300 KB for the demo so that compaction produces more than one file.

- **OPTIMIZE** compacted the table to 34 live files, but the parquet files on disk went up to 133, because the old files are kept for time travel.
- **ZORDER BY customer_key** rewrote the table to 24 files. A lookup of one `customer_key` had to open 34 of 34 files before and 1 of 24 after (data skipping). Disk held 157 files.
- **VACUUM** with a 0-hour retention (safety check disabled for the demo only; the default is 7 days) deleted 133 files, about 15 MB, leaving the 24 live files.
- **Time travel after VACUUM:** reading version 0 failed, because its files were gone, while the current version read fine.
- **Gotcha:** a plain `count()` on the vacuumed old version still succeeded, because Delta answers row counts from statistics in the transaction log without opening data files. The check that confirmed the failure reads actual column data.

## Cost control

- Azure usage ran on a Pay-As-You-Go subscription, with a budget alert configured; spend stayed at roughly $20.
- The Databricks cluster was terminated and the ADF trigger stopped after the build.
- Code, notebooks, pipeline JSON and SQL were exported to this repository, then the Azure resource groups were deleted.
- Phase 6 onward runs locally at no cost.

## Design decisions and lessons learned

- **Relative paths over absolute paths.** Scripts hardcoded host paths, which broke once the project was mounted at `/opt/retailflow` in a container. All paths are now relative to the project root.
- **Secrets stay out of code.** The Slack webhook lives in an Airflow Variable; the storage key lives in a Databricks secret scope; `.env` is gitignored and was never committed.
- **Retry, then alert.** `on_failure_callback` fires only after retries are exhausted, so one transient blip does not raise a false alarm.
- **Full refresh first, incremental second.** Full refresh is simple to verify against known counts, so it came first; Phase 6 builds and tests the incremental patterns separately.
- **Guard against re-running on existing data.** Bronze is append-only, so a pipeline run on top of an existing lakehouse produced 1,789,384 fact rows instead of 112,650 and 4,784 missing geography keys instead of 302. `run_pipeline.py` now checks that the lakehouse is clean before it starts.
- **De-duplicate before MERGE.** A source batch with two rows for the same key makes the merge fail, so the batch is reduced to one row per key first.
- **Do not trust history logic that assumes ordered arrival.** The late-arriving test broke the first SCD2 implementation; the fix looks up the version valid at the change date instead of only the current row.
- **Verify with a query that reads data.** A row count can be answered from Delta's log statistics, so it can look healthy while data files are missing.
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
- **Local: `lakehouse/gold` missing, then doubled row counts.** The first demo failed because the Gold folder did not exist. Rebuilding on top of old Bronze data multiplied the counts because Bronze is append-only; wiping `lakehouse/` and rebuilding restored 112,650 rows.
- **Delta: SQL `UPDATE` or `ALTER TABLE` against a path fails.** Use the `DeltaTable` Python API for updates and deletes by path, and set the Change Data Feed through a session default (`spark.databricks.delta.properties.defaults.enableChangeDataFeed`) so new tables have it from version 0.
- **Delta: MERGE fails with no rows committed.** The source batch contained a duplicate key; de-duplicate it first.

## Future work

- Phase 7: data quality checks (Great Expectations), unit tests (pytest), and CI with GitHub Actions
- Phase 8: streaming ingestion (local)
- Phase 9: dashboard, Spark tuning, and final documentation
- Wire the incremental logic into the pipeline and record measured full-refresh vs incremental timings
- Re-point fact rows to the correct dimension key after a late-arriving SCD2 change
- Evaluate Microsoft Fabric as the cloud target
