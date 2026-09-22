# RetailFlow

A data lakehouse and orchestration pipeline built on the Olist Brazilian
e-commerce dataset, demonstrating a full modern data engineering stack: SQL
dimensional modeling, a Spark-based medallion architecture, and
production-style Airflow orchestration with retries and failure alerting.

## Table of contents

- [What this demonstrates](#what-this-demonstrates)
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
- [Monitoring & alerting](#monitoring--alerting)
- [Design decisions & lessons learned](#design-decisions--lessons-learned)
- [Troubleshooting](#troubleshooting)
- [Future work](#future-work)

## What this demonstrates

- Designing a dimensional (star schema) data model from a raw relational dataset
- Building a medallion (Bronze → Silver → Gold) lakehouse with PySpark and Delta Lake
- Implementing Slowly Changing Dimension (SCD Type 2) history tracking
- Orchestrating a multi-stage pipeline with Apache Airflow: scheduling,
  retry policies with backoff, and Slack-based failure alerting
- Running the whole stack containerized with Docker, including a custom
  Airflow image carrying project-specific dependencies
- Debugging real cross-environment issues (hardcoded paths, network
  instability during image pulls, orchestration-specific gotchas)

## Dataset

[Olist Brazilian E-Commerce Public Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) —
real, anonymized order data from a Brazilian marketplace, spanning
customers, orders, order items, products, sellers, payments, reviews, and
geolocation, across multiple raw CSV files.

## Architecture

```
Raw CSVs (Olist dataset)
        |
        v
+-------------------+
|   Bronze layer     |  Raw ingestion, append-only, schema-on-read
+-------------------+
        |
        v
+-------------------+
|   Silver layer      |  Cleaned, validated, deduplicated, typed
+-------------------+
        |
        v
+-------------------+
|   Gold layer         |  Dimensional model: dim_customer (SCD2),
|                       |  dim_product, dim_seller, dim_date,
|                       |  fct_order_items
+-------------------+
```

Every run does a **full refresh**: Bronze is intentionally append-only, so
each pipeline run starts by wiping `lakehouse/{bronze,silver,gold}` clean
before re-ingesting from the raw CSVs. There's no incremental/CDC logic yet
— see [Future work](#future-work).

## Data model

The Gold layer implements a standard **star schema**:

| Table | Type | Notes |
|---|---|---|
| `dim_customer` | Dimension | **SCD Type 2** — tracks customer attribute history with effective-dated rows and a current-record flag |
| `dim_product` | Dimension | Product category, dimensions, etc. |
| `dim_seller` | Dimension | Seller location and identifiers |
| `dim_date` | Dimension | Standard date dimension for time-based analysis |
| `fct_order_items` | Fact | Grain: one row per order item; foreign keys to all dimensions above, plus price/freight measures |

Phase 2 (PostgreSQL) established this same star schema relationally first,
before Phase 3 rebuilt it as a Spark-based lakehouse.

## Tech stack

| Layer | Tool |
|---|---|
| Language | Python |
| Distributed processing | PySpark 4.2.0 |
| Table format | Delta Lake (delta-spark 4.4.0) |
| Relational modeling | PostgreSQL 16 |
| Orchestration | Apache Airflow 3.3.1 (CeleryExecutor) |
| Alerting | Slack (Incoming Webhooks) |
| Containerization | Docker / Docker Compose |
| Runtime | OpenJDK 17, Ubuntu |

## Project phases

- **Phase 1 — Foundations.** Initial exploration of the raw Olist dataset
  and local environment setup.
- **Phase 2 — Relational modeling.** Designed and built the star schema
  above in PostgreSQL (`pg_dataeng`, via `docker-compose.yml` in this repo).
- **Phase 3 — Lakehouse rebuild.** Rebuilt the same model as a PySpark
  medallion architecture (Bronze → Silver → Gold), chained end-to-end via
  `run_pipeline.py`. Verified against known-good row counts:
  - `fct_order_items`: 112,650 rows
  - `dim_customer` (SCD2): 96,355 total rows / 96,096 current / 252 changed customers
  - Parquet-vs-CSV storage comparison explicitly scoped out.
- **Phase 4 — Orchestration.** Rebuilt the pipeline as an Airflow DAG
  (`retailflow_gold_pipeline`) in a separate Dockerized Airflow deployment,
  using a custom image with the same PySpark/Delta/Java versions as the
  core project. Added:
  - A daily schedule (`0 6 * * *`, `catchup=False`)
  - Retries with backoff (2 retries, 5-minute delay) for transient failures
  - Slack alerting on task failure, via an Airflow Variable-stored webhook
    (no secrets in code)
- **Phase 5 — Cloud (in progress).** Migrating to Azure (ADLS Gen2 /
  Databricks / ADF / Synapse).

## Repository structure

```
retailflow/                   # Core project (this repo)
├── data/                     # Raw Olist CSVs
├── spark/                    # PySpark ETL scripts
│   ├── 01_bronze_ingest.py
│   ├── 02_silver_transform.py
│   ├── 03_gold_dimensions.py
│   ├── 04_gold_dim_customer_scd2.py
│   └── 05_gold_fct_order_items.py
├── lakehouse/                 # Bronze / Silver / Gold Delta tables (generated, gitignored)
├── run_pipeline.py            # Chains all 5 spark scripts end-to-end (host-run)
├── docker-compose.yml         # Postgres (pg_dataeng) for Phase 2
└── venv/                      # Python virtual environment

airflow-retailflow/            # Separate Airflow deployment (sibling directory,
│                                deliberately kept apart from retailflow/'s own
│                                docker-compose.yml to avoid conflating the two)
├── dags/
│   └── retailflow_pipeline.py    # retailflow_gold_pipeline DAG definition
├── Dockerfile                 # Extends apache/airflow:3.3.1 with Java 17 + PySpark + Delta
└── docker-compose.yaml        # Airflow services (apiserver, scheduler,
                                 # dag-processor, worker, triggerer, postgres,
                                 # redis); mounts ~/retailflow into containers
                                 # at /opt/retailflow
```

## Prerequisites

- Ubuntu (or another Linux distro) with Docker and Docker Compose installed
- Python 3.x with `venv`
- ~4 GB+ free RAM for Spark + Airflow's Celery stack running concurrently
- A Slack workspace (only needed for failure-alerting; optional otherwise)

## Setup

**1. Core project — Postgres + Python environment**
```bash
cd ~/retailflow
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt   # pyspark==4.2.0, delta-spark==4.4.0, psycopg2, etc.
docker compose up -d              # starts Postgres (pg_dataeng)
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

UI available at `http://localhost:8080` (default login `airflow` / `airflow`
unless changed).

**3. (Optional) Slack alerting**

See [Monitoring & alerting](#monitoring--alerting) below.

## Running it

**Core pipeline (host, manual run):**
```bash
cd ~/retailflow
source venv/bin/activate
python run_pipeline.py
```

**Orchestrated (Airflow):**
```bash
cd ~/airflow-retailflow
docker compose up -d
# DAG retailflow_gold_pipeline runs daily at 6 AM automatically, or trigger manually:
docker exec -it airflow-retailflow-airflow-apiserver-1 \
  airflow dags trigger retailflow_gold_pipeline
```

## Verifying correctness

After a run, row counts in the Gold layer should match:

| Table | Expected count |
|---|---|
| `fct_order_items` | 112,650 |
| `dim_customer` (total rows, SCD2) | 96,355 |
| `dim_customer` (current rows only) | 96,096 |
| `dim_customer` (changed customers) | 252 |

## Monitoring & alerting

The Airflow DAG posts a Slack message whenever a task fails (after retries
are exhausted, not on every individual attempt):

1. Create a Slack app at [api.slack.com/apps](https://api.slack.com/apps) →
   enable **Incoming Webhooks** → add a webhook to your chosen channel.
2. Store the webhook URL as an Airflow Variable (never hardcoded in the DAG):
   ```bash
   docker exec -it airflow-retailflow-airflow-apiserver-1 \
     airflow variables set slack_webhook_url "https://hooks.slack.com/services/..."
   ```
3. The DAG's `on_failure_callback` reads this Variable and posts a message
   with the DAG name, failed task name, run timestamp, and a direct link to
   that task's logs.

Retry policy: 2 retries, 5-minute delay, before a task is considered truly
failed and the Slack callback fires.

## Design decisions & lessons learned

- **Relative paths over absolute paths.** ETL scripts originally hardcoded
  absolute host paths (`/home/.../retailflow/data`), which broke once the
  project was mounted into a container at a different path (`/opt/retailflow`).
  Fixed by switching all path constants to relative paths, since both the
  host runner and the Airflow container `cd` into the project root before
  running each script — a genuine portability fix, not just an
  Airflow-specific workaround.
- **Secrets in Airflow Variables, not code.** The Slack webhook URL is
  stored as an Airflow Variable and read at runtime via `Variable.get()`,
  never hardcoded in the DAG file.
- **Retry-then-alert, not alert-on-every-attempt.** `on_failure_callback`
  only fires after all retries are exhausted, so a single transient blip
  doesn't trigger a false alarm — only a genuinely failed task does.
- **Full refresh, not incremental (for now).** Given Bronze is append-only
  by design, each run wipes and rebuilds the lakehouse from scratch.
  Simpler to reason about and verify correctness against known row counts,
  at the cost of reprocessing everything each run.
- **Separate Airflow deployment, separate compose file.** Kept
  `~/airflow-retailflow/` deliberately apart from `~/retailflow/`'s own
  `docker-compose.yml` (Postgres) to avoid the two `docker compose` files
  being conflated — running commands from the wrong directory silently
  grabs the wrong compose file and produces confusing "service not running"
  errors.

## Troubleshooting

Real issues hit during development, kept here for reference:

- **Large Docker image pulls timing out on home WiFi.** Small HTTPS
  requests worked fine, but the ~1.5 GB combined Airflow image pull
  consistently timed out or reset mid-transfer. IPv6 and Docker daemon
  config were ruled out; root cause was the home network choking on
  sustained large transfers specifically. Fixed by switching to a mobile
  hotspot for the initial image pull only — once cached locally, regular
  WiFi is fine for everyday use.
- **DAG appears "missing" after being added.** Airflow 3 DAGs start
  **paused** by default and can get buried among ~100 built-in example
  DAGs. Use the search box in the UI, or:
  ```bash
  docker exec -it airflow-retailflow-airflow-apiserver-1 \
    airflow dags unpause retailflow_gold_pipeline
  ```
- **Task fails with a file-not-found error inside the container but works
  on the host.** Almost always a hardcoded absolute path — check that
  path constants are relative to the project root (see Design decisions
  above).
- **Slack alert doesn't show up right after a failure.**
  `on_failure_callback` fires only once retries are exhausted — with
  `retries=2` and a 5-minute delay, that can be ~10 minutes after the
  first failed attempt. For faster testing, temporarily set `retries=0`.
- **Looking for task/callback output and it's not in the per-task UI log.**
  In Airflow 3's split-component `CeleryExecutor` setup, task execution and
  callback output land in the **worker** container's own logs, not always
  the per-task log file:
  ```bash
  docker logs airflow-retailflow-airflow-worker-1 --since 15m | grep -i "slack\|callback"
  ```

## Future work

- Incremental/CDC ingestion instead of full refresh
- Migrate storage and compute to Azure (ADLS Gen2, Databricks, ADF,
  Synapse) — Phase 5
- Data quality checks (e.g. Great Expectations) as an explicit pipeline
  stage
- CI checks (lint, DAG import-error check) on push
