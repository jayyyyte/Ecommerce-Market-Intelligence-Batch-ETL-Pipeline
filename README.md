# Batch ETL Pipeline — E-Commerce Market Analysis

> Automated daily pipeline that extracts product data from e-commerce sources, cleans and validates it, stores it in PostgreSQL, and exposes market insights through a Metabase dashboard. Orchestrated by Apache Airflow with full retry, deduplication, and alerting support.

---

## Features

- **Automated daily ingestion** — Airflow DAG runs at 00:00 UTC every day; no manual trigger required
- **Idempotent UPSERT** — re-running any DAG execution date produces exactly the same DB state; zero duplicate rows
- **Retry with exponential backoff** — handles network timeouts (30s → 60s → 120s + jitter) transparently
- **Full audit trail** — every rejected row is recorded in `rejected_records` with a reason code; every run logged in `pipeline_runs`
- **Data quality scoring** — per-run `rows_loaded / rows_extracted` score stored and surfaced in dashboards
- **Schema drift detection** — Pandera validation halts the pipeline and alerts if the source adds or removes columns
- **Slack notifications** — success and failure summaries posted at end of every run
- **Backfill support** — reprocess any historical date range with a single Airflow CLI command
- **Fully containerised** — one `docker compose up` brings the entire stack live

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.11 |
| Orchestration | Apache Airflow 2.7.3 (LocalExecutor) |
| Data processing | Pandas 2.2, (PySpark optional) |
| Database | PostgreSQL 15 |
| ORM / DB access | SQLAlchemy 2.0, psycopg2-binary |
| Schema validation | Pandera 0.20 |
| HTTP client | requests 2.31 |
| Alerting | Slack Incoming Webhooks |
| Visualization | Metabase (self-hosted) |
| Containerisation | Docker + Docker Compose |
| Logging | loguru |
| Testing | pytest, pytest-mock, responses |

---

## Project Structure

```
ecommerce-etl/
│
├── dags/                            # Airflow DAG definitions
│   ├── ecommerce_market_dag.py      # Main production DAG (Sprint 3)
│   └── hello_world_dag.py           # Phase 1 smoke-test DAG
│
├── etl/                             # Core ETL logic (importable Python package)
│   ├── __init__.py
│   ├── config.py                    # Central config (Airflow Vars → env → defaults)
│   ├── extractor.py                 # Data source connectors + retry logic
│   ├── transformer.py               # Pandas cleaning pipeline + schema validation
│   ├── loader.py                    # PostgreSQL UPSERT + pipeline run logging
│   └── notifier.py                  # Slack success/failure notifications
│
├── scripts/                         # One-time setup and admin scripts
│   ├── init_db.py                   # Creates schema in ecommerce_db
│   └── setup_airflow_connections.py # Creates Airflow Connection + Variables
│
├── sql/                             # SQL files
│   ├── 00_init_databases.sql        # Creates airflow_db + metabaseappdb (auto-run by Docker)
│   ├── create_tables.sql            # DDL for 3 pipeline tables + 9 indexes
│   └── queries/                     # Analysis queries for Metabase / ad-hoc
│
├── config/
│   └── schema_spec.yaml             # Pandera schema spec — expected columns and types
│
├── tests/                           # pytest test suite
│   ├── test_extractor.py
│   ├── test_transformer.py
│   ├── test_loader.py
│   └── test_integration.py
│
├── docs/                            # Module-level documentation
│   ├── modules/                     # config, extractor, transformer, loader, notifier
│   ├── scripts/                     # init_db, setup_airflow_connections
│   ├── dags/                        # hello_world_dag
│   └── sql/                         # schema
│
├── staging/                         # Raw extracted files (git-ignored)
├── archive/                         # Archived raw files by date (git-ignored)
├── logs/                            # Airflow logs (git-ignored)
│
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── .env.example                     # Template — copy to .env and fill in secrets
└── README.md
```

---

## Architecture

### Design Pattern
Modular **Batch ETL** with staged responsibilities. Each module has a single job; modules communicate via Airflow XCom and the filesystem (staging directory), not direct function calls across DAG boundaries.

### Flow
```
[FakeStoreAPI / Tiki]
        │
        ▼
  [Extractor]          Python · requests · BeautifulSoup
        │  raw JSON
        ▼
  [Staging Area]       staging/{date}/raw_*.json
        │
        ▼
  [Transformer]        Pandas — clean, validate, deduplicate
        │  clean DataFrame
        ▼
  [Loader]             SQLAlchemy — UPSERT into PostgreSQL
        │
        ▼
  [PostgreSQL]         products_market · pipeline_runs · rejected_records
        │
        ▼
  [Metabase]           Dashboards for analysts & stakeholders

All steps orchestrated and monitored by Apache Airflow (DAG: ecommerce_market_etl)
```

### Component Interaction

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Apache Airflow (Scheduler + Webserver)        │
│                                                                       │
│  DAG: ecommerce_market_etl  (daily @ 00:00 UTC)                      │
│                                                                       │
│  [extract_data] → [validate_schema] → [transform_data]               │
│                                             │                         │
│                                      [load_to_db] → [notify_status]  │
└─────────────────────────────────────────────────────────────────────┘
        │                                    │              │
        ▼                                    ▼              ▼
 ┌─────────────┐                    ┌──────────────┐  ┌──────────────┐
 │  Data Source │                   │  PostgreSQL   │  │    Slack     │
 │  (API/Web)   │                   │  ecommerce_db │  │  Webhook     │
 └─────────────┘                    └──────────────┘  └──────────────┘
        │                                    │
        ▼                                    ▼
 staging/{date}/                    ┌──────────────────────┐
 raw_products.json                  │  Metabase Dashboard  │
                                    │  localhost:3000       │
                                    └──────────────────────┘
```

### Key Design Decisions

| Decision | Choice | Reason |
|---|---|---|
| Executor | LocalExecutor | Simpler Docker setup; CeleryExecutor when scaling needed |
| Deduplication | `ON CONFLICT DO UPDATE` (UPSERT) | DB-level guarantee; no application-level diff needed |
| Secret management | Airflow Variables + Connections | Industry standard; auditable; no secrets in code |
| Raw data persistence | JSON files in `staging/` | Enables re-processing without re-fetching; cheap debugging |
| Idempotency scope | `(product_id, source, extraction_date)` | Day-level granularity matches the batch cadence |

---

## Setup & Installation

### Prerequisites

- Docker Desktop (or Docker Engine + Compose plugin)
- Git
- 4 GB RAM available for Docker

### 1. Clone the repository

```bash
git clone https://github.com/your-org/ecommerce-etl.git
cd ecommerce-etl
```

### 2. Configure environment variables

```bash
cp .env.example .env
```

Open `.env` and fill in:

```bash
# Generate a Fernet key:
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

AIRFLOW__CORE__FERNET_KEY=<paste generated key>
AIRFLOW__WEBSERVER__SECRET_KEY=<any random string>
SLACK_WEBHOOK_URL=<your Slack webhook URL, or leave empty>
```

### 3. Start the stack

```bash
# First run — builds the image and initialises everything
docker compose up airflow-init

# Start all services
docker compose up -d

# Watch logs until scheduler is ready (usually 60–90 seconds)
docker compose logs -f airflow-scheduler
```

### 4. Verify

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8080 | `admin / admin123` |
| Metabase | http://localhost:3000 | Set up on first visit |
| PostgreSQL | `localhost:5432` | `etl_user / etl_password` |

### 5. Run the smoke-test DAG

In the Airflow UI: **DAGs → `hello_world_etl_check` → Toggle ON → ▶ Trigger**

All 5 tasks should turn green. If any fail, see the [troubleshooting table](#troubleshooting) below.

---

## Usage

### Trigger a manual pipeline run

```bash
# Via Airflow UI: DAGs → ecommerce_market_etl → ▶ Trigger
# Via CLI:
docker exec airflow_scheduler \
    airflow dags trigger ecommerce_market_etl
```

### Backfill historical dates

```bash
docker exec airflow_scheduler \
    airflow dags backfill \
    --start-date 2025-06-01 \
    --end-date 2025-06-14 \
    ecommerce_market_etl
```

### Query the data

```bash
# Open psql
docker exec -it ecommerce_postgres psql -U etl_user -d ecommerce_db

# Example queries
SELECT category, AVG(price)::NUMERIC(10,2) AS avg_price
FROM products_market
GROUP BY category
ORDER BY avg_price DESC;

SELECT * FROM pipeline_runs ORDER BY started_at DESC LIMIT 5;

SELECT rejection_reason, COUNT(*) FROM rejected_records GROUP BY rejection_reason;
```

---

## Modules

| Module | File | Description |
|---|---|---|
| **Config** | `etl/config.py` | Single source of truth for all config; 3-level priority chain |
| **Extractor** | `etl/extractor.py` | Fetches raw data with pagination, rate limiting, and retry backoff |
| **Transformer** | `etl/transformer.py` | 7-step cleaning pipeline; Pandera validation; rejected row tracking |
| **Loader** | `etl/loader.py` | Transactional UPSERT to PostgreSQL; pipeline run audit logging |
| **Notifier** | `etl/notifier.py` | Slack success/failure notifications; quality score alerts |

See `docs/modules/` for full documentation of each module.

---

## Data Flow

```
1. EXTRACT
   FakeStoreExtractor / TikiScraper
   → paginate source → @retry_with_backoff on failures
   → enrich rows (source, extraction_timestamp)
   → write atomically to staging/{date}/raw_products.json

2. VALIDATE
   Pandera schema check on raw DataFrame
   → halt pipeline if columns are missing or types are wrong (schema drift)

3. TRANSFORM
   DataTransformer (7 steps)
   → drop nulls → normalize types → validate ranges → fill defaults
   → collect rejected rows with reason codes
   → output: clean_df + rejected_df + quality_score

4. LOAD
   DataLoader (single transaction)
   → UPSERT clean_df → products_market (ON CONFLICT DO UPDATE)
   → bulk insert rejected_df → rejected_records
   → UPDATE pipeline_runs row with final counts and status
   → archive raw file to archive/{YYYY}/{MM}/{DD}/

5. NOTIFY
   PipelineNotifier (always runs via TriggerRule.ALL_DONE)
   → post SUCCESS or FAILURE summary to Slack
   → include run_id, row counts, quality score, duration
```

---

## Environment Variables

See `.env.example` for the full list. Key variables:

| Variable | Required | Description |
|---|---|---|
| `POSTGRES_USER` | ✅ | PostgreSQL username |
| `POSTGRES_PASSWORD` | ✅ | PostgreSQL password |
| `POSTGRES_DB` | ✅ | Main application database name |
| `AIRFLOW_DB` | ✅ | Airflow metadata database name |
| `AIRFLOW__CORE__FERNET_KEY` | ✅ | Fernet key for Airflow secret encryption |
| `AIRFLOW__WEBSERVER__SECRET_KEY` | ✅ | Flask session secret for Airflow webserver |
| `DATA_SOURCE_URL` | ✅ | API endpoint or scraping target URL |
| `STAGING_DIR` | ✅ | Path where raw files are written |
| `SLACK_WEBHOOK_URL` | ⬜ | Slack webhook (empty = notifications disabled) |
| `PIPELINE_ENV` | ⬜ | `dev` / `staging` / `prod` (default: `dev`) |
| `MAX_RETRIES` | ⬜ | HTTP retry attempts (default: `3`) |
| `ARCHIVE_RETENTION_DAYS` | ⬜ | Days to keep archived raw files (default: `90`) |

---

## Scripts / Commands

```bash
# ── Docker ────────────────────────────────────────────────────────────────────
docker compose up -d                    # Start all services
docker compose down                     # Stop all services (data preserved)
docker compose down -v                  # Full reset (destroys DB volume)
docker compose logs -f airflow-scheduler

# ── Database ──────────────────────────────────────────────────────────────────
docker exec airflow_scheduler python /opt/airflow/scripts/init_db.py
docker exec -it ecommerce_postgres psql -U etl_user -d ecommerce_db

# ── Airflow setup ─────────────────────────────────────────────────────────────
docker exec airflow_scheduler python /opt/airflow/scripts/setup_airflow_connections.py
docker exec airflow_scheduler python /opt/airflow/scripts/setup_airflow_connections.py --overwrite

# ── Pipeline ──────────────────────────────────────────────────────────────────
docker exec airflow_scheduler airflow dags trigger ecommerce_market_etl
docker exec airflow_scheduler airflow dags backfill -s 2025-06-01 -e 2025-06-14 ecommerce_market_etl

# ── Testing ───────────────────────────────────────────────────────────────────
docker exec airflow_scheduler pytest /opt/airflow/tests/ -v
docker exec airflow_scheduler pytest /opt/airflow/tests/ --cov=etl --cov-report=html
docker exec airflow_scheduler flake8 /opt/airflow/etl/ --max-line-length=120

# ── Fernet key generation ─────────────────────────────────────────────────────
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `check_db_connection` task fails with "table not found" | `init_db.py` didn't run | `docker exec airflow_scheduler python /opt/airflow/scripts/init_db.py` |
| `check_airflow_variables` fails | Variables not created | `docker exec airflow_scheduler python /opt/airflow/scripts/setup_airflow_connections.py` |
| Metabase can't connect to DB | `metabaseappdb` not created | Run `docker compose down -v && docker compose up -d` to re-trigger `00_init_databases.sql` |
| Airflow UI shows no DAGs | `dags/` volume not mounted | Check `volumes` in `docker-compose.yml` |
| `UPSERT` creates duplicates | Missing UNIQUE index | Re-run `init_db.py` — index creation is idempotent |
| Scheduler says "Fernet key invalid" | `.env` has placeholder key | Generate a real Fernet key and restart |

---

## Notes

- **Data source:** `FakeStoreAPI` is used by default (free, stable, no auth). Swap `DATA_SOURCE_URL` and the extractor class for Tiki/Shopee in production.
- **LocalExecutor limitation:** Tasks run sequentially within a DAG run. If the pipeline needs true parallel task execution, switch to `CeleryExecutor` and add Redis to the compose stack.
- **PySpark:** Not enabled by default. To demonstrate scalability, add `pyspark==3.5.*` to `requirements.txt` and create a `SparkDataTransformer` subclass alongside the Pandas one.
- **Metabase auto-refresh:** Set dashboard refresh to 24h in Metabase settings — it does not auto-update in real time.
- **`airflow-init` is a one-shot container** — it exits after setup. That is expected behaviour; do not restart it.