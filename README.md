# Batch ETL Pipeline — E-Commerce Market Analysis

Automated pipeline that collects, cleans, and stores e-commerce product data
for market analysis. Runs daily via Apache Airflow, stores results in
PostgreSQL, and exposes insights through a Metabase dashboard.

**Stack:** Python 3.11 · Apache Airflow · PostgreSQL · Metabase · Docker

---

## Architecture

```
[FakeStoreAPI / Tiki]
        |
        v
  [Extractor]          Python, requests, BeautifulSoup
        |  raw JSON
        v
  [Staging Area]       staging\{date}\raw_*.json
        |
        v
  [Transformer]        Pandas -- clean, validate, deduplicate
        |  clean DataFrame
        v
  [Loader]             SQLAlchemy -- UPSERT into PostgreSQL
        |
        v
  [PostgreSQL]         products_market, pipeline_runs, rejected_records
        |
        v
  [Metabase]           Dashboards for analysts & stakeholders

All steps orchestrated and monitored by Apache Airflow (DAG: ecommerce_market_etl)
```

---

## Quick Start (Python only, no Docker needed yet)

### Prerequisites

- Python 3.11 or later — https://www.python.org/downloads/
- Git — https://git-scm.com/downloads

### 1. Clone and enter the project

Open **Command Prompt** or **PowerShell**:

```
git clone https://github.com/your-org/ecommerce-etl.git
cd ecommerce-etl
```

### 2. Create and activate a virtual environment

```
python -m venv venv
venv\Scripts\activate
```

Your prompt will show `(venv)` when the environment is active.

### 3. Install dependencies

```
pip install requests pytest pytest-mock beautifulsoup4 pyyaml
```

### 4. Run the FakeStore extractor

```
python -m etl.extractor
```

This fetches ~20 products from FakeStoreAPI and saves them locally. Look for a new folder under `staging\` named with today's date.

### 5. Verify the file was created

```
dir staging
```

You should see a folder like `2025-04-18`. Inside it: `raw_products.json`.

---

## Verifying the Retry Functionality

The retry decorator in `utils\retry.py` handles network timeouts, HTTP 429, and HTTP 5xx errors automatically. There are two ways to verify it:

### Option A — Interactive demo script (quickest)

Simulates failures without any real server. Production delays (30 s, 60 s, 120 s) are compressed to 0.05 s, so the whole thing finishes in under 2 seconds:

```
python scripts\demo_retry.py
```

All 5 demos should print `>> PASS`. Here is what each one proves:

| Demo | What is being tested |
|------|----------------------|
| 1 | `requests.Timeout` on attempts 1 and 2, success on attempt 3 |
| 2 | HTTP 429 triggers retry with a minimum 60 s delay (not the standard 30 s) |
| 3 | HTTP 503 uses exponential backoff: 30 s then 60 s then 120 s |
| 4 | After all retries are used up, the final exception is re-raised to the caller |
| 5 | A 404 `ExtractionError` is NOT retried — it is a client-side bug, not transient |

### Option B — Unit tests

84 tests covering every scenario with mocked HTTP calls (no real network needed):

```
python -m pytest tests\ -v
```

To run only the retry-related tests:

```
python -m pytest tests\test_extractor.py::TestRetryWithBackoff -v
```

---

## Running All Tests

```
python -m pytest tests\ -v
```

Run a single test file:

```
python -m pytest tests\test_extractor.py -v
python -m pytest tests\test_tiki_scraper.py -v
```

Run a specific test by name:

```
python -m pytest tests\test_extractor.py::TestExtract::test_exact_page_limit_does_not_duplicate -v
```

With coverage (requires `pip install pytest-cov`):

```
python -m pytest tests\ --cov=etl --cov=utils --cov-report=term-missing
```

Security check — confirm no secrets are in source code:

```
findstr /s /i "password secret token apikey" etl\*.py utils\*.py dags\*.py
```

Expected: no output (no results found).

---

## Data Sources

### Primary: FakeStoreAPI

URL: https://fakestoreapi.com/products  
Free, stable, no authentication required. Used for development and CI.  
Returns ~20 products in a single call (no real pagination).

| FakeStore field | Schema field | Notes |
|-----------------|--------------|-------|
| `id`            | `product_id` | Converted to string |
| `title`         | `name`       | |
| `price`         | `price`      | USD |
| `category`      | `category`   | Normalised in transformer |
| `image`         | `source_url` | Image URL only — no product page URL available |
| `rating.rate`   | `rating`     | Clipped to 0.0–5.0 |
| `rating.count`  | `review_count` | |

### Bonus: Tiki scraper

Scrapes tiki.vn Laptop category using the site's own listing API plus BeautifulSoup. Includes 1.5 s delay between pages and User-Agent rotation.

```
python -m etl.tiki_scraper
```

Note: prices are in VND (Vietnamese Dong). Do not mix with FakeStore USD data in dashboards.

---

## Project Structure

```
ecommerce-etl\
|-- dags\
|   |-- ecommerce_market_dag.py     Airflow DAG (Sprint 3)
|-- etl\
|   |-- extractor.py                BaseExtractor + FakeStoreExtractor
|   |-- tiki_scraper.py             TikiScraper
|   |-- transformer.py              Pandas cleaning pipeline (Sprint 2)
|   |-- loader.py                   PostgreSQL UPSERT loader (Sprint 2)
|   |-- notifier.py                 Slack/email alerts (Sprint 3)
|-- utils\
|   |-- retry.py                    @retry_with_backoff decorator
|-- scripts\
|   |-- demo_retry.py               Interactive retry verification
|-- tests\
|   |-- test_extractor.py           35 tests
|   |-- test_tiki_scraper.py        49 tests
|-- config\
|   |-- schema_spec.yaml            Column types for validation
|-- staging\                        Raw extracted files (git-ignored)
|-- sql\
|   |-- create_tables.sql           Schema DDL (3 tables)
|-- README.md
|-- requirements.txt
|-- .env.example
```

---

## Docker + Airflow Setup (Sprint 3)

These steps require Docker Desktop to be installed and running.

### Copy and fill in environment variables

```
copy .env.example .env
```

Open `.env` in Notepad or VS Code and fill in the values. To generate a Fernet key for Airflow:

```
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Paste the output as the value of `AIRFLOW__CORE__FERNET_KEY` in `.env`.

### Start the full stack

```
docker compose up airflow-init
docker compose up -d
```

Wait about 60 seconds. Services will be available at:

| Service | URL | Default login |
|---------|-----|---------------|
| Airflow | http://localhost:8080 | admin / admin123 |
| Metabase | http://localhost:3000 | Set on first visit |
| PostgreSQL | localhost:5432 | From your .env file |

### Create database tables

```
docker exec airflow_scheduler python /opt/airflow/scripts/init_db.py
```

### Verify tables exist

```
docker exec -it ecommerce_postgres psql -U etl_user -d ecommerce_db -c "\dt"
```

Expected: `pipeline_runs`, `products_market`, `rejected_records`.

### Configure Airflow connections and variables

```
docker exec airflow_scheduler python /opt/airflow/scripts/setup_airflow_connections.py
```

### Trigger a manual DAG run

```
docker exec airflow_scheduler airflow dags trigger ecommerce_market_etl
```

### Backfill past dates

```
docker exec airflow_scheduler airflow dags backfill --start-date 2025-04-01 --end-date 2025-04-10 ecommerce_market_etl
```

### Query the database directly

```
docker exec -it ecommerce_postgres psql -U etl_user -d ecommerce_db
```

Once inside psql, example queries:

```sql
SELECT category, AVG(price)::NUMERIC(10,2) AS avg_price
FROM products_market
GROUP BY category
ORDER BY avg_price DESC;

SELECT * FROM pipeline_runs ORDER BY started_at DESC LIMIT 5;

SELECT rejection_reason, COUNT(*) FROM rejected_records GROUP BY rejection_reason;
```

Type `\q` to exit psql.

### Stop all services

```
docker compose down
```

### Full reset including database

```
docker compose down -v
```

---

## Troubleshooting

**`python -m etl.extractor` gives `ModuleNotFoundError`**  
Make sure you are inside the `ecommerce-etl` folder when you run the command, and that your virtual environment is active (look for `(venv)` in your prompt).

**`venv\Scripts\activate` is blocked by PowerShell**  
Run this once to allow scripts, then try again:
```
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

**No staging file after running the extractor**  
Check the terminal output for errors. If you see a connection error, confirm your internet is working — FakeStoreAPI is free and needs no authentication.

**Tiki scraper returns 0 records**  
Tiki may be rate-limiting your IP. Wait 5–10 minutes and try again. For development, FakeStore is the recommended source.

**Docker: Airflow UI not reachable at localhost:8080**  
```
docker compose ps
docker compose logs airflow-webserver
```

**Docker: PostgreSQL connection refused**  
```
docker exec ecommerce_postgres pg_isready -U etl_user
```
