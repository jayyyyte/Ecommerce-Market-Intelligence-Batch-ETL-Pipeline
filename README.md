# Batch ETL Pipeline — E-Commerce Market Analysis

Automated pipeline that collects, cleans, and stores e-commerce product data
for market analysis. Runs daily via Apache Airflow, stores results in
PostgreSQL, and exposes insights through a Metabase dashboard.

**Stack:** Python · Apache Airflow · PostgreSQL · Metabase · Docker

---

## Architecture

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

---

## Quick Start

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (v24+)
- [Docker Compose](https://docs.docker.com/compose/) (v2.20+)
- Git
- 4 GB RAM allocated to Docker (Airflow requires it)

### 1. Clone the repository

```bash
git clone https://github.com/your-org/ecommerce-etl.git
cd ecommerce-etl
```

### 2. Configure environment variables

```bash
cp .env.example .env
```

Open `.env` and fill in the required values:

```dotenv
# PostgreSQL
POSTGRES_USER=etl_user
POSTGRES_PASSWORD=your_secure_password_here
POSTGRES_DB=ecommerce_db

# Airflow
AIRFLOW_UID=50000
AIRFLOW__CORE__FERNET_KEY=          # generate with: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
AIRFLOW__WEBSERVER__SECRET_KEY=     # any random string

# Notifications (optional for Sprint 1)
SLACK_WEBHOOK_URL=
```

### 3. Initialise and start the full stack

```bash
# First-time setup: initialise the Airflow metadata DB and create the admin user
docker compose up airflow-init

# Start all services in the background
docker compose up -d
```

Wait ~60 seconds for services to become healthy, then verify:

| Service   | URL                    | Default credentials     |
|-----------|------------------------|-------------------------|
| Airflow   | http://localhost:8080  | admin / admin           |
| Metabase  | http://localhost:3000  | set on first visit      |
| PostgreSQL| localhost:5432         | from your `.env`        |

### 4. Initialise the database schema

```bash
# Run the schema creation script inside the running postgres container
docker compose exec postgres psql \
  -U $POSTGRES_USER \
  -d $POSTGRES_DB \
  -f /docker-entrypoint-initdb.d/create_tables.sql

# Verify all 3 tables were created
docker compose exec postgres psql \
  -U $POSTGRES_USER \
  -d $POSTGRES_DB \
  -c "\dt"
```

Expected output:
```
             List of relations
 Schema |       Name        | Type  |  Owner
--------+-------------------+-------+---------
 public | pipeline_runs     | table | etl_user
 public | products_market   | table | etl_user
 public | rejected_records  | table | etl_user
```

### 5. Configure Airflow connections and variables

In the Airflow UI (http://localhost:8080):

**Add Connection** → Admin → Connections → (+):
```
Connection ID:   postgres_ecommerce
Connection Type: Postgres
Host:            postgres
Schema:          ecommerce_db
Login:           etl_user        (from .env)
Password:        your_password   (from .env)
Port:            5432
```

**Add Variables** → Admin → Variables → (+):

| Key              | Value                          |
|------------------|--------------------------------|
| DATA_SOURCE_URL  | https://fakestoreapi.com       |
| STAGING_DIR      | /opt/airflow/staging           |
| SLACK_WEBHOOK_URL| (your webhook or leave empty)  |
| MAX_RETRIES      | 3                              |
| PIPELINE_ENV     | dev                            |

---

## Running the Pipeline

### Manual trigger (development)

```bash
# Run FakeStore extractor directly — no Airflow needed
python -m etl.extractor

# Run Tiki scraper (1 page, requires internet)
python -m etl.tiki_scraper

# Check staging output
ls staging/$(date +%Y-%m-%d)/
```

### Via Airflow UI

1. Open http://localhost:8080
2. Find DAG: `ecommerce_market_etl`
3. Toggle the DAG **On** (top-left switch)
4. Click ▶ (Trigger DAG) for an immediate run
5. Watch the Graph View — all 5 tasks should turn green

### Backfill past dates

```bash
docker compose exec airflow-scheduler \
  airflow dags backfill \
    --start-date 2025-04-01 \
    --end-date   2025-04-10 \
    ecommerce_market_etl
```

---

## Data Sources

### Primary: FakeStoreAPI (Development / CI)

- **URL:** https://fakestoreapi.com/products
- **Type:** REST API (JSON)
- **Auth:** None required
- **Fields returned:** `id`, `title`, `price`, `description`, `category`, `image`, `rating.rate`, `rating.count`
- **Pagination:** `?limit=N` — returns full catalogue (~20 products) in one call
- **Rate limit:** None enforced; we default to polite 1 s delay anyway
- **Why:** Stable, predictable schema — ideal for development and CI/CD pipelines

Field mapping to our schema:

| FakeStore field | Our schema field | Notes                         |
|-----------------|------------------|-------------------------------|
| `id`            | `product_id`     | Stringified                   |
| `title`         | `name`           |                               |
| `price`         | `price`          | USD float                     |
| `category`      | `category`       | Raw; normalised in transformer|
| `image`         | `source_url`     | Image URL (no product page URL available) |
| `rating.rate`   | `rating`         | Clipped to [0.0, 5.0]         |
| `rating.count`  | `review_count`   |                               |

### Bonus: Tiki (Production Demo)

- **URL:** https://tiki.vn (category listing API)
- **Type:** Semi-public JSON API + BeautifulSoup for HTML detail pages
- **Auth:** None required for listing
- **Category scraped (default):** Laptops (`category_id=8095`)
- **Pagination:** `?page=N&limit=40` — true paginated offset
- **Rate limit:** 1.5 s sleep between pages; User-Agent rotated from 5 real browser strings
- **Risk:** API endpoints may change; use FakeStore for CI, Tiki only for demo runs
- **Prices:** VND (Vietnamese Dong)

---

## Testing

### Run the full test suite

```bash
# Install test dependencies (local dev, outside Docker)
pip install requests pytest pytest-mock beautifulsoup4 pyyaml

# Run all tests with verbose output
python -m pytest tests/ -v

# Run with coverage report
python -m pytest tests/ --cov=etl --cov=utils --cov-report=term-missing
```

Expected output: **84 passed** (35 extractor + 49 tiki scraper tests)

### Run a specific test class

```bash
# Just the retry decorator tests
python -m pytest tests/test_extractor.py::TestRetryWithBackoff -v

# Just the Tiki scraper integration tests
python -m pytest tests/test_tiki_scraper.py::TestExtract -v

# The key pagination non-duplication regression test
python -m pytest tests/test_extractor.py::TestExtract::test_exact_page_limit_does_not_duplicate -v
```

### Security check — no hardcoded credentials

```bash
grep -r 'password\|secret\|token\|apikey' dags/ etl/ utils/ --include="*.py"
# Expected: 0 results (all secrets are in .env / Airflow Variables)
```

---

## Project Structure

```
ecommerce-etl/
├── dags/
│   └── ecommerce_market_dag.py     # Airflow DAG (Sprint 3)
├── etl/
│   ├── extractor.py                # BaseExtractor + FakeStoreExtractor
│   ├── tiki_scraper.py             # TikiScraper (bonus scraping source)
│   ├── transformer.py              # Pandas cleaning pipeline (Sprint 2)
│   ├── loader.py                   # PostgreSQL UPSERT loader (Sprint 2)
│   └── notifier.py                 # Slack/email alerts (Sprint 3)
├── utils/
│   └── retry.py                    # @retry_with_backoff decorator
├── tests/
│   ├── test_extractor.py           # 35 tests — FakeStore + retry
│   ├── test_tiki_scraper.py        # 49 tests — Tiki scraper
│   ├── test_transformer.py         # (Sprint 2)
│   └── test_loader.py              # (Sprint 2)
├── sql/
│   ├── create_tables.sql           # Schema DDL (3 tables)
│   └── queries/                    # Analysis SQL queries (Sprint 4)
├── config/
│   └── schema_spec.yaml            # Expected column types for validation
├── staging/                        # Raw extracted files per date
├── logs/                           # Airflow / pipeline logs
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── .env.example
└── README.md
```

---

## Troubleshooting

**Airflow UI not reachable at localhost:8080**
```bash
docker compose ps          # check all containers are healthy
docker compose logs airflow-webserver --tail=50
```

**PostgreSQL connection refused**
```bash
docker compose exec postgres pg_isready -U etl_user
# Should print: /var/run/postgresql:5432 - accepting connections
```

**Staging file not created after running extractor**
```bash
# Check for errors in the run
python -m etl.extractor 2>&1 | head -50

# Confirm staging directory exists
ls -la staging/
```

**Tiki scraper returns 0 records**
Tiki's API may return empty results when:
- Your IP is rate-limited (wait 10 min, retry)
- The `x-guest-token` header format changed (check Tiki network requests in browser DevTools)
- Use FakeStore for development; Tiki is for live demo only

---

## Changelog

### v0.1.0-sprint1
- Docker Compose stack: Airflow + PostgreSQL + Metabase
- PostgreSQL schema: `products_market`, `pipeline_runs`, `rejected_records`
- `FakeStoreExtractor`: full extraction with retry, atomic staging write
- `TikiScraper`: paginated scraping with User-Agent rotation (bonus)
- `@retry_with_backoff`: exponential backoff 30 s / 60 s / 120 s with jitter
- 84 unit tests (0 failures), all HTTP mocked