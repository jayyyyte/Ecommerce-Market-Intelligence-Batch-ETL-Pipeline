# Module: `etl/config.py`

## Overview

Central configuration module for the ETL pipeline. Provides a single, immutable `PipelineConfig` object populated from a 3-level priority chain: **Airflow Variable → environment variable → hardcoded default**. All other ETL modules consume config exclusively from here.

---

## Responsibilities

### ✅ Handles
- Reading configuration values from Airflow Variables (when inside Airflow runtime)
- Falling back to `os.environ` when Airflow is unavailable (local dev, unit tests)
- Falling back to safe hardcoded defaults when neither source provides a value
- Assembling the SQLAlchemy database URL from individual connection parts
- Exposing a single `get_config()` factory function as the public API

### ❌ Does NOT handle
- Writing or updating Airflow Variables (see `scripts/setup_airflow_connections.py`)
- Validating that values are reachable (e.g. does not ping the DB URL)
- Secrets management or encryption
- Per-task or per-DAG-run configuration overrides

---

## Structure

```
etl/
└── config.py
    ├── _get(key, default)      # Internal helper — 3-level priority lookup
    ├── PipelineConfig          # Frozen dataclass holding all settings
    │   ├── .db_url             # Derived property: full SQLAlchemy URL
    │   └── .is_production      # Derived property: True when PIPELINE_ENV=prod
    └── get_config()            # Public factory — call this from other modules
```

---

## Usage

Call `get_config()` once at the start of each ETL task function. Do **not** import `PipelineConfig` directly or construct it manually.

```python
from etl.config import get_config

def my_airflow_task(**context):
    cfg = get_config()

    print(cfg.data_source_url)   # https://fakestoreapi.com/products
    print(cfg.staging_dir)       # /opt/airflow/staging
    print(cfg.db_url)            # postgresql+psycopg2://etl_user:...@postgres:5432/ecommerce_db
    print(cfg.max_retries)       # 3
    print(cfg.is_production)     # False (dev mode)
```

Using the DB URL with SQLAlchemy:

```python
import sqlalchemy as sa
from etl.config import get_config

cfg = get_config()
engine = sa.create_engine(cfg.db_url)
```

---

## API

### `_get(key: str, default: str = "") → str`  *(internal)*

| Priority | Source | Condition |
|---|---|---|
| 1st | `airflow.models.Variable.get(key)` | Airflow importable and variable exists |
| 2nd | `os.environ.get(key)` | Environment variable is set |
| 3rd | `default` argument | Always available as final fallback |

Silently degrades — if Airflow is not installed, it skips to `os.environ` without raising.

---

### `class PipelineConfig` *(frozen dataclass)*

All fields are read-only after construction.

| Field | Type | Default | Description |
|---|---|---|---|
| `data_source_url` | `str` | `https://fakestoreapi.com/products` | API endpoint or scraping target |
| `staging_dir` | `str` | `/opt/airflow/staging` | Raw file output directory |
| `archive_retention_days` | `int` | `90` | Days to keep archived raw files |
| `postgres_user` | `str` | `etl_user` | DB username |
| `postgres_password` | `str` | `etl_password` | DB password |
| `postgres_host` | `str` | `postgres` | DB host (Docker service name) |
| `postgres_port` | `str` | `5432` | DB port |
| `postgres_db` | `str` | `ecommerce_db` | Target database name |
| `postgres_conn_id` | `str` | `postgres_ecommerce` | Airflow Connection ID |
| `max_retries` | `int` | `3` | HTTP retry attempts for extractor |
| `request_timeout` | `int` | `30` | HTTP request timeout (seconds) |
| `rate_limit_delay` | `float` | `1.5` | Seconds between page requests (scraping) |
| `slack_webhook_url` | `str` | `""` | Slack Incoming Webhook URL; empty = disabled |
| `pipeline_env` | `str` | `dev` | Runtime environment: `dev` / `staging` / `prod` |
| `min_quality_score` | `float` | `70.0` | Min acceptable data quality % before warning |

**Derived properties (computed, not stored):**

| Property | Returns | Description |
|---|---|---|
| `db_url` | `str` | Full `postgresql+psycopg2://...` SQLAlchemy URL |
| `is_production` | `bool` | `True` when `pipeline_env == "prod"` |

---

### `get_config() → PipelineConfig`

Public factory. Calls `_get()` for every field and constructs a frozen `PipelineConfig`. Safe to call multiple times — each call returns a fresh snapshot of current configuration.

---

## Data Flow

```
Airflow Variable store
    │  (1st priority — only available inside Airflow tasks)
    ▼
os.environ  ←── .env file loaded by Docker Compose
    │  (2nd priority — always available)
    ▼
Hardcoded defaults
    │  (3rd priority — safe dev fallback)
    ▼
_get(key, default)
    │
    ▼
PipelineConfig(frozen dataclass)
    │
    ▼
get_config() → returned to caller (extractor / transformer / loader / notifier)
```

---

## Dependencies

### External
| Library | Used for |
|---|---|
| `airflow.models.Variable` | Reading Airflow Variables (optional import — graceful fallback) |

### Internal
- None — this is the **root** module with no internal dependencies

### Standard library
- `os`, `dataclasses`, `typing`

---

## Notes / Constraints

- **`frozen=True`** on the dataclass prevents accidental mutation anywhere in the codebase. If a task needs a different value, it must call `get_config()` again or set the Airflow Variable before the DAG run.
- **`postgres_host` defaults to `"postgres"`** (the Docker Compose service name). When running `init_db.py` locally outside Docker, override with `POSTGRES_HOST=localhost`.
- **`slack_webhook_url=""` disables Slack** — the `notifier` module must check `if cfg.slack_webhook_url` before attempting to send.
- Airflow import errors are silently swallowed by design — the module must be importable in plain Python unit tests without a running Airflow instance.
- Type coercions (`int(...)`, `float(...)`) happen in `get_config()`. If a Variable contains a non-numeric string for `max_retries`, it will raise `ValueError` at config load time — fail-fast by design.