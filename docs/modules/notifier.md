# Module: `etl/notifier.py`

## Overview

Sends pipeline status notifications to Slack (and optionally email) at the end of every DAG run. Composes a structured summary message containing run outcome, row counts, data quality score, and error details. Designed to run as the final Airflow task using `TriggerRule.ALL_DONE` so it fires on both success and failure.

---

## Responsibilities

### ✅ Handles
- Posting a formatted Slack message to the configured Incoming Webhook URL
- Distinguishing `SUCCESS` vs `FAILURE` vs `PARTIAL` with distinct message formatting
- Including: `run_id`, `execution_date`, task that failed (if any), row counts, quality score, duration
- Gracefully skipping Slack delivery when `SLACK_WEBHOOK_URL` is empty (dev mode)
- Logging all notification attempts and HTTP responses to the pipeline log
- Supporting Airflow's `on_failure_callback` for immediate task-level alerts (separate from the summary)

### ❌ Does NOT handle
- Loading or querying data from PostgreSQL
- Determining pipeline status — it reads status from XCom / context passed by the caller
- Retrying failed notifications (a notification failure should not fail the DAG)
- Email sending (deferred to future sprint — Airflow's built-in email operator covers this)

---

## Structure

```
etl/
└── notifier.py
    ├── class PipelineNotifier          # Main notification class
    │   ├── send_success(context, ...)  # Sends green success summary
    │   ├── send_failure(context, ...)  # Sends red failure summary
    │   └── _post_to_slack(payload)     # Internal — HTTP POST to webhook
    ├── notify_on_failure(context)      # Standalone callback for on_failure_callback
    └── _build_slack_payload(...)       # Internal — constructs Block Kit message dict
```

---

## Usage

### As a DAG task (end-of-run summary)

The `notify_status` task always runs regardless of upstream task outcomes:

```python
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import PythonOperator
from etl.notifier import PipelineNotifier
from etl.config import get_config

def notify_status(**context):
    cfg      = get_config()
    ti       = context["ti"]
    notifier = PipelineNotifier(cfg)

    # Collect metrics from XCom
    rows_extracted = ti.xcom_pull(task_ids="extract_data",   key="row_count")    or 0
    rows_loaded    = ti.xcom_pull(task_ids="load_to_db",     key="rows_inserted") or 0
    quality_score  = ti.xcom_pull(task_ids="transform_data", key="quality_score") or 0.0

    # Check if any upstream task failed
    failed_tasks = [
        t.task_id for t in context["dag_run"].get_task_instances()
        if t.state == "failed"
    ]

    if failed_tasks:
        notifier.send_failure(context, failed_tasks=failed_tasks)
    else:
        notifier.send_success(context,
                              rows_extracted=rows_extracted,
                              rows_loaded=rows_loaded,
                              quality_score=quality_score)

notify_task = PythonOperator(
    task_id="notify_status",
    python_callable=notify_status,
    trigger_rule=TriggerRule.ALL_DONE,   # ← runs even if upstream failed
)
```

### As an `on_failure_callback` (immediate task-level alert)

```python
from etl.notifier import notify_on_failure

default_args = {
    "on_failure_callback": notify_on_failure,
    ...
}
```

---

## API

### `class PipelineNotifier`

```python
notifier = PipelineNotifier(cfg: PipelineConfig)
```

---

#### `send_success(context, rows_extracted, rows_loaded, quality_score) → None`

Posts a green ✅ Slack message. Message includes:

- DAG ID and execution date
- Rows extracted / loaded / rejected
- Data quality score (warns inline if below `cfg.min_quality_score`)
- Total pipeline duration

---

#### `send_failure(context, failed_tasks) → None`

Posts a red ❌ Slack message. Message includes:

- Which task(s) failed
- Error message excerpt from the Airflow task log
- Link to Airflow UI for the failed run
- Execution date and DAG ID

---

#### `notify_on_failure(context) → None`  *(module-level function)*

Standalone callback compatible with Airflow's `on_failure_callback` signature. Instantiates `PipelineNotifier` internally and calls `send_failure()`.

---

### Slack Message Format (Block Kit)

**Success example:**
```
✅  ecommerce_market_etl — SUCCESS
────────────────────────────────────
📅  Execution date : 2025-06-15
⏱  Duration        : 3m 42s
📦  Rows extracted : 100
✅  Rows loaded     : 97
❌  Rows rejected   : 3
📊  Quality score   : 97.0%
```

**Failure example:**
```
❌  ecommerce_market_etl — FAILED
────────────────────────────────────
📅  Execution date : 2025-06-15
💥  Failed task    : load_to_db
🔴  Error          : could not connect to server: Connection refused
🔗  View logs      : http://localhost:8080/dags/ecommerce_market_etl/...
```

---

## Data Flow

```
Airflow context + XCom values
    │
    ▼
PipelineNotifier.send_success() / send_failure()
    │
    ├── _build_slack_payload(...)   →  Block Kit JSON dict
    │
    └── _post_to_slack(payload)
            │
            ├── cfg.slack_webhook_url == ""  →  log "Slack disabled" and return
            │
            └── requests.post(webhook_url, json=payload, timeout=10)
                    │
                    ├── 200 OK   →  log success
                    └── non-200  →  log warning (do NOT raise — notification failure ≠ pipeline failure)
```

---

## Dependencies

### External
| Library | Used for |
|---|---|
| `requests` | HTTP POST to Slack Incoming Webhook |
| `loguru` | Logging notification attempts and responses |

### Internal
| Module | Used for |
|---|---|
| `etl.config` | `cfg.slack_webhook_url`, `cfg.min_quality_score`, `cfg.pipeline_env` |

---

## Notes / Constraints

- **Never raise from `_post_to_slack()`** — a notification failure must not fail the DAG. All exceptions inside `_post_to_slack` are caught and logged as warnings.
- **`trigger_rule=TriggerRule.ALL_DONE`** is mandatory on the `notify_status` task. Without it, the task is skipped when any upstream task fails, and you lose failure alerts.
- **Empty `SLACK_WEBHOOK_URL`** is valid in `dev` mode — the notifier logs `"Slack notifications disabled"` and returns immediately. No error is raised.
- **Quality score warning:** If `quality_score < cfg.min_quality_score`, the success message includes an inline `⚠️  Quality below threshold` warning — it does not convert the message to a failure.
- The notifier is **stateless** — it does not read from or write to the database. All information it needs must be passed via arguments or Airflow context.
- For email notifications, use Airflow's built-in `email_on_failure=True` in `default_args` — do not re-implement SMTP in this module.