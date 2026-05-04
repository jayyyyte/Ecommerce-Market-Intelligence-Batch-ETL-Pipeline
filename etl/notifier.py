"""Notification helpers for Airflow pipeline status messages."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import requests

from etl.config import PipelineConfig, get_config

logger = logging.getLogger(__name__)


class PipelineNotifier:
    """Send Slack notifications without making alert delivery pipeline-critical."""

    def __init__(self, cfg: PipelineConfig | None = None) -> None:
        self.cfg = cfg or get_config()

    def send_success(
        self,
        context: dict[str, Any],
        *,
        run_id: str | None = None,
        status: str = "SUCCESS",
        rows_extracted: int = 0,
        rows_loaded: int = 0,
        rows_rejected: int = 0,
        quality_score: float = 0.0,
    ) -> None:
        """Post a success or partial-success summary to Slack."""
        quality_note = ""
        if quality_score < self.cfg.min_quality_score:
            quality_note = (
                f"\nQuality below threshold: {quality_score:.2f}% "
                f"< {self.cfg.min_quality_score:.2f}%"
            )

        text = (
            f"{self._dag_id(context)} - {status}\n"
            f"Execution date: {self._execution_date(context)}\n"
            f"Run ID: {run_id or 'unknown'}\n"
            f"Rows extracted: {rows_extracted}\n"
            f"Rows loaded: {rows_loaded}\n"
            f"Rows rejected: {rows_rejected}\n"
            f"Quality score: {quality_score:.2f}%"
            f"{quality_note}"
        )
        self._post_to_slack(self._build_slack_payload(text))

    def send_failure(
        self,
        context: dict[str, Any],
        *,
        run_id: str | None = None,
        failed_tasks: list[str] | None = None,
        error_message: str | None = None,
    ) -> None:
        """Post a failure summary to Slack."""
        task_list = ", ".join(failed_tasks or []) or "unknown"
        text = (
            f"{self._dag_id(context)} - FAILED\n"
            f"Execution date: {self._execution_date(context)}\n"
            f"Run ID: {run_id or 'unknown'}\n"
            f"Failed task(s): {task_list}\n"
            f"Error: {self._shorten(error_message or 'See Airflow task logs')}\n"
            f"Log URL: {self._log_url(context)}"
        )
        self._post_to_slack(self._build_slack_payload(text))

    def _build_slack_payload(self, text: str) -> dict[str, Any]:
        return {
            "text": text,
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"```{text}```",
                    },
                }
            ],
        }

    def _post_to_slack(self, payload: dict[str, Any]) -> None:
        webhook_url = self.cfg.slack_webhook_url
        if not webhook_url:
            logger.info("Slack notifications disabled; no webhook URL configured.")
            return

        try:
            response = requests.post(webhook_url, json=payload, timeout=10)
        except requests.RequestException as exc:
            logger.warning("Slack notification failed: %s", exc)
            return

        if response.status_code >= 400:
            logger.warning(
                "Slack notification returned HTTP %s: %s",
                response.status_code,
                self._shorten(response.text),
            )
            return

        logger.info("Slack notification sent.")

    def _dag_id(self, context: dict[str, Any]) -> str:
        dag = context.get("dag")
        if dag is not None and getattr(dag, "dag_id", None):
            return str(dag.dag_id)
        dag_run = context.get("dag_run")
        if dag_run is not None and getattr(dag_run, "dag_id", None):
            return str(dag_run.dag_id)
        return str(context.get("dag_id", "unknown_dag"))

    def _execution_date(self, context: dict[str, Any]) -> str:
        if context.get("ds"):
            return str(context["ds"])
        logical_date = context.get("logical_date") or context.get("execution_date")
        if isinstance(logical_date, datetime):
            return logical_date.date().isoformat()
        return str(logical_date or "unknown")

    def _log_url(self, context: dict[str, Any]) -> str:
        task_instance = context.get("task_instance") or context.get("ti")
        return str(getattr(task_instance, "log_url", "unavailable"))

    def _shorten(self, value: str, limit: int = 500) -> str:
        normalized = " ".join(str(value).split())
        if len(normalized) <= limit:
            return normalized
        return normalized[: limit - 3] + "..."


def notify_on_failure(context: dict[str, Any]) -> None:
    """Airflow-compatible failure callback."""
    task_instance = context.get("task_instance") or context.get("ti")
    task_id = getattr(task_instance, "task_id", None)
    exception = context.get("exception")
    notifier = PipelineNotifier()
    notifier.send_failure(
        context,
        failed_tasks=[task_id] if task_id else None,
        error_message=str(exception) if exception else None,
    )


def utc_now_iso() -> str:
    """Return an ISO timestamp for simple tests and payload metadata."""
    return datetime.now(timezone.utc).isoformat()
