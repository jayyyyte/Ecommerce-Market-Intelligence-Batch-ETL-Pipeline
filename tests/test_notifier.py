from __future__ import annotations

import requests

from etl.config import PipelineConfig
from etl.notifier import PipelineNotifier


class DummyResponse:
    def __init__(self, status_code: int = 200, text: str = "ok") -> None:
        self.status_code = status_code
        self.text = text


def _context() -> dict:
    return {"dag_id": "ecommerce_market_etl", "ds": "2026-04-21"}


def test_empty_slack_webhook_noops(monkeypatch) -> None:
    called = False

    def fake_post(*args, **kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr("etl.notifier.requests.post", fake_post)
    notifier = PipelineNotifier(PipelineConfig(slack_webhook_url=""))

    notifier.send_success(_context(), run_id="run-1", rows_extracted=1, rows_loaded=1)

    assert called is False


def test_send_success_posts_summary_payload(monkeypatch) -> None:
    captured = {}

    def fake_post(url, json, timeout):
        captured["url"] = url
        captured["payload"] = json
        captured["timeout"] = timeout
        return DummyResponse()

    monkeypatch.setattr("etl.notifier.requests.post", fake_post)
    notifier = PipelineNotifier(PipelineConfig(slack_webhook_url="https://hooks.example"))

    notifier.send_success(
        _context(),
        run_id="run-1",
        status="PARTIAL",
        rows_extracted=10,
        rows_loaded=8,
        rows_rejected=2,
        quality_score=80.0,
    )

    assert captured["url"] == "https://hooks.example"
    assert captured["timeout"] == 10
    assert "PARTIAL" in captured["payload"]["text"]
    assert "Rows loaded: 8" in captured["payload"]["text"]


def test_send_failure_posts_failed_task_summary(monkeypatch) -> None:
    captured = {}

    def fake_post(url, json, timeout):
        captured["payload"] = json
        return DummyResponse()

    monkeypatch.setattr("etl.notifier.requests.post", fake_post)
    notifier = PipelineNotifier(PipelineConfig(slack_webhook_url="https://hooks.example"))

    notifier.send_failure(
        _context(),
        run_id="run-1",
        failed_tasks=["load_to_db"],
        error_message="database unavailable",
    )

    assert "FAILED" in captured["payload"]["text"]
    assert "load_to_db" in captured["payload"]["text"]
    assert "database unavailable" in captured["payload"]["text"]


def test_non_200_slack_response_does_not_raise(monkeypatch) -> None:
    def fake_post(url, json, timeout):
        return DummyResponse(status_code=500, text="server error")

    monkeypatch.setattr("etl.notifier.requests.post", fake_post)
    notifier = PipelineNotifier(PipelineConfig(slack_webhook_url="https://hooks.example"))

    notifier.send_success(_context(), run_id="run-1")


def test_slack_request_exception_does_not_raise(monkeypatch) -> None:
    def fake_post(url, json, timeout):
        raise requests.Timeout("timed out")

    monkeypatch.setattr("etl.notifier.requests.post", fake_post)
    notifier = PipelineNotifier(PipelineConfig(slack_webhook_url="https://hooks.example"))

    notifier.send_failure(_context(), run_id="run-1", failed_tasks=["extract_data"])
