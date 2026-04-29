from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path

import pandas as pd
import pytest

from etl.config import PipelineConfig

pytest.importorskip("airflow")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

dag_module = importlib.import_module("dags.ecommerce_market_dag")


class DummyTI:
    def __init__(self, values=None) -> None:
        self.values = values or {}
        self.pushed = {}

    def xcom_push(self, key, value) -> None:
        self.pushed[key] = value

    def xcom_pull(self, task_ids, key="return_value"):
        return self.values.get((task_ids, key))


def test_dag_shape_and_runtime_config() -> None:
    dag = dag_module.dag

    assert dag.dag_id == "ecommerce_market_etl"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert set(dag.task_ids) == {
        "extract_data",
        "validate_schema",
        "transform_data",
        "load_to_db",
        "notify_status",
    }
    assert dag.task_dict["notify_status"].trigger_rule == "all_done"
    assert dag.task_dict["extract_data"].downstream_task_ids == {"validate_schema"}
    assert dag.task_dict["validate_schema"].downstream_task_ids == {"transform_data"}
    assert dag.task_dict["transform_data"].downstream_task_ids == {"load_to_db"}
    assert dag.task_dict["load_to_db"].downstream_task_ids == {"notify_status"}


def test_deterministic_run_id_is_stable_for_same_date() -> None:
    first = dag_module.deterministic_run_id("ecommerce_market_etl", "2026-04-21")
    second = dag_module.deterministic_run_id("ecommerce_market_etl", "2026-04-21")
    third = dag_module.deterministic_run_id("ecommerce_market_etl", "2026-04-22")

    assert first == second
    assert first != third


def test_extract_data_uses_metadata_xcom_and_no_payload(monkeypatch, tmp_path: Path) -> None:
    class FakeLoader:
        def __init__(self, engine, cfg) -> None:
            self.started = False

        def log_run_start(self, **kwargs) -> None:
            self.started = True

    class FakeExtractor:
        def __init__(self, staging_dir) -> None:
            self.staging_dir = Path(staging_dir)

        def extract(self, execution_date, run_id):
            output_dir = self.staging_dir / execution_date
            output_dir.mkdir(parents=True, exist_ok=True)
            (output_dir / "raw_products.json").write_text("[]", encoding="utf-8")
            return [{"product_id": "1"}]

    monkeypatch.setattr(
        dag_module,
        "get_config",
        lambda: PipelineConfig(staging_dir=str(tmp_path)),
    )
    monkeypatch.setattr(dag_module, "build_postgres_engine", lambda cfg: object())
    monkeypatch.setattr(dag_module, "DataLoader", FakeLoader)
    monkeypatch.setattr(dag_module, "FakeStoreExtractor", FakeExtractor)
    ti = DummyTI()

    result = dag_module.extract_data(ds="2026-04-21", ti=ti)

    assert result["rows_extracted"] == 1
    assert result["raw_path"].endswith("raw_products.json")
    assert ti.pushed["rows_extracted"] == 1
    assert "product_id" not in ti.pushed


def test_transform_data_writes_clean_rejected_and_metrics(monkeypatch, tmp_path: Path) -> None:
    raw_dir = tmp_path / "2026-04-21"
    raw_dir.mkdir()
    raw_path = raw_dir / "raw_products.json"
    raw_path.write_text('[{"product_id":"1","price":10}]', encoding="utf-8")
    extract_meta = {
        "run_id": "run-1",
        "execution_date": "2026-04-21",
        "raw_path": str(raw_path),
        "rows_extracted": 1,
    }

    class FakeTransformer:
        def __init__(self, cfg) -> None:
            pass

        def transform(self, df, run_id):
            clean_df = pd.DataFrame([{"product_id": "1", "price": 10.0}])
            rejected_df = pd.DataFrame(columns=["run_id", "raw_data", "rejection_reason"])
            return clean_df, rejected_df, 100.0

    monkeypatch.setattr(dag_module, "get_config", lambda: PipelineConfig())
    monkeypatch.setattr(dag_module, "DataTransformer", FakeTransformer)
    ti = DummyTI({("extract_data", "return_value"): extract_meta})

    result = dag_module.transform_data(ds="2026-04-21", ti=ti)

    assert Path(result["clean_path"]).exists()
    assert Path(result["rejected_path"]).exists()
    metrics = json.loads(Path(result["metrics_path"]).read_text(encoding="utf-8"))
    assert metrics["rows_transformed"] == 1
    assert metrics["data_quality_score"] == 100.0


def test_load_to_db_reads_artifacts_and_archives_raw(monkeypatch, tmp_path: Path) -> None:
    raw_path = tmp_path / "raw_products.json"
    clean_path = tmp_path / "clean_products.json"
    rejected_path = tmp_path / "rejected_records.json"
    raw_path.write_text("[]", encoding="utf-8")
    clean_path.write_text('[{"product_id":"1","price":10.0}]', encoding="utf-8")
    rejected_path.write_text("[]", encoding="utf-8")
    transform_meta = {
        "run_id": "run-1",
        "execution_date": "2026-04-21",
        "raw_path": str(raw_path),
        "clean_path": str(clean_path),
        "rejected_path": str(rejected_path),
        "rows_extracted": 1,
        "rows_transformed": 1,
        "rows_rejected": 0,
        "data_quality_score": 100.0,
    }

    class FakeLoader:
        def __init__(self, engine, cfg) -> None:
            self.archive_root = tmp_path / "archive"

        def run_load(self, clean_df, rejected_df, **kwargs):
            assert len(clean_df) == 1
            assert rejected_df.empty
            return {
                "status": "SUCCESS",
                "rows_loaded": 1,
                "rows_inserted": 1,
                "rows_updated": 0,
                "error_message": None,
            }

        def archive_staging(self, staging_path, execution_date):
            return tmp_path / "archive" / "raw_products.json"

        def cleanup_archive(self):
            return 0

    monkeypatch.setattr(dag_module, "get_config", lambda: PipelineConfig())
    monkeypatch.setattr(dag_module, "build_postgres_engine", lambda cfg: object())
    monkeypatch.setattr(dag_module, "DataLoader", FakeLoader)
    ti = DummyTI({("transform_data", "return_value"): transform_meta})

    result = dag_module.load_to_db(ds="2026-04-21", ti=ti)

    assert result["status"] == "SUCCESS"
    assert result["rows_loaded"] == 1
    assert result["archived_path"].endswith("raw_products.json")
