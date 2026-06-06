"""v1.9 PromptC：AutoDL worker dry-run / claim / 执行 测试（mock DB + mock backend）。"""

from __future__ import annotations

import importlib.util
import pathlib
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from sqlalchemy.orm import Session

from robot_dh.ai_tasks.state import JOB_QUEUED, JOB_RUNNING, JOB_SUCCEEDED
from robot_dh.models.backends import openai_compatible as oc
from robot_dh.registry import get_engine, resolve_db_uri
from robot_dh.warehouse.models import InferenceJobRow, ensure_lake_tables

_WORKER_PATH = pathlib.Path(__file__).resolve().parents[1] / "workers" / "autodl_inference_worker" / "worker.py"


def _load_worker():
    spec = importlib.util.spec_from_file_location("autodl_inference_worker", _WORKER_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


worker = _load_worker()


@pytest.fixture()
def engine_and_input(monkeypatch, tmp_path):
    uri = f"sqlite:///{tmp_path/'w.db'}"
    monkeypatch.setenv("ROBOT_DH_DB_URI", uri)
    engine = get_engine(resolve_db_uri(uri))
    ensure_lake_tables(engine)
    d = tmp_path / "ml-ready/demo/v1"
    d.mkdir(parents=True)
    pq.write_table(pa.table({"episode_id": ["e0", "e1", "e2"]}), (d / "train.parquet").as_posix())
    input_uri = f"file://{d.as_posix()}"
    out_uri = f"file://{(tmp_path / 'infer/out').as_posix()}"
    with Session(engine, future=True) as session:
        session.add(InferenceJobRow(
            job_id="job-queued-1", model_id="openai-compatible-chat-v1",
            input_uri=input_uri, output_uri=out_uri, task_type="caption", status=JOB_QUEUED,
            batch_size=2,
        ))
        session.commit()
    return engine, uri, tmp_path


def _config(uri: str):
    return worker.WorkerConfig(
        db_uri=uri, model_id="openai-compatible-chat-v1",
        openai_base_url="http://fake:8000/v1", max_jobs=1,
    )


def test_dry_run_lists_without_status_change(engine_and_input, capsys) -> None:
    engine, uri, _ = engine_and_input
    rc = worker.run_worker(_config(uri), dry_run=True)
    assert rc == 0
    out = capsys.readouterr().out
    assert "job-queued-1" in out
    # 状态未变。
    with Session(engine, future=True) as session:
        row = session.get(InferenceJobRow, "job-queued-1")
        assert row.status == JOB_QUEUED


def test_claim_transitions_and_is_exclusive(engine_and_input) -> None:
    engine, uri, _ = engine_and_input
    claimed = worker.claim_next_job(engine, "openai-compatible-chat-v1")
    assert claimed is not None and claimed["job_id"] == "job-queued-1"
    with Session(engine, future=True) as session:
        assert session.get(InferenceJobRow, "job-queued-1").status == JOB_RUNNING
    # 第二次认领无可用 job。
    assert worker.claim_next_job(engine, "openai-compatible-chat-v1") is None


def test_run_claimed_job_executes(engine_and_input, monkeypatch) -> None:
    engine, uri, tmp_path = engine_and_input

    def fake_post(url, payload, *, headers, timeout):
        return {"choices": [{"message": {"content": "a caption"}}], "usage": {"total_tokens": 2}}

    monkeypatch.setattr(oc, "_http_post_json", fake_post)
    claimed = worker.claim_next_job(engine, "openai-compatible-chat-v1")
    summary = worker.run_claimed_job(engine, claimed, _config(uri))
    assert summary["status"] == JOB_SUCCEEDED
    assert summary["total_samples"] == 3
    with Session(engine, future=True) as session:
        row = session.get(InferenceJobRow, "job-queued-1")
        assert row.status == JOB_SUCCEEDED
        assert row.processed_samples == 3
    # predictions 写到 output_uri。
    pred = tmp_path / "infer/out/predictions.parquet"
    assert pred.exists()
    assert pq.read_table(pred.as_posix()).num_rows == 3
