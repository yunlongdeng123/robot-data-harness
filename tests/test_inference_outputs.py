"""v1.9 推理输出 schema / manifest 测试。"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from robot_dh.inference.outputs import (
    PREDICTIONS_SCHEMA,
    InferenceOutputRecord,
    read_predictions,
    write_predictions,
)


def test_predictions_schema_columns() -> None:
    expected = [
        "output_id", "job_id", "model_id", "sample_id", "dataset_id", "version",
        "episode_id", "frame_id", "input_uri", "prediction_type", "prediction_json",
        "confidence", "latency_ms", "token_count", "status", "error_message", "created_at",
    ]
    assert PREDICTIONS_SCHEMA.names == expected


def test_write_and_read_predictions_roundtrip(tmp_path) -> None:
    out = f"file://{(tmp_path / 'infer').as_posix()}"
    recs = [
        InferenceOutputRecord(
            job_id="job-1", model_id="mc", sample_id="s1", prediction_type="caption",
            prediction={"text": "hello"}, confidence=0.9, latency_ms=1.2, token_count=1,
            dataset_id="demo", version="v1", episode_id="e0",
        )
    ]
    uri = write_predictions(out, recs)
    assert uri.endswith("predictions.parquet")
    rows = read_predictions(out)
    assert len(rows) == 1
    row = rows[0]
    # prediction_json 在 parquet 里是字符串。
    assert json.loads(row["prediction_json"]) == {"text": "hello"}
    assert row["sample_id"] == "s1"
    assert row["status"] == "OK"


def test_empty_predictions_writes_schema(tmp_path) -> None:
    out = f"file://{(tmp_path / 'empty').as_posix()}"
    uri = write_predictions(out, [])
    table = pq.read_table(uri.replace("file://", ""))
    assert table.num_rows == 0
    assert table.schema.names == PREDICTIONS_SCHEMA.names


def test_manifest_structure(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("ROBOT_DH_DB_URI", f"sqlite:///{tmp_path/'m.db'}")
    monkeypatch.setenv("ROBOT_DH_AI_EVENTS_DIR", str(tmp_path / "events"))
    d = tmp_path / "ml-ready/demo/v1"
    d.mkdir(parents=True)
    pq.write_table(pa.table({"episode_id": ["e0", "e1"]}), (d / "train.parquet").as_posix())
    from robot_dh.models import ModelRegistry, ModelSpec
    from robot_dh.inference import run_inference

    ModelRegistry().register(ModelSpec(model_id="mc", model_name="MC", model_type="caption", backend="mock"))
    out = tmp_path / "infer"
    run_inference(input_uri=f"file://{d.as_posix()}", model_id="mc", output_uri=f"file://{out.as_posix()}")
    manifest = json.loads((out / "_manifest.json").read_text())
    assert manifest["kind"] == "inference_job"
    assert manifest["schema_version"] == "1.9"
    assert manifest["total_samples"] == 2
    paths = {f["path"] for f in manifest["files"]}
    assert {"predictions.parquet", "failed_samples.parquet", "inference_report.json"} == paths
