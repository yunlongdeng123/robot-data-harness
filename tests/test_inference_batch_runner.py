"""v1.9 推理 runner 测试：本地 ml-ready parquet -> predictions/report + 部分失败。"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from robot_dh.models import ModelRegistry, ModelSpec
from robot_dh.models.backends import openai_compatible as oc


@pytest.fixture()
def env(monkeypatch, tmp_path):
    monkeypatch.setenv("ROBOT_DH_DB_URI", f"sqlite:///{tmp_path/'infer.db'}")
    monkeypatch.setenv("ROBOT_DH_AI_EVENTS_DIR", str(tmp_path / "events"))
    return tmp_path


def _write_mlr(tmp_path: Path, n: int = 4) -> str:
    d = tmp_path / "ml-ready/demo/v1"
    d.mkdir(parents=True)
    pq.write_table(
        pa.table({
            "episode_id": [f"e{i}" for i in range(n)],
            "quality_score": [0.9 - 0.1 * i for i in range(n)],
        }),
        (d / "train.parquet").as_posix(),
    )
    return f"file://{d.as_posix()}"


def test_infer_run_writes_predictions_and_report(env) -> None:
    tmp_path = env
    input_uri = _write_mlr(tmp_path, 4)
    ModelRegistry().register(ModelSpec(model_id="mc", model_name="MC", model_type="caption", backend="mock", max_batch_size=2))
    from robot_dh.inference import run_inference

    out = tmp_path / "infer/cap"
    res = run_inference(input_uri=input_uri, model_id="mc", output_uri=f"file://{out.as_posix()}",
                        batch_size=2, max_workers=2)
    assert res.job.status == "SUCCEEDED"
    assert res.exit_code == 0
    assert res.job.total_samples == 4
    for name in ("predictions.parquet", "failed_samples.parquet", "inference_report.json", "_manifest.json"):
        assert (out / name).exists()
    table = pq.read_table((out / "predictions.parquet").as_posix())
    assert table.num_rows == 4
    report = json.loads((out / "inference_report.json").read_text())
    assert report["total_samples"] == 4
    assert report["failed_samples"] == 0
    assert report["samples_per_sec"] is not None


def test_infer_partial_failure(env, monkeypatch) -> None:
    tmp_path = env
    input_uri = _write_mlr(tmp_path, 4)
    ModelRegistry().register(ModelSpec(
        model_id="oc", model_name="OC", model_type="llm", backend="openai_compatible",
        endpoint_url="http://fake:8000/v1", max_batch_size=1,
    ))

    def fake_post(url, payload, *, headers, timeout):
        content = payload["messages"][1]["content"]
        # 偶数 episode 失败、奇数成功 -> 50% 失败率（不超阈值 -> SUCCEEDED+WARN）。
        if "e0" in content or "e2" in content:
            raise oc.OpenAIBackendError(oc.ERR_TIMEOUT, "boom")
        return {"choices": [{"message": {"content": "ok"}}], "usage": {"total_tokens": 2}}

    monkeypatch.setattr(oc, "_http_post_json", fake_post)
    from robot_dh.inference import run_inference

    out = tmp_path / "infer/oc"
    res = run_inference(input_uri=input_uri, model_id="oc", output_uri=f"file://{out.as_posix()}",
                        batch_size=1, max_workers=1, retry=0)
    assert res.job.failed_samples == 2
    assert res.warn is True
    assert res.job.status == "SUCCEEDED"  # 50% 不 > max_error_rate 0.5
    failed = pq.read_table((out / "failed_samples.parquet").as_posix())
    assert failed.num_rows == 2
    cols = set(failed.column_names)
    assert {"error_type", "error_message", "sample_id"}.issubset(cols)


def test_infer_fail_fast(env) -> None:
    tmp_path = env
    input_uri = _write_mlr(tmp_path, 4)
    ModelRegistry().register(ModelSpec(
        model_id="oc2", model_name="OC2", model_type="llm", backend="openai_compatible",
        max_batch_size=1,  # 无 endpoint -> 全部 ENDPOINT_UNAVAILABLE
    ))
    from robot_dh.inference import run_inference

    out = tmp_path / "infer/ff"
    res = run_inference(input_uri=input_uri, model_id="oc2", output_uri=f"file://{out.as_posix()}",
                        batch_size=1, max_workers=1, fail_fast=True)
    assert res.exit_code == 1
    assert res.job.status == "FAILED"
