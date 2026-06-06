"""v1.9 蒸馏 builder 测试：instruction_tuning JSONL + dataset_card + split。"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from robot_dh.distill import build_distill
from robot_dh.distill.formats import build_record


@pytest.fixture()
def teacher_output(monkeypatch, tmp_path) -> str:
    monkeypatch.setenv("ROBOT_DH_DB_URI", f"sqlite:///{tmp_path/'d.db'}")
    monkeypatch.setenv("ROBOT_DH_AI_EVENTS_DIR", str(tmp_path / "events"))
    d = tmp_path / "ml-ready/demo/v1"
    d.mkdir(parents=True)
    pq.write_table(pa.table({"episode_id": [f"e{i}" for i in range(20)]}), (d / "train.parquet").as_posix())
    from robot_dh.models import ModelRegistry, ModelSpec
    from robot_dh.inference import run_inference

    ModelRegistry().register(ModelSpec(model_id="mc", model_name="MC", model_type="caption", backend="mock"))
    out = tmp_path / "infer/cap"
    run_inference(input_uri=f"file://{d.as_posix()}", model_id="mc", output_uri=f"file://{out.as_posix()}")
    return f"file://{out.as_posix()}"


def test_instruction_tuning_jsonl(teacher_output, tmp_path) -> None:
    out = tmp_path / "distill/it"
    res = build_distill(
        teacher_output_uri=teacher_output, distill_format="instruction_tuning",
        output_uri=f"file://{out.as_posix()}", split=(0.8, 0.1, 0.1),
    )
    for name in ("train.jsonl", "val.jsonl", "test.jsonl", "distill_report.json", "dataset_card.md", "_manifest.json"):
        assert (out / name).exists()
    # split 计数之和 == num_total
    assert res.report.num_train + res.report.num_val + res.report.num_test == res.report.num_total == 20
    # train 应占多数
    assert res.report.num_train >= res.report.num_val
    # 单行结构
    first = json.loads((out / "train.jsonl").read_text().splitlines()[0])
    assert set(first.keys()) == {"id", "instruction", "input", "output", "teacher_model", "metadata"}
    assert first["instruction"] == "Describe the robot episode."
    card = (out / "dataset_card.md").read_text()
    assert "蒸馏数据集卡片" in card


def test_split_is_stable(teacher_output, tmp_path) -> None:
    out1 = tmp_path / "d1"
    out2 = tmp_path / "d2"
    r1 = build_distill(teacher_output_uri=teacher_output, distill_format="caption_sft",
                       output_uri=f"file://{out1.as_posix()}", split=(0.8, 0.1, 0.1))
    r2 = build_distill(teacher_output_uri=teacher_output, distill_format="caption_sft",
                       output_uri=f"file://{out2.as_posix()}", split=(0.8, 0.1, 0.1))
    assert (r1.report.num_train, r1.report.num_val, r1.report.num_test) == (
        r2.report.num_train, r2.report.num_val, r2.report.num_test)


def test_build_record_formats() -> None:
    caption_row = {"sample_id": "s1", "prediction_json": json.dumps({"text": "hi"}), "dataset_id": "d", "episode_id": "e0"}
    rec = build_record("instruction_tuning", caption_row, teacher_model="mc", instruction_templates={"caption": "Cap."})
    assert rec["output"] == "hi" and rec["instruction"] == "Cap."

    emb_row = {"sample_id": "s1", "prediction_json": json.dumps({"embedding": [0.1, 0.2], "dim": 2})}
    rec2 = build_record("embedding_pairs", emb_row, teacher_model="me", instruction_templates={})
    assert rec2["embedding"] == [0.1, 0.2]

    anom_row = {"sample_id": "s1", "prediction_json": json.dumps({"anomaly_score": 0.7, "label": "anomaly"})}
    rec3 = build_record("anomaly_detection", anom_row, teacher_model="ma", instruction_templates={})
    assert rec3["anomaly_score"] == 0.7 and rec3["label"] == "anomaly"

    # 缺字段返回 None
    assert build_record("embedding_pairs", caption_row, teacher_model="x", instruction_templates={}) is None
