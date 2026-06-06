"""蒸馏格式：把 teacher predictions 转成不同训练样本结构。

支持格式（见 v1_9_promptB 第八节）：
    instruction_tuning / caption_sft / embedding_pairs / anomaly_detection
"""

from __future__ import annotations

import json
from typing import Any, Iterator

INSTRUCTION_TUNING = "instruction_tuning"
CAPTION_SFT = "caption_sft"
EMBEDDING_PAIRS = "embedding_pairs"
ANOMALY_DETECTION = "anomaly_detection"

DISTILL_FORMATS: tuple[str, ...] = (
    INSTRUCTION_TUNING,
    CAPTION_SFT,
    EMBEDDING_PAIRS,
    ANOMALY_DETECTION,
)

DEFAULT_INSTRUCTION_TEMPLATES: dict[str, str] = {
    "caption": "Describe the robot episode.",
    "anomaly": "Determine whether the robot episode is anomalous.",
}


def _parse_prediction(row: dict[str, Any]) -> dict[str, Any]:
    """predictions.parquet 的 prediction_json 是字符串，这里解析回 dict。"""
    raw = row.get("prediction_json")
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw:
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {}
        except ValueError:
            return {}
    return {}


def _input_block(row: dict[str, Any]) -> dict[str, Any]:
    refs = [u for u in [row.get("input_uri")] if u]
    return {
        "sample_id": row.get("sample_id"),
        "dataset_id": row.get("dataset_id"),
        "episode_id": row.get("episode_id"),
        "input_refs": refs,
    }


def build_record(
    fmt: str,
    row: dict[str, Any],
    *,
    teacher_model: str,
    instruction_templates: dict[str, str],
) -> dict[str, Any] | None:
    """把一条 teacher 输出转成指定格式的训练样本；不适配则返回 None。"""
    pred = _parse_prediction(row)
    sample_id = row.get("sample_id")
    if not sample_id:
        return None

    if fmt in (INSTRUCTION_TUNING, CAPTION_SFT):
        text = pred.get("text")
        if not text:
            return None
        instruction = instruction_templates.get("caption", DEFAULT_INSTRUCTION_TEMPLATES["caption"])
        return {
            "id": sample_id,
            "instruction": instruction,
            "input": _input_block(row),
            "output": text,
            "teacher_model": teacher_model,
            "metadata": {"confidence": row.get("confidence"), "prediction_type": row.get("prediction_type")},
        }

    if fmt == EMBEDDING_PAIRS:
        emb = pred.get("embedding")
        if not isinstance(emb, list):
            return None
        return {
            "id": sample_id,
            "sample_id": sample_id,
            "dataset_id": row.get("dataset_id"),
            "episode_id": row.get("episode_id"),
            "embedding": emb,
            "dim": pred.get("dim", len(emb)),
            "teacher_model": teacher_model,
        }

    if fmt == ANOMALY_DETECTION:
        score = pred.get("anomaly_score")
        if score is None:
            return None
        return {
            "id": sample_id,
            "sample_id": sample_id,
            "dataset_id": row.get("dataset_id"),
            "episode_id": row.get("episode_id"),
            "anomaly_score": score,
            "label": pred.get("label", "anomaly" if float(score) >= 0.5 else "normal"),
            "teacher_model": teacher_model,
        }

    raise ValueError(f"未知 distill_format={fmt!r}；允许：{', '.join(DISTILL_FORMATS)}")


def iter_records(
    fmt: str,
    rows: list[dict[str, Any]],
    *,
    teacher_model: str,
    instruction_templates: dict[str, str],
) -> Iterator[dict[str, Any]]:
    """遍历 teacher 输出行，产出可用训练样本（跳过缺字段的）。"""
    for row in rows:
        if (row.get("status") or "OK") != "OK":
            continue
        rec = build_record(fmt, row, teacher_model=teacher_model, instruction_templates=instruction_templates)
        if rec is not None:
            yield rec
