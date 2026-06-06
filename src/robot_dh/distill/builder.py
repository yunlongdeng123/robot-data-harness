"""蒸馏数据集 builder：从 teacher 推理输出生成 train/val/test JSONL + 卡片 + 报告。"""

from __future__ import annotations

import hashlib
import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from robot_dh.ai_tasks.events import (
    AiTaskEvent,
    EVENT_DISTILL_BUILD_FINISHED,
    EVENT_DISTILL_BUILD_STARTED,
)
from robot_dh.ai_tasks.store import AiTaskStore, resolve_optional_engine
from robot_dh.distill.dataset_card import render_dataset_card
from robot_dh.distill.formats import DEFAULT_INSTRUCTION_TEMPLATES, DISTILL_FORMATS, iter_records
from robot_dh.distill.report import DistillReport
from robot_dh.inference.outputs import read_predictions
from robot_dh.lake.store import create_lake_store
from robot_dh.lake.uri import join_uri
from robot_dh.warehouse.models import DistillationDatasetRow

LOG = logging.getLogger(__name__)

MANIFEST_FILENAME = "_manifest.json"
REPORT_FILENAME = "distill_report.json"
DATASET_CARD_FILENAME = "dataset_card.md"
SPLIT_FILENAMES = {"train": "train.jsonl", "val": "val.jsonl", "test": "test.jsonl"}


@dataclass
class DistillResult:
    report: DistillReport
    report_uri: str
    dataset_card_uri: str
    manifest_uri: str

    def to_dict(self) -> dict[str, Any]:
        return {
            **self.report.to_dict(),
            "report_uri": self.report_uri,
            "dataset_card_uri": self.dataset_card_uri,
            "manifest_uri": self.manifest_uri,
        }


def build_distill(
    *,
    teacher_output_uri: str,
    distill_format: str,
    output_uri: str,
    split: tuple[float, float, float] = (0.8, 0.1, 0.1),
    instruction_templates: dict[str, str] | None = None,
    teacher_model: str | None = None,
    source_job_id: str | None = None,
    dataset_id: str | None = None,
    version: str | None = None,
    db_uri: str | None = None,
    local_only: bool = False,
) -> DistillResult:
    """从 teacher 输出蒸馏数据集。"""
    if distill_format not in DISTILL_FORMATS:
        raise ValueError(f"未知 distill_format={distill_format!r}；允许：{', '.join(DISTILL_FORMATS)}")
    ratios = _normalize_split(split)
    templates = {**DEFAULT_INSTRUCTION_TEMPLATES, **(instruction_templates or {})}

    engine = None if local_only else resolve_optional_engine(db_uri)
    events = AiTaskStore(db_uri=db_uri, local_only=local_only)
    distill_id = f"distill-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}"

    # 读 teacher 输出 + 元信息回填。
    rows = read_predictions(teacher_output_uri)
    meta = _read_teacher_report(teacher_output_uri)
    teacher_model = teacher_model or meta.get("model_id")
    source_job_id = source_job_id or meta.get("job_id")
    dataset_id = dataset_id or meta.get("dataset_id") or _first_value(rows, "dataset_id")
    version = version or meta.get("version") or _first_value(rows, "version")

    events.emit(AiTaskEvent(
        event_type=EVENT_DISTILL_BUILD_STARTED, task_id=distill_id, job_id=source_job_id,
        model_id=teacher_model, dataset_id=dataset_id, version=version,
        payload={"distill_format": distill_format, "teacher_output_uri": teacher_output_uri},
    ))

    records = list(iter_records(
        distill_format, rows, teacher_model=teacher_model or "unknown", instruction_templates=templates,
    ))
    num_total = len(records)
    num_skipped = len(rows) - num_total

    splits: dict[str, list[dict[str, Any]]] = {"train": [], "val": [], "test": []}
    for rec in records:
        splits[_assign_split(str(rec["id"]), ratios)].append(rec)

    store = create_lake_store(output_uri)
    split_uris: dict[str, str] = {}
    for name, recs in splits.items():
        text = "\n".join(json.dumps(r, ensure_ascii=False) for r in recs)
        text = text + "\n" if text else ""
        split_uris[name] = store.write_text(join_uri(output_uri, SPLIT_FILENAMES[name]), text)

    report = DistillReport(
        distill_id=distill_id,
        distill_format=distill_format,
        teacher_model_id=teacher_model,
        source_inference_job_id=source_job_id,
        dataset_id=dataset_id,
        version=version,
        output_uri=output_uri,
        train_uri=split_uris["train"],
        val_uri=split_uris["val"],
        test_uri=split_uris["test"],
        num_total=num_total,
        num_train=len(splits["train"]),
        num_val=len(splits["val"]),
        num_test=len(splits["test"]),
        num_skipped=num_skipped,
        status="READY",
        split_ratio=list(ratios),
    )

    report_uri = store.write_json(join_uri(output_uri, REPORT_FILENAME), report.to_dict())
    card_uri = store.write_text(join_uri(output_uri, DATASET_CARD_FILENAME), render_dataset_card(report))
    manifest_uri = store.write_json(join_uri(output_uri, MANIFEST_FILENAME), _build_manifest(report, split_uris))

    if engine is not None:
        _write_distill_pg(engine, report, card_uri)

    events.emit(AiTaskEvent(
        event_type=EVENT_DISTILL_BUILD_FINISHED, task_id=distill_id, job_id=source_job_id,
        model_id=teacher_model, dataset_id=dataset_id, version=version,
        payload={"num_total": num_total, "num_train": report.num_train, "report_uri": report_uri},
    ))

    return DistillResult(
        report=report, report_uri=report_uri, dataset_card_uri=card_uri, manifest_uri=manifest_uri,
    )


def _normalize_split(split: tuple[float, float, float]) -> tuple[float, float, float]:
    vals = [max(0.0, float(s)) for s in split]
    total = sum(vals)
    if total <= 0:
        return (0.8, 0.1, 0.1)
    return (vals[0] / total, vals[1] / total, vals[2] / total)


def _assign_split(rec_id: str, ratios: tuple[float, float, float]) -> str:
    """按 id hash 稳定分桶，保证同一 id 永远落同一 split。"""
    h = int.from_bytes(hashlib.sha256(rec_id.encode("utf-8")).digest()[:8], "big") % 10000 / 10000.0
    train_t = ratios[0]
    val_t = ratios[0] + ratios[1]
    if h < train_t:
        return "train"
    if h < val_t:
        return "val"
    return "test"


def _read_teacher_report(teacher_output_uri: str) -> dict[str, Any]:
    """尽量读 teacher 的 inference_report.json 回填 model_id / job_id / dataset_id。"""
    try:
        store = create_lake_store(teacher_output_uri)
        return store.read_json(join_uri(teacher_output_uri, "inference_report.json"))
    except Exception:  # 缺报告不致命
        return {}


def _first_value(rows: list[dict[str, Any]], key: str) -> Any:
    for row in rows:
        if row.get(key):
            return row[key]
    return None


def _write_distill_pg(engine: Engine, report: DistillReport, card_uri: str) -> None:
    now = datetime.now(timezone.utc)
    try:
        with Session(engine, expire_on_commit=False, future=True) as session:
            row = session.get(DistillationDatasetRow, report.distill_id)
            kwargs = dict(
                dataset_id=report.dataset_id,
                version=report.version,
                source_inference_job_id=report.source_inference_job_id,
                teacher_model_id=report.teacher_model_id,
                distill_format=report.distill_format,
                output_uri=report.output_uri,
                train_uri=report.train_uri,
                val_uri=report.val_uri,
                test_uri=report.test_uri,
                dataset_card_uri=card_uri,
                num_train=report.num_train,
                num_val=report.num_val,
                num_test=report.num_test,
                status=report.status,
                metrics_json={"num_total": report.num_total, "num_skipped": report.num_skipped},
            )
            if row is None:
                session.add(DistillationDatasetRow(distill_id=report.distill_id, created_at=now, updated_at=now, **kwargs))
            else:
                for k, v in kwargs.items():
                    setattr(row, k, v)
                row.updated_at = now
            session.commit()
    except SQLAlchemyError as err:
        LOG.warning("distillation_datasets PG 写入失败：%s", err)


def _build_manifest(report: DistillReport, split_uris: dict[str, str]) -> dict[str, Any]:
    return {
        "schema_version": "1.9",
        "kind": "distillation_dataset",
        "distill_id": report.distill_id,
        "distill_format": report.distill_format,
        "teacher_model_id": report.teacher_model_id,
        "dataset_id": report.dataset_id,
        "version": report.version,
        "output_uri": report.output_uri,
        "files": [
            {"path": "train.jsonl", "uri": split_uris["train"]},
            {"path": "val.jsonl", "uri": split_uris["val"]},
            {"path": "test.jsonl", "uri": split_uris["test"]},
        ],
        "num_total": report.num_total,
        "created_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    }
