"""normalize / etl 长任务的 checkpoint：步骤级 _checkpoint.json。

设计：
- 每个 normalize 输出目录写一份 _checkpoint.json，记录 phase / completed_steps / files。
- 中断重跑时，read_checkpoint() + 文件存在性检查 -> 跳过已完成 step。
- 对 S3 输出，checkpoint 也通过 LakeStore.write_json 写到 S3。
- 与 _manifest.json 互补：_manifest.json 是「任务成功收尾」的最终凭证；
  _checkpoint.json 是「任务进行中 / 部分完成」的中间凭证。
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from robot_dh.lake.store import LakeStore, create_lake_store
from robot_dh.lake.uri import join_uri

LOG = logging.getLogger(__name__)

CHECKPOINT_FILENAME = "_checkpoint.json"
CHECKPOINT_SCHEMA_VERSION = "1.6"


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


@dataclass
class CheckpointFile:
    """单个产出文件的状态。"""

    name: str
    status: str = "PENDING"
    uri: str | None = None
    row_count: int | None = None
    size_bytes: int | None = None
    checksum_sha256: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class Checkpoint:
    dataset_id: str
    version: str
    phase: str
    source_uri: str
    output_uri: str
    status: str = "RUNNING"
    completed_steps: list[str] = field(default_factory=list)
    files: dict[str, CheckpointFile] = field(default_factory=dict)
    metrics: dict[str, Any] = field(default_factory=dict)
    schema_version: str = CHECKPOINT_SCHEMA_VERSION
    updated_at: str = field(default_factory=_utcnow_iso)

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset_id": self.dataset_id,
            "version": self.version,
            "phase": self.phase,
            "source_uri": self.source_uri,
            "output_uri": self.output_uri,
            "status": self.status,
            "completed_steps": list(self.completed_steps),
            "files": {k: v.to_dict() for k, v in self.files.items()},
            "metrics": dict(self.metrics),
            "schema_version": self.schema_version,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "Checkpoint":
        files_raw = payload.get("files") or {}
        files = {
            k: CheckpointFile(
                name=v.get("name", k),
                status=v.get("status", "PENDING"),
                uri=v.get("uri"),
                row_count=v.get("row_count"),
                size_bytes=v.get("size_bytes"),
                checksum_sha256=v.get("checksum_sha256"),
            )
            for k, v in files_raw.items()
        }
        return cls(
            dataset_id=str(payload.get("dataset_id", "")),
            version=str(payload.get("version", "")),
            phase=str(payload.get("phase", "")),
            source_uri=str(payload.get("source_uri", "")),
            output_uri=str(payload.get("output_uri", "")),
            status=str(payload.get("status", "RUNNING")),
            completed_steps=list(payload.get("completed_steps") or []),
            files=files,
            metrics=dict(payload.get("metrics") or {}),
            schema_version=str(payload.get("schema_version", CHECKPOINT_SCHEMA_VERSION)),
            updated_at=str(payload.get("updated_at", _utcnow_iso())),
        )

    def mark_step(self, step: str) -> None:
        if step not in self.completed_steps:
            self.completed_steps.append(step)
        self.updated_at = _utcnow_iso()

    def upsert_file(self, file: CheckpointFile) -> None:
        self.files[file.name] = file
        self.updated_at = _utcnow_iso()

    def has_step(self, step: str) -> bool:
        return step in self.completed_steps

    def file_status(self, name: str) -> str:
        f = self.files.get(name)
        return f.status if f is not None else "PENDING"


class CheckpointStore:
    """LakeStore 之上的 checkpoint I/O 包装。"""

    def __init__(self, *, output_uri: str, store: LakeStore | None = None) -> None:
        self._output_uri = output_uri
        self._store = store or create_lake_store(output_uri)

    @property
    def checkpoint_uri(self) -> str:
        return join_uri(self._output_uri, CHECKPOINT_FILENAME)

    def exists(self) -> bool:
        return self._store.exists(self.checkpoint_uri)

    def load(self) -> Checkpoint | None:
        if not self.exists():
            return None
        try:
            payload = self._store.read_json(self.checkpoint_uri)
            return Checkpoint.from_dict(payload)
        except Exception as err:
            LOG.warning("checkpoint load failed (treat as missing): %s", err)
            return None

    def save(self, checkpoint: Checkpoint) -> str:
        checkpoint.updated_at = _utcnow_iso()
        return self._store.write_json(self.checkpoint_uri, checkpoint.to_dict())


def load_checkpoint(output_uri: str) -> Checkpoint | None:
    """便捷读取；输出目录不存在或无 checkpoint 时返回 None。"""
    return CheckpointStore(output_uri=output_uri).load()


def save_checkpoint(output_uri: str, checkpoint: Checkpoint) -> str:
    return CheckpointStore(output_uri=output_uri).save(checkpoint)
