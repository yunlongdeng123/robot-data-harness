"""perf record pending 存储：远端 etl_perf_runs schema 漂移时的兜底落盘 + S3 mirror。

背景见 `docs/history/v1_6_etl_perf_runs_schema_align_request.md`。当 `WarehouseService.record_etl_perf_run`
抛 `V15SchemaMissingError`（infra 端 PG 表缺列）时，调用方不再 abort 整个 ETL step，
而是把 record 写到本地 pending 目录 + best-effort 同步到 S3
`s3://robot-dh-artifacts/perf-records-pending/...`，等 infra 跑完 migration 后再用
`robot-dh perf reingest-pending` 批量回灌。

本地写入是强约束（失败必须 raise），S3 mirror 是 best-effort（失败仅 warning）。
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from pathlib import Path
from typing import Any

LOG = logging.getLogger(__name__)

# 默认落盘位置：在容器里通常 ROBOT_DH_PERF_PENDING_DIR 会被 override 到 /tmp 或 emptyDir 卷
DEFAULT_LOCAL_PENDING_DIR = Path.home() / ".cache" / "robot-dh" / "perf-records-pending"
DEFAULT_LOCAL_ARCHIVE_DIR = Path.home() / ".cache" / "robot-dh" / "perf-records-archived"
DEFAULT_S3_PENDING_PREFIX = "perf-records-pending"
DEFAULT_S3_ARCHIVE_PREFIX = "perf-records-archived"

PENDING_DIR_ENV = "ROBOT_DH_PERF_PENDING_DIR"
ARCHIVE_DIR_ENV = "ROBOT_DH_PERF_ARCHIVE_DIR"


def _safe_segment(value: Any, *, fallback: str) -> str:
    """把任意值转成安全的路径段（仅 alnum + `-_.`）。空值用 fallback。"""
    text = str(value).strip() if value is not None else ""
    if not text:
        text = fallback
    return "".join(ch if (ch.isalnum() or ch in ("-", "_", ".")) else "_" for ch in text)


def resolve_local_pending_dir(explicit: Path | None = None) -> Path:
    if explicit is not None:
        return explicit.expanduser()
    raw = os.environ.get(PENDING_DIR_ENV)
    if raw:
        return Path(raw).expanduser()
    return DEFAULT_LOCAL_PENDING_DIR


def resolve_local_archive_dir(explicit: Path | None = None) -> Path:
    if explicit is not None:
        return explicit.expanduser()
    raw = os.environ.get(ARCHIVE_DIR_ENV)
    if raw:
        return Path(raw).expanduser()
    return DEFAULT_LOCAL_ARCHIVE_DIR


def _record_payload(record: Any) -> dict[str, Any]:
    if hasattr(record, "to_dict"):
        return dict(record.to_dict())
    return dict(record)


def _pending_relative_key(payload: dict[str, Any], salt: str) -> str:
    dataset_id = _safe_segment(payload.get("dataset_id"), fallback="_unknown_dataset")
    version = _safe_segment(payload.get("version"), fallback="_unknown_version")
    phase = _safe_segment(payload.get("phase"), fallback="_unknown_phase")
    job_id = _safe_segment(payload.get("job_id"), fallback=f"job-{salt}")
    return f"{dataset_id}/{version}/{phase}/{job_id}-{salt}.json"


class PendingPerfStore:
    """本地必落 + 可选 S3 mirror 的 pending perf record 存储。

    线程不安全（同一 store 实例不要跨线程并发 emit），但 ETL step 每进程只创建一次即可。
    """

    def __init__(
        self,
        *,
        local_dir: Path | None = None,
        s3_client: Any = None,
        s3_bucket: str | None = None,
        s3_pending_prefix: str = DEFAULT_S3_PENDING_PREFIX,
    ) -> None:
        self._local_dir = resolve_local_pending_dir(local_dir)
        self._s3_client = s3_client
        self._s3_bucket = s3_bucket
        self._s3_pending_prefix = s3_pending_prefix.rstrip("/")

    @classmethod
    def from_env(cls, *, local_dir: Path | None = None) -> "PendingPerfStore":
        """从环境变量构造；S3 配置不全时退化为纯本地。"""
        s3_client: Any = None
        s3_bucket: str | None = None
        try:
            from robot_dh.artifacts.s3 import S3ArtifactStore

            store = S3ArtifactStore.from_env()
            s3_client = store.client
            s3_bucket = store.bucket
        except Exception as err:  # 包括缺 ENV / boto3 / 网络
            LOG.debug("S3 artifact store unavailable for pending perf store: %s", err)
        return cls(local_dir=local_dir, s3_client=s3_client, s3_bucket=s3_bucket)

    @property
    def local_dir(self) -> Path:
        return self._local_dir

    @property
    def s3_uri_prefix(self) -> str | None:
        if not self._s3_bucket:
            return None
        return f"s3://{self._s3_bucket}/{self._s3_pending_prefix}"

    def emit(self, record: Any, *, reason: str) -> dict[str, str]:
        """落一条 pending record；本地必须成功，S3 失败只 warning。

        返回 {"local": <path>, "s3": <uri or "skipped">}。
        """
        payload = _record_payload(record)
        payload["_pending"] = {"reason": str(reason)}
        salt = uuid.uuid4().hex[:8]
        rel_key = _pending_relative_key(payload, salt)

        local_path = self._local_dir / rel_key
        local_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False))

        result: dict[str, str] = {"local": str(local_path), "s3": "skipped"}
        if self._s3_client and self._s3_bucket:
            s3_key = f"{self._s3_pending_prefix}/{rel_key}"
            try:
                self._s3_client.upload_file(str(local_path), self._s3_bucket, s3_key)
                result["s3"] = f"s3://{self._s3_bucket}/{s3_key}"
            except Exception as err:
                LOG.warning(
                    "pending perf record local-only (S3 mirror failed for s3://%s/%s): %s",
                    self._s3_bucket,
                    s3_key,
                    err,
                )
        return result


def list_pending_files(pending_dir: Path) -> list[Path]:
    """按 mtime 升序列出所有 *.json，便于按发生顺序回灌。"""
    if not pending_dir.is_dir():
        return []
    return sorted(pending_dir.rglob("*.json"), key=lambda p: p.stat().st_mtime)


def archive_pending_file(
    src: Path,
    *,
    pending_dir: Path,
    archive_dir: Path,
    s3_client: Any = None,
    s3_bucket: str | None = None,
    s3_pending_prefix: str = DEFAULT_S3_PENDING_PREFIX,
    s3_archive_prefix: str = DEFAULT_S3_ARCHIVE_PREFIX,
) -> Path:
    """把一条 pending record move 到 archive 目录；同步 S3（best-effort）。返回 archive 后的本地路径。"""
    rel = src.relative_to(pending_dir)
    archive_path = archive_dir / rel
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    src.replace(archive_path)
    _cleanup_empty_dirs(src.parent, stop_at=pending_dir)
    if s3_client and s3_bucket:
        rel_posix = rel.as_posix()
        pending_key = f"{s3_pending_prefix.rstrip('/')}/{rel_posix}"
        archive_key = f"{s3_archive_prefix.rstrip('/')}/{rel_posix}"
        try:
            s3_client.copy_object(
                Bucket=s3_bucket,
                Key=archive_key,
                CopySource={"Bucket": s3_bucket, "Key": pending_key},
            )
            s3_client.delete_object(Bucket=s3_bucket, Key=pending_key)
        except Exception as err:
            LOG.warning(
                "archive S3 mirror failed for s3://%s/%s -> %s: %s",
                s3_bucket,
                pending_key,
                archive_key,
                err,
            )
    return archive_path


def _cleanup_empty_dirs(start: Path, *, stop_at: Path) -> None:
    """move 后递归清空目录树，避免 pending_dir 残留一堆空 dataset_id/version/phase 子目录。"""
    current = start
    stop_resolved = stop_at.resolve()
    while True:
        try:
            current_resolved = current.resolve()
        except FileNotFoundError:
            return
        if current_resolved == stop_resolved:
            return
        try:
            current.rmdir()
        except OSError:
            return
        current = current.parent
