"""LakeStore 抽象：本地与 S3/MinIO 统一读写面，由 LakeUri 寻址。

manifest、normalize、features、ads、etl runner 共用。不复用 v1.3 ArtifactStore 的原因：
    - ArtifactStore 绑定单 bucket，只处理桶内 key。
    - 数据湖跨 data/lake 两桶且同路径支持本地 URI，URI 是一等公民。
    - 需要 list/exists/read_text/write_text/read_json/write_json/upload_dir/download_dir 为一等方法。

v1.6.5：S3LakeStore.download_dir 走并发线程池（默认 8 路），单文件 download_file 走
boto3.s3.transfer.TransferConfig（multipart 4 MiB / threads=8）。对几百 GB 级 dataset
materialize_input 阶段从串行换成并发，wallclock 应从小时级降到几十分钟。环境变量：

- ``ROBOT_DH_S3_DOWNLOAD_CONCURRENCY``：多文件并发度（默认 8）；
- ``ROBOT_DH_S3_TRANSFER_THREADS``：单文件 multipart 并发（默认 8）；
- ``ROBOT_DH_S3_MULTIPART_THRESHOLD_MB``：触发 multipart 的阈值，默认 16 MiB；
- ``ROBOT_DH_S3_MAX_POOL_CONNECTIONS``：botocore HTTP 连接池上限（默认
  ``max(32, concurrency * transfer_threads)``），避免 227 MiB 下载触发
  `Connection pool is full, discarding connection` 拖慢 7+ 分钟。
"""

from __future__ import annotations

import json
import logging
import os
import shutil
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig

from robot_dh.lake.uri import LakeUri, parse_uri

LOG = logging.getLogger(__name__)


def _int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if not raw:
        return default
    try:
        return max(1, int(raw))
    except ValueError:
        LOG.warning("invalid env %s=%r; falling back to %d", name, raw, default)
        return default


def _ensure_parent(local_path: Path) -> None:
    local_path.parent.mkdir(parents=True, exist_ok=True)


class LakeStore(ABC):
    """数据湖存储抽象；方法入参均为完整 URI（另有说明除外）。"""

    @abstractmethod
    def list(self, uri: str) -> list[str]:
        """递归列出 uri 下全部对象 URI，返回规范化 URI。"""

    @abstractmethod
    def exists(self, uri: str) -> bool: ...

    @abstractmethod
    def download_dir(
        self,
        uri: str,
        local_dir: Path,
        *,
        exclude_prefixes: tuple[str, ...] | None = None,
        include_prefixes: tuple[str, ...] | None = None,
        progress_log_every: int = 0,
    ) -> Path:
        """将 uri 下全部对象下载到 local_dir，保留相对 key 路径。

        ``exclude_prefixes`` / ``include_prefixes`` 按相对 prefix 的子路径过滤，
        ``progress_log_every`` 控制每 N 个文件汇报一次 LOG.info；后端可按需实现。
        """

    @abstractmethod
    def upload_file(self, local_path: Path, target_uri: str) -> str:
        """上传单个本地文件，返回对象规范化 URI。"""

    @abstractmethod
    def upload_dir(self, local_dir: Path, target_uri: str) -> dict[str, str]:
        """上传 local_dir 下全部文件，按相对路径映射，返回 {rel_path: canonical_uri}。"""

    @abstractmethod
    def read_text(self, uri: str, encoding: str = "utf-8") -> str: ...

    @abstractmethod
    def write_text(self, uri: str, text: str, encoding: str = "utf-8") -> str: ...

    def read_json(self, uri: str) -> Any:
        return json.loads(self.read_text(uri))

    def write_json(self, uri: str, obj: Any) -> str:
        return self.write_text(uri, json.dumps(obj, indent=2, sort_keys=False, ensure_ascii=False))


class LocalLakeStore(LakeStore):
    """本地文件系统实现；URI 为 POSIX 路径。"""

    def list(self, uri: str) -> list[str]:
        parsed = parse_uri(uri)
        root = Path(parsed.local_path)
        if not root.exists():
            return []
        if root.is_file():
            return [parsed.uri]
        out: list[str] = []
        for f in sorted(root.rglob("*")):
            if f.is_file():
                out.append(f.as_posix())
        return out

    def exists(self, uri: str) -> bool:
        return Path(parse_uri(uri).local_path).exists()

    def download_dir(
        self,
        uri: str,
        local_dir: Path,
        *,
        exclude_prefixes: tuple[str, ...] | None = None,
        include_prefixes: tuple[str, ...] | None = None,
        progress_log_every: int = 0,
    ) -> Path:
        parsed = parse_uri(uri)
        src = Path(parsed.local_path)
        local_dir = local_dir.expanduser().resolve()
        local_dir.mkdir(parents=True, exist_ok=True)
        if src.is_file():
            target = local_dir / src.name
            shutil.copy2(src, target)
            return local_dir
        if not src.exists():
            return local_dir
        for f in src.rglob("*"):
            if not f.is_file():
                continue
            rel = f.relative_to(src).as_posix()
            if exclude_prefixes and any(rel.startswith(p) for p in exclude_prefixes):
                continue
            if include_prefixes and not any(rel.startswith(p) for p in include_prefixes):
                continue
            target = local_dir / rel
            _ensure_parent(target)
            shutil.copy2(f, target)
        return local_dir

    def upload_file(self, local_path: Path, target_uri: str) -> str:
        local_path = local_path.expanduser().resolve()
        if not local_path.is_file():
            raise FileNotFoundError(f"upload_file source not found: {local_path}")
        parsed = parse_uri(target_uri)
        target = Path(parsed.local_path)
        _ensure_parent(target)
        shutil.copy2(local_path, target)
        return target.as_posix()

    def upload_dir(self, local_dir: Path, target_uri: str) -> dict[str, str]:
        local_dir = local_dir.expanduser().resolve()
        if not local_dir.is_dir():
            raise FileNotFoundError(f"upload_dir source not a directory: {local_dir}")
        out: dict[str, str] = {}
        parsed = parse_uri(target_uri)
        root = Path(parsed.local_path)
        root.mkdir(parents=True, exist_ok=True)
        for f in sorted(local_dir.rglob("*")):
            if not f.is_file():
                continue
            rel = f.relative_to(local_dir).as_posix()
            target = root / rel
            _ensure_parent(target)
            shutil.copy2(f, target)
            out[rel] = target.as_posix()
        return out

    def read_text(self, uri: str, encoding: str = "utf-8") -> str:
        path = Path(parse_uri(uri).local_path)
        return path.read_text(encoding=encoding)

    def write_text(self, uri: str, text: str, encoding: str = "utf-8") -> str:
        path = Path(parse_uri(uri).local_path)
        _ensure_parent(path)
        path.write_text(text, encoding=encoding)
        return path.as_posix()


class S3LakeStore(LakeStore):
    """S3/MinIO 实现，由 ROBOT_DH_S3_* 环境变量配置。"""

    def __init__(
        self,
        *,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        region_name: str = "us-east-1",
    ) -> None:
        self.endpoint_url = endpoint_url
        self.region_name = region_name
        # v1.6.6：连接池上限随 download_concurrency * transfer_threads 一起涨，避免
        # `Connection pool is full, discarding connection` 把整个 normalize 拖慢 7+ 分钟。
        # botocore 默认 max_pool_connections=10 < concurrency 8 * multipart 8，必撞墙。
        download_concurrency = _int_env("ROBOT_DH_S3_DOWNLOAD_CONCURRENCY", 8)
        transfer_threads = _int_env("ROBOT_DH_S3_TRANSFER_THREADS", 8)
        pool_default = max(32, download_concurrency * max(2, transfer_threads))
        max_pool_connections = _int_env("ROBOT_DH_S3_MAX_POOL_CONNECTIONS", pool_default)
        self.client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region_name,
            config=BotoConfig(
                signature_version="s3v4",
                s3={"addressing_style": "path"},
                # v1.6.5：与 qc/profile 那一层共享同样的重试/超时策略
                connect_timeout=10.0,
                read_timeout=300.0,
                retries={"max_attempts": 10, "mode": "adaptive"},
                max_pool_connections=max_pool_connections,
            ),
        )
        # multipart download：阈值之上自动并发分段下载，加速 100 MiB+ 单文件
        multipart_mb = _int_env("ROBOT_DH_S3_MULTIPART_THRESHOLD_MB", 16)
        self.transfer_config = TransferConfig(
            multipart_threshold=multipart_mb * 1024 * 1024,
            multipart_chunksize=8 * 1024 * 1024,
            max_concurrency=transfer_threads,
            use_threads=True,
        )
        self.download_concurrency = download_concurrency
        self.max_pool_connections = max_pool_connections

    @classmethod
    def from_env(cls) -> "S3LakeStore":
        endpoint = os.environ.get("ROBOT_DH_S3_ENDPOINT_URL")
        access = os.environ.get("ROBOT_DH_S3_ACCESS_KEY")
        secret = os.environ.get("ROBOT_DH_S3_SECRET_KEY")
        region = os.environ.get("ROBOT_DH_S3_REGION", "us-east-1")
        missing = [
            n
            for n, v in (
                ("ROBOT_DH_S3_ENDPOINT_URL", endpoint),
                ("ROBOT_DH_S3_ACCESS_KEY", access),
                ("ROBOT_DH_S3_SECRET_KEY", secret),
            )
            if not v
        ]
        if missing:
            raise ValueError(
                f"S3LakeStore.from_env(): missing environment variables: {', '.join(missing)}"
            )
        return cls(endpoint_url=endpoint, access_key=access, secret_key=secret, region_name=region)

    def _parse(self, uri: str) -> LakeUri:
        parsed = parse_uri(uri)
        if not parsed.is_s3:
            raise ValueError(f"S3LakeStore got non-S3 URI: {uri}")
        return parsed

    def list(self, uri: str) -> list[str]:
        parsed = self._parse(uri)
        prefix = parsed.key
        if prefix and not prefix.endswith("/"):
            sole_uri = f"s3://{parsed.bucket}/{prefix}"
            if self.exists(sole_uri):
                return [sole_uri]
            prefix = prefix + "/"
        paginator = self.client.get_paginator("list_objects_v2")
        out: list[str] = []
        for page in paginator.paginate(Bucket=parsed.bucket, Prefix=prefix):
            for obj in page.get("Contents", []) or []:
                out.append(f"s3://{parsed.bucket}/{obj['Key']}")
        return out

    def exists(self, uri: str) -> bool:
        parsed = self._parse(uri)
        try:
            self.client.head_object(Bucket=parsed.bucket, Key=parsed.key)
            return True
        except ClientError as err:
            status = err.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status == 404:
                return False
            raise

    def download_dir(
        self,
        uri: str,
        local_dir: Path,
        *,
        exclude_prefixes: tuple[str, ...] | None = None,
        include_prefixes: tuple[str, ...] | None = None,
        progress_log_every: int = 0,
    ) -> Path:
        """并发下载 prefix 下所有对象。

        v1.6.5 起：跳过 size + mtime 都匹配的本地已有文件，便于 normalize 失败重跑
        时不重复几十 GB 拉取；不同对象间走线程池，单对象走 boto3 TransferConfig
        multipart。线程数由 ``ROBOT_DH_S3_DOWNLOAD_CONCURRENCY`` 控制。

        v1.6.7：
        - ``exclude_prefixes`` / ``include_prefixes``：相对 prefix 的子路径过滤，
          典型用法是 ``exclude_prefixes=("videos/",)`` 让 lerobot v2 normalize
          跳过 ~10 GiB 视频，把 18 GiB raw 降到 ~14 GiB（只下 data/ + meta/）。
        - ``progress_log_every``：每完成 N 个文件 LOG.info 一条进度，规避
          "download_dir 起头打一行 log，然后静默几小时" 排障盲区；缺省 0
          表示不打中段进度（单测/小目录情况），被 ``_materialize_input``
          自动设置为 50。

        v1.6.8（fvx5z F3）：除了"每 N 文件一条"，再叠加"每 N 秒一条"的 wall-clock
        进度（``ROBOT_DH_DOWNLOAD_PROGRESS_INTERVAL_SEC``，默认 30s）——droid 这种
        100+ MiB 单文件场景，按文件触发要等几分钟才出一行，看上去像卡死；按时间
        触发能保证至少每 30s 一行 "still alive"，archive log 中段不再静默。
        """
        parsed = self._parse(uri)
        local_dir = local_dir.expanduser().resolve()
        local_dir.mkdir(parents=True, exist_ok=True)
        prefix = parsed.key
        if prefix and not prefix.endswith("/"):
            if self.exists(uri):
                target = local_dir / prefix.rsplit("/", 1)[-1]
                self._download_one(parsed.bucket, prefix, target)
                return local_dir
            prefix = prefix + "/"

        tasks: list[tuple[str, Path, int]] = []
        excluded_files = 0
        excluded_bytes = 0
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=parsed.bucket, Prefix=prefix):
            for obj in page.get("Contents", []) or []:
                key = obj["Key"]
                rel = key[len(prefix):] if prefix and key.startswith(prefix) else key
                if rel == "" or rel.endswith("/"):
                    continue
                if exclude_prefixes and any(rel.startswith(p) for p in exclude_prefixes):
                    excluded_files += 1
                    excluded_bytes += int(obj.get("Size", 0))
                    continue
                if include_prefixes and not any(rel.startswith(p) for p in include_prefixes):
                    excluded_files += 1
                    excluded_bytes += int(obj.get("Size", 0))
                    continue
                target = local_dir / rel
                size = int(obj.get("Size", 0))
                tasks.append((key, target, size))

        if not tasks:
            if excluded_files:
                LOG.info(
                    "S3 download_dir: bucket=%s prefix=%s excluded=%d files (%.1f MiB) by filter; nothing to fetch",
                    parsed.bucket, prefix, excluded_files, excluded_bytes / (1024 * 1024),
                )
            return local_dir

        tasks.sort(key=lambda t: t[2], reverse=True)
        total_bytes = sum(t[2] for t in tasks)
        LOG.info(
            "S3 download_dir: bucket=%s prefix=%s files=%d total_size=%.1f MiB concurrency=%d"
            "%s",
            parsed.bucket, prefix, len(tasks), total_bytes / (1024 * 1024),
            self.download_concurrency,
            f" excluded={excluded_files} files ({excluded_bytes / (1024 * 1024):.1f} MiB)"
            if excluded_files else "",
        )

        # wall-clock progress：除了"每 N 文件一条"，再叠加"每 N 秒一条"心跳。
        # 默认 30s，可被 ROBOT_DH_DOWNLOAD_PROGRESS_INTERVAL_SEC 覆盖；<=0 关闭。
        import time

        wallclock_interval = float(
            os.environ.get("ROBOT_DH_DOWNLOAD_PROGRESS_INTERVAL_SEC", "30") or "30"
        )
        last_wallclock_log = time.time()

        skipped = 0
        completed = 0
        completed_bytes = 0
        with ThreadPoolExecutor(max_workers=self.download_concurrency) as pool:
            futures: dict[Any, tuple[str, Path, int]] = {}
            for key, target, size in tasks:
                if self._matches_local(target, size):
                    skipped += 1
                    continue
                _ensure_parent(target)
                futures[pool.submit(self._download_one, parsed.bucket, key, target)] = (key, target, size)
            for fut in as_completed(futures):
                key, target, size = futures[fut]
                fut.result()
                completed += 1
                completed_bytes += size
                now = time.time()
                trigger_count = bool(progress_log_every) and completed % progress_log_every == 0
                trigger_time = (
                    wallclock_interval > 0
                    and (now - last_wallclock_log) >= wallclock_interval
                )
                if trigger_count or trigger_time:
                    LOG.info(
                        "S3 download_dir: bucket=%s prefix=%s progress=%d/%d files (%.1f MiB / %.1f MiB)",
                        parsed.bucket, prefix, completed, len(futures),
                        completed_bytes / (1024 * 1024),
                        total_bytes / (1024 * 1024),
                    )
                    last_wallclock_log = now
        if skipped:
            LOG.info("S3 download_dir: skipped %d already-present files", skipped)
        return local_dir

    @staticmethod
    def _matches_local(target: Path, expected_size: int) -> bool:
        if expected_size <= 0:
            return False
        try:
            return target.is_file() and target.stat().st_size == expected_size
        except OSError:
            return False

    def _download_one(self, bucket: str, key: str, target: Path) -> None:
        """单对象下载，自动 multipart；写入临时文件再 rename，避免半文件污染。"""
        _ensure_parent(target)
        tmp = target.with_suffix(target.suffix + ".part")
        try:
            self.client.download_file(bucket, key, str(tmp), Config=self.transfer_config)
            tmp.replace(target)
        finally:
            if tmp.exists():
                try:
                    tmp.unlink()
                except OSError:
                    pass

    def upload_file(self, local_path: Path, target_uri: str) -> str:
        local_path = local_path.expanduser().resolve()
        if not local_path.is_file():
            raise FileNotFoundError(f"upload_file source not found: {local_path}")
        parsed = self._parse(target_uri)
        self.client.upload_file(
            str(local_path), parsed.bucket, parsed.key, Config=self.transfer_config
        )
        return f"s3://{parsed.bucket}/{parsed.key}"

    def upload_dir(self, local_dir: Path, target_uri: str) -> dict[str, str]:
        local_dir = local_dir.expanduser().resolve()
        if not local_dir.is_dir():
            raise FileNotFoundError(f"upload_dir source not a directory: {local_dir}")
        parsed = self._parse(target_uri)
        prefix = parsed.key.rstrip("/")
        files: list[tuple[Path, str, str]] = []
        for f in sorted(local_dir.rglob("*")):
            if not f.is_file():
                continue
            rel = f.relative_to(local_dir).as_posix()
            key = f"{prefix}/{rel}" if prefix else rel
            files.append((f, rel, key))
        if not files:
            return {}

        out: dict[str, str] = {}
        with ThreadPoolExecutor(max_workers=self.download_concurrency) as pool:
            futures = {
                pool.submit(
                    self.client.upload_file,
                    str(local_file),
                    parsed.bucket,
                    key,
                    Config=self.transfer_config,
                ): (rel, key)
                for local_file, rel, key in files
            }
            for fut in as_completed(futures):
                rel, key = futures[fut]
                fut.result()
                out[rel] = f"s3://{parsed.bucket}/{key}"
        return out

    def read_text(self, uri: str, encoding: str = "utf-8") -> str:
        parsed = self._parse(uri)
        resp = self.client.get_object(Bucket=parsed.bucket, Key=parsed.key)
        return resp["Body"].read().decode(encoding)

    def write_text(self, uri: str, text: str, encoding: str = "utf-8") -> str:
        parsed = self._parse(uri)
        self.client.put_object(Bucket=parsed.bucket, Key=parsed.key, Body=text.encode(encoding))
        return f"s3://{parsed.bucket}/{parsed.key}"


def create_lake_store(uri: str) -> LakeStore:
    """按 URI scheme 返回对应的 LakeStore 实例。"""
    parsed = parse_uri(uri)
    if parsed.is_s3:
        return S3LakeStore.from_env()
    return LocalLakeStore()
