"""s3fs / botocore 共享配置：lazy parquet 读取 + materialize-first 下载。

v1.6 QC profile 之前的实现是先把整个对象 download 到 /tmp 再 probe，82 MiB 的 droid parquet
和 798 MiB 的 robomimic HDF5 在弱网下会顶到 botocore 默认 60s read_timeout，最终被吞成
"Max Retries Exceeded" 这种没法排障的字串。本模块负责两件事：

1. 暴露一个共享的 ``get_s3fs()``：用 `s3fs.S3FileSystem` + 显式 retry/timeout，配合
   PyArrow 的 `pq.ParquetFile(fs.open(uri))` 只读 footer，QC parquet profile 真正 lazy。
2. 暴露一个共享的 ``get_s3_boto_client()``：botocore retries='adaptive' + 10 次重试 +
   连接/读超时显式给值。HDF5 没有靠谱的 cloud-native reader，沿用 materialize-first，但
   download 阶段稳得多。

环境变量来源沿用 `S3LakeStore.from_env()`：`ROBOT_DH_S3_ENDPOINT_URL` / `_ACCESS_KEY` /
`_SECRET_KEY` / `_REGION`。模块级单例缓存（按 endpoint）避免每次新建连接池。
"""

from __future__ import annotations

import os
import threading
from typing import Any
from urllib.parse import urlparse

import boto3
from botocore.config import Config as BotoConfig

# 共享 retry/timeout 默认值；与 S3LakeStore 用同一组数字，便于联调时统一调参。
DEFAULT_CONNECT_TIMEOUT = 10.0
DEFAULT_READ_TIMEOUT = 300.0
DEFAULT_MAX_ATTEMPTS = 10
DEFAULT_RETRY_MODE = "adaptive"
DEFAULT_MAX_POOL_CONNECTIONS = 64

# v1.6.8（fvx5z F1/F2）"fast" 档：用于
#   1) bridge enrichment 这种**单次只读 footer + 一列**的轻量调用；
#   2) hdf5 probe materialize-first 单文件 download。
# 想避免：默认档 `read_timeout=300s × max_attempts=10 + adaptive token bucket` 在
# 弱网下会让单次 GET 卡 30+ 分钟（fvx5z bridge-qc duration=1849s 的根因）。
# fast 档 worst case：connect 5s + read 10/60s × max_attempts 3 = 单调用 30s/180s 上限。
FAST_CONNECT_TIMEOUT = 5.0
FAST_READ_TIMEOUT_BRIDGE = 10.0
FAST_READ_TIMEOUT_HDF5 = 60.0
FAST_MAX_ATTEMPTS = 3
FAST_RETRY_MODE = "standard"


def _max_pool_connections() -> int:
    raw = os.environ.get("ROBOT_DH_S3_MAX_POOL_CONNECTIONS")
    if not raw:
        return DEFAULT_MAX_POOL_CONNECTIONS
    try:
        return max(1, int(raw))
    except ValueError:
        return DEFAULT_MAX_POOL_CONNECTIONS


_S3FS_LOCK = threading.Lock()
_S3FS_CACHE: dict[str, Any] = {}

_BOTO_LOCK = threading.Lock()
_BOTO_CACHE: dict[str, Any] = {}

# fast 档的独立缓存（按 endpoint+region+access_key+档位名 keying）
_S3FS_FAST_LOCK = threading.Lock()
_S3FS_FAST_CACHE: dict[str, Any] = {}

_BOTO_FAST_LOCK = threading.Lock()
_BOTO_FAST_CACHE: dict[str, Any] = {}


def _env_or_raise(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(
            f"missing env var {name}; QC s3 lazy probe requires "
            "ROBOT_DH_S3_ENDPOINT_URL / _ACCESS_KEY / _SECRET_KEY to be set"
        )
    return value


def _s3_cache_key(endpoint: str, region: str, access_key: str) -> str:
    return f"{endpoint}|{region}|{access_key}"


def get_s3fs() -> Any:
    """返回共享 s3fs.S3FileSystem 实例。

    s3fs 内部 client 不是线程安全的，但同进程内对同 endpoint 共享一个实例是 OK 的：
    PyArrow 使用 `fs.open(uri, "rb")` 拿到的是独立 file handle。
    """
    import s3fs  # 延迟 import：测试环境可能不装

    endpoint = _env_or_raise("ROBOT_DH_S3_ENDPOINT_URL")
    access_key = _env_or_raise("ROBOT_DH_S3_ACCESS_KEY")
    secret_key = _env_or_raise("ROBOT_DH_S3_SECRET_KEY")
    region = os.environ.get("ROBOT_DH_S3_REGION", "us-east-1")
    key = _s3_cache_key(endpoint, region, access_key)
    with _S3FS_LOCK:
        cached = _S3FS_CACHE.get(key)
        if cached is not None:
            return cached
        fs = s3fs.S3FileSystem(
            key=access_key,
            secret=secret_key,
            client_kwargs={
                "endpoint_url": endpoint,
                "region_name": region,
            },
            config_kwargs={
                "connect_timeout": DEFAULT_CONNECT_TIMEOUT,
                "read_timeout": DEFAULT_READ_TIMEOUT,
                "retries": {
                    "max_attempts": DEFAULT_MAX_ATTEMPTS,
                    "mode": DEFAULT_RETRY_MODE,
                },
                "signature_version": "s3v4",
                "s3": {"addressing_style": "path"},
                "max_pool_connections": _max_pool_connections(),
            },
        )
        _S3FS_CACHE[key] = fs
        return fs


def get_s3_boto_client(*, region: str | None = None) -> Any:
    """返回共享 boto3 s3 client，带 retry/timeout，用于 HDF5 materialize-first 下载。"""

    endpoint = _env_or_raise("ROBOT_DH_S3_ENDPOINT_URL")
    access_key = _env_or_raise("ROBOT_DH_S3_ACCESS_KEY")
    secret_key = _env_or_raise("ROBOT_DH_S3_SECRET_KEY")
    region = region or os.environ.get("ROBOT_DH_S3_REGION", "us-east-1")
    key = _s3_cache_key(endpoint, region, access_key)
    with _BOTO_LOCK:
        cached = _BOTO_CACHE.get(key)
        if cached is not None:
            return cached
        client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
            config=BotoConfig(
                connect_timeout=DEFAULT_CONNECT_TIMEOUT,
                read_timeout=DEFAULT_READ_TIMEOUT,
                retries={
                    "max_attempts": DEFAULT_MAX_ATTEMPTS,
                    "mode": DEFAULT_RETRY_MODE,
                },
                signature_version="s3v4",
                s3={"addressing_style": "path"},
                max_pool_connections=_max_pool_connections(),
            ),
        )
        _BOTO_CACHE[key] = client
        return client


def get_s3fs_fast(*, read_timeout: float = FAST_READ_TIMEOUT_BRIDGE) -> Any:
    """fast 档 s3fs：用于 bridge enrichment 这种"超时 30s 内必须放手"的轻量 footer 读。

    与默认 ``get_s3fs()`` 的差别：``connect_timeout`` 5s、``read_timeout`` 10s、
    ``max_attempts`` 3、``mode='standard'``（不走 adaptive token bucket）。worst-case
    单次 GET 不超过 30s。``read_timeout`` 可被 caller 微调（hdf5 probe 用 60s）。
    """
    import s3fs

    endpoint = _env_or_raise("ROBOT_DH_S3_ENDPOINT_URL")
    access_key = _env_or_raise("ROBOT_DH_S3_ACCESS_KEY")
    secret_key = _env_or_raise("ROBOT_DH_S3_SECRET_KEY")
    region = os.environ.get("ROBOT_DH_S3_REGION", "us-east-1")
    cache_key = f"{endpoint}|{region}|{access_key}|fast|{read_timeout}"
    with _S3FS_FAST_LOCK:
        cached = _S3FS_FAST_CACHE.get(cache_key)
        if cached is not None:
            return cached
        fs = s3fs.S3FileSystem(
            key=access_key,
            secret=secret_key,
            client_kwargs={
                "endpoint_url": endpoint,
                "region_name": region,
            },
            config_kwargs={
                "connect_timeout": FAST_CONNECT_TIMEOUT,
                "read_timeout": float(read_timeout),
                "retries": {
                    "max_attempts": FAST_MAX_ATTEMPTS,
                    "mode": FAST_RETRY_MODE,
                },
                "signature_version": "s3v4",
                "s3": {"addressing_style": "path"},
                "max_pool_connections": _max_pool_connections(),
            },
        )
        _S3FS_FAST_CACHE[cache_key] = fs
        return fs


def get_s3_boto_client_fast(
    *,
    read_timeout: float = FAST_READ_TIMEOUT_HDF5,
    region: str | None = None,
) -> Any:
    """fast 档 boto3 client：HDF5 probe materialize-first 用，单文件 worst-case
    ``connect 5s + read 60s × max_attempts 3 = 195s`` 而不是默认 ``300s × 10 = 3000s``。

    qc-contract-run 整 step 的 ``activeDeadlineSeconds`` 1800s 下，26 文件 ÷ 4 并发
    × 195s ≈ 1267s，留 9 分钟富裕给 contract aggregate / report write。
    """
    endpoint = _env_or_raise("ROBOT_DH_S3_ENDPOINT_URL")
    access_key = _env_or_raise("ROBOT_DH_S3_ACCESS_KEY")
    secret_key = _env_or_raise("ROBOT_DH_S3_SECRET_KEY")
    region = region or os.environ.get("ROBOT_DH_S3_REGION", "us-east-1")
    cache_key = f"{endpoint}|{region}|{access_key}|fast|{read_timeout}"
    with _BOTO_FAST_LOCK:
        cached = _BOTO_FAST_CACHE.get(cache_key)
        if cached is not None:
            return cached
        client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
            config=BotoConfig(
                connect_timeout=FAST_CONNECT_TIMEOUT,
                read_timeout=float(read_timeout),
                retries={
                    "max_attempts": FAST_MAX_ATTEMPTS,
                    "mode": FAST_RETRY_MODE,
                },
                signature_version="s3v4",
                s3={"addressing_style": "path"},
                max_pool_connections=_max_pool_connections(),
            ),
        )
        _BOTO_FAST_CACHE[cache_key] = client
        return client


def split_s3_uri(uri: str) -> tuple[str, str]:
    """``s3://bucket/key/path`` -> (bucket, key/path)。"""
    parsed = urlparse(uri)
    if parsed.scheme != "s3":
        raise ValueError(f"not an s3 URI: {uri}")
    return parsed.netloc, parsed.path.lstrip("/")


def reset_cache() -> None:
    """测试用：清掉模块级 client 缓存。"""
    with _S3FS_LOCK:
        _S3FS_CACHE.clear()
    with _BOTO_LOCK:
        _BOTO_CACHE.clear()
    with _S3FS_FAST_LOCK:
        _S3FS_FAST_CACHE.clear()
    with _BOTO_FAST_LOCK:
        _BOTO_FAST_CACHE.clear()
