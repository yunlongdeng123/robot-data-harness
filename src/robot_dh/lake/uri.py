"""Lake URI 解析与拼接。

v1.4 ETL 统一支持三种 URI：
    - 裸本地路径：./samples/foo
    - file:// URI：file:///abs/path 或 file://samples/foo
    - s3:// URI：s3://bucket/key/sub

目标：LakeUri 含 .scheme/.bucket/.key/.local_path/.uri；s3 与本地路径 round-trip 可预期；
S3 key 不做 URL 编码（字面量，调用方须避免 ? # 等）。
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Final

S3_SCHEME: Final[str] = "s3"
FILE_SCHEME: Final[str] = "file"
LOCAL_SCHEME: Final[str] = "local"


@dataclass(frozen=True, slots=True)
class LakeUri:
    """已解析的数据湖 URI。

    scheme : "s3" | "local"
    bucket : s3 非空，本地为 ""
    key    : S3 key（无前导 /）或 POSIX 本地路径
    local_path : 本地绝对或工作区相对路径（仅 local 有意义）
    uri    : 规范化重建串，可 str() 或再 parse_uri
    """

    scheme: str
    bucket: str
    key: str
    local_path: str
    uri: str

    @property
    def is_s3(self) -> bool:
        return self.scheme == S3_SCHEME

    @property
    def is_local(self) -> bool:
        return self.scheme == LOCAL_SCHEME

    def as_path(self) -> Path:
        if not self.is_local:
            raise ValueError(f"as_path() is only valid for local URIs; got {self.uri}")
        return Path(self.local_path)


def is_s3_uri(uri: str) -> bool:
    if not isinstance(uri, str):
        return False
    return uri.startswith("s3://")


def is_file_uri(uri: str) -> bool:
    """是否以 file:// 开头（仅 v1.7 起 local-first 流程做严格区分时使用）。

    与 ``is_local_uri`` 的区别：``is_local_uri`` 把"裸路径 ./foo / /abs/foo"也算
    local，而 ``is_file_uri`` 只识别显式的 ``file://`` URI；适合需要"显式声明
    是不是用户主动指本地"的场景。
    """
    if not isinstance(uri, str):
        return False
    return uri.startswith("file://")


def is_local_uri(uri: str) -> bool:
    return not is_s3_uri(uri)


def to_local_path(uri: str) -> Path:
    """把 file:// / 裸本地路径 转成 ``Path``。s3:// 抛 ``ValueError``。"""
    parsed = parse_uri(uri)
    if not parsed.is_local:
        raise ValueError(f"to_local_path() requires a local URI; got {uri}")
    return Path(parsed.local_path)


def to_file_uri(path: str | Path) -> str:
    """把任意本地路径/Path 规范化成 ``file:///abs/...``，便于跨进程传递。"""
    p = Path(str(path)).expanduser()
    try:
        resolved = p.resolve()
    except OSError:
        resolved = p
    posix = resolved.as_posix()
    if not posix.startswith("/"):
        # Windows 等绝对路径不带前导斜杠时补一个，保持 RFC8089 形式。
        posix = "/" + posix
    return f"file://{posix}"


def _strip_double_slashes(key: str) -> str:
    while "//" in key:
        key = key.replace("//", "/")
    return key.strip("/")


def parse_uri(uri: str) -> LakeUri:
    if not isinstance(uri, str) or uri == "":
        raise ValueError("uri must be a non-empty string")

    if uri.startswith("s3://"):
        rest = uri[len("s3://"):]
        if "/" in rest:
            bucket, key = rest.split("/", 1)
        else:
            bucket, key = rest, ""
        if bucket == "":
            raise ValueError(f"Invalid s3 URI (empty bucket): {uri}")
        key = _strip_double_slashes(key)
        canonical = f"s3://{bucket}/{key}" if key else f"s3://{bucket}/"
        return LakeUri(
            scheme=S3_SCHEME,
            bucket=bucket,
            key=key,
            local_path="",
            uri=canonical,
        )

    if uri.startswith("file://"):
        path_part = uri[len("file://"):]
        if path_part.startswith("/"):
            local = path_part
        else:
            local = path_part
        if local == "":
            raise ValueError(f"Invalid file URI (empty path): {uri}")
    else:
        local = uri

    return LakeUri(
        scheme=LOCAL_SCHEME,
        bucket="",
        key=local,
        local_path=local,
        uri=local,
    )


def join_uri(base: str, *parts: str) -> str:
    """按 POSIX 语义拼接 lake URI 段；s3 固定 bucket，后续段追加到 key 下。"""
    parsed = parse_uri(base)
    cleaned_parts: list[str] = []
    for part in parts:
        if part is None:
            continue
        text = str(part).strip()
        if text == "":
            continue
        cleaned_parts.append(text.strip("/"))

    if parsed.is_s3:
        suffix = "/".join(cleaned_parts)
        key = parsed.key
        if key and suffix:
            combined = f"{key}/{suffix}"
        elif suffix:
            combined = suffix
        else:
            combined = key
        combined = _strip_double_slashes(combined)
        return f"s3://{parsed.bucket}/{combined}" if combined else f"s3://{parsed.bucket}/"

    suffix = "/".join(cleaned_parts)
    base_local = parsed.local_path.rstrip("/")
    if base_local and suffix:
        combined = f"{base_local}/{suffix}"
    elif suffix:
        combined = suffix
    else:
        combined = base_local
    return combined
