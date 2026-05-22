"""LakeStore 抽象：本地与 S3/MinIO 统一读写面，由 LakeUri 寻址。

manifest、normalize、features、ads、etl runner 共用。不复用 v1.3 ArtifactStore 的原因：
    - ArtifactStore 绑定单 bucket，只处理桶内 key。
    - 数据湖跨 data/lake 两桶且同路径支持本地 URI，URI 是一等公民。
    - 需要 list/exists/read_text/write_text/read_json/write_json/upload_dir/download_dir 为一等方法。
"""

from __future__ import annotations

import json
import os
import shutil
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from robot_dh.lake.uri import LakeUri, parse_uri


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
    def download_dir(self, uri: str, local_dir: Path) -> Path:
        """将 uri 下全部对象下载到 local_dir，保留相对 key 路径。"""

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

    def download_dir(self, uri: str, local_dir: Path) -> Path:
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
            if f.is_file():
                rel = f.relative_to(src)
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
        self.client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region_name,
            config=BotoConfig(signature_version="s3v4", s3={"addressing_style": "path"}),
        )

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

    def download_dir(self, uri: str, local_dir: Path) -> Path:
        parsed = self._parse(uri)
        local_dir = local_dir.expanduser().resolve()
        local_dir.mkdir(parents=True, exist_ok=True)
        prefix = parsed.key
        if prefix and not prefix.endswith("/"):
            if self.exists(uri):
                target = local_dir / prefix.rsplit("/", 1)[-1]
                self.client.download_file(parsed.bucket, prefix, str(target))
                return local_dir
            prefix = prefix + "/"
        paginator = self.client.get_paginator("list_objects_v2")
        any_found = False
        for page in paginator.paginate(Bucket=parsed.bucket, Prefix=prefix):
            for obj in page.get("Contents", []) or []:
                any_found = True
                key = obj["Key"]
                rel = key[len(prefix):] if prefix and key.startswith(prefix) else key
                if rel == "" or rel.endswith("/"):
                    continue
                target = local_dir / rel
                _ensure_parent(target)
                self.client.download_file(parsed.bucket, key, str(target))
        if not any_found:
            return local_dir
        return local_dir

    def upload_file(self, local_path: Path, target_uri: str) -> str:
        local_path = local_path.expanduser().resolve()
        if not local_path.is_file():
            raise FileNotFoundError(f"upload_file source not found: {local_path}")
        parsed = self._parse(target_uri)
        self.client.upload_file(str(local_path), parsed.bucket, parsed.key)
        return f"s3://{parsed.bucket}/{parsed.key}"

    def upload_dir(self, local_dir: Path, target_uri: str) -> dict[str, str]:
        local_dir = local_dir.expanduser().resolve()
        if not local_dir.is_dir():
            raise FileNotFoundError(f"upload_dir source not a directory: {local_dir}")
        parsed = self._parse(target_uri)
        prefix = parsed.key.rstrip("/")
        out: dict[str, str] = {}
        for f in sorted(local_dir.rglob("*")):
            if not f.is_file():
                continue
            rel = f.relative_to(local_dir).as_posix()
            key = f"{prefix}/{rel}" if prefix else rel
            self.client.upload_file(str(f), parsed.bucket, key)
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
