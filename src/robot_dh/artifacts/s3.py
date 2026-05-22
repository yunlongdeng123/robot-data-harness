from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import urlparse

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from robot_dh.artifacts.base import ArtifactStore


def _normalize_key(artifact_path: str) -> str:
    return artifact_path.strip("/")


def _split_s3_uri(artifact_uri: str) -> tuple[str, str]:
    parsed = urlparse(artifact_uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError(f"Unsupported S3 artifact URI: {artifact_uri}")
    return parsed.netloc, parsed.path.lstrip("/")


class S3ArtifactStore(ArtifactStore):
    def __init__(
        self,
        *,
        bucket: str,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        region_name: str = "us-east-1",
    ) -> None:
        self.bucket = bucket
        self.endpoint_url = endpoint_url
        self.region_name = region_name
        self.client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region_name,
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        )

    @classmethod
    def from_env(cls) -> "S3ArtifactStore":
        endpoint_url = os.environ.get("ROBOT_DH_S3_ENDPOINT_URL")
        access_key = os.environ.get("ROBOT_DH_S3_ACCESS_KEY")
        secret_key = os.environ.get("ROBOT_DH_S3_SECRET_KEY")
        bucket = os.environ.get("ROBOT_DH_S3_ARTIFACT_BUCKET", "robot-dh-artifacts")
        region_name = os.environ.get("ROBOT_DH_S3_REGION", "us-east-1")
        missing = [
            name
            for name, value in (
                ("ROBOT_DH_S3_ENDPOINT_URL", endpoint_url),
                ("ROBOT_DH_S3_ACCESS_KEY", access_key),
                ("ROBOT_DH_S3_SECRET_KEY", secret_key),
            )
            if not value
        ]
        if missing:
            raise ValueError(f"Missing S3 artifact store environment variables: {', '.join(missing)}")
        return cls(
            bucket=bucket,
            endpoint_url=endpoint_url,
            access_key=access_key,
            secret_key=secret_key,
            region_name=region_name,
        )

    def put_file(self, local_path: Path, artifact_path: str) -> str:
        local_path = local_path.expanduser().resolve()
        if not local_path.exists() or not local_path.is_file():
            raise FileNotFoundError(f"Artifact file not found: {local_path}")
        key = _normalize_key(artifact_path)
        self.client.upload_file(str(local_path), self.bucket, key)
        return f"s3://{self.bucket}/{key}"

    def put_dir(self, local_dir: Path, artifact_prefix: str) -> dict[str, str]:
        local_dir = local_dir.expanduser().resolve()
        if not local_dir.exists() or not local_dir.is_dir():
            raise FileNotFoundError(f"Artifact directory not found: {local_dir}")
        uploaded: dict[str, str] = {}
        prefix = _normalize_key(artifact_prefix)
        for source_path in sorted(local_dir.rglob("*")):
            if not source_path.is_file():
                continue
            relative_path = source_path.relative_to(local_dir).as_posix()
            key = f"{prefix}/{relative_path}" if prefix else relative_path
            uploaded[relative_path] = self.put_file(source_path, key)
        return uploaded

    def get_file(self, artifact_uri: str, local_path: Path) -> Path:
        bucket, key = _split_s3_uri(artifact_uri)
        local_path = local_path.expanduser().resolve()
        local_path.parent.mkdir(parents=True, exist_ok=True)
        self.client.download_file(bucket, key, str(local_path))
        return local_path

    def exists(self, artifact_uri: str) -> bool:
        bucket, key = _split_s3_uri(artifact_uri)
        try:
            self.client.head_object(Bucket=bucket, Key=key)
        except ClientError as error:
            status_code = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status_code == 404:
                return False
            raise
        return True