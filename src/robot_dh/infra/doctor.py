from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Iterable

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
import redis
from sqlalchemy import inspect, text

from robot_dh.registry import get_db_backend, get_engine, init_db, resolve_db_uri


_SUPPORTED_CHECKS = {"db", "s3", "redis", "lake"}


def parse_check_list(value: str | None) -> list[str]:
    if value is None or value.strip() == "":
        return ["db", "s3", "redis", "lake"]
    checks = [item.strip().lower() for item in value.split(",") if item.strip()]
    invalid = sorted(set(checks) - _SUPPORTED_CHECKS)
    if invalid:
        raise ValueError(f"Unsupported infra checks: {', '.join(invalid)}")
    return checks


def _status_payload(name: str, status: str, **fields: Any) -> dict[str, Any]:
    return {"name": name, "status": status, **fields}


def _check_db(db_uri: str | None) -> dict[str, Any]:
    try:
        resolved_uri = resolve_db_uri(db_uri)
        init_db(resolved_uri)
        engine = get_engine(resolved_uri)
        backend = get_db_backend(resolved_uri)
        with engine.connect() as connection:
            ping_value = connection.execute(text("SELECT 1")).scalar_one()
        tables = sorted(inspect(engine).get_table_names())
        return _status_payload(
            "db",
            "PASS",
            backend=backend,
            db_uri=resolved_uri,
            ping=int(ping_value),
            tables=tables,
        )
    except Exception as error:  # pragma: no cover - exercised in failure paths
        return _status_payload("db", "FAIL", error=str(error))


def _make_s3_client() -> tuple[object | None, dict[str, str] | None, dict[str, Any] | None]:
    endpoint = os.environ.get("ROBOT_DH_S3_ENDPOINT_URL")
    access_key = os.environ.get("ROBOT_DH_S3_ACCESS_KEY")
    secret_key = os.environ.get("ROBOT_DH_S3_SECRET_KEY")
    region = os.environ.get("ROBOT_DH_S3_REGION", "us-east-1")
    artifact_bucket = os.environ.get("ROBOT_DH_S3_ARTIFACT_BUCKET", "robot-dh-artifacts")
    data_bucket = os.environ.get("ROBOT_DH_S3_DATA_BUCKET", "robot-datasets")
    if not endpoint or not access_key or not secret_key:
        return None, None, _status_payload(
            "s3",
            "SKIP",
            reason="S3 environment variables are not configured",
        )
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )
    buckets = {"artifact_bucket": artifact_bucket, "data_bucket": data_bucket}
    return client, buckets, None


def _check_s3() -> dict[str, Any]:
    client, buckets, skipped = _make_s3_client()
    if skipped is not None:
        return skipped
    assert client is not None
    assert buckets is not None
    try:
        for bucket in buckets.values():
            client.head_bucket(Bucket=bucket)
        return _status_payload("s3", "PASS", endpoint=os.environ.get("ROBOT_DH_S3_ENDPOINT_URL"), **buckets)
    except (ClientError, BotoCoreError) as error:
        return _status_payload("s3", "FAIL", error=str(error), **buckets)


def _check_lake() -> dict[str, Any]:
    """v1.4 检查：lake bucket head + 6 个标准 layer prefix。"""
    endpoint = os.environ.get("ROBOT_DH_S3_ENDPOINT_URL")
    access_key = os.environ.get("ROBOT_DH_S3_ACCESS_KEY")
    secret_key = os.environ.get("ROBOT_DH_S3_SECRET_KEY")
    lake_bucket = os.environ.get("ROBOT_DH_S3_LAKE_BUCKET")
    if not endpoint or not access_key or not secret_key or not lake_bucket:
        return _status_payload(
            "lake",
            "SKIP",
            reason="lake env vars not configured (ROBOT_DH_S3_LAKE_BUCKET, ROBOT_DH_S3_*)",
        )
    region = os.environ.get("ROBOT_DH_S3_REGION", "us-east-1")
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )
    try:
        client.head_bucket(Bucket=lake_bucket)
    except (ClientError, BotoCoreError) as error:
        return _status_payload("lake", "FAIL", error=str(error), lake_bucket=lake_bucket)

    layer_counts: dict[str, int] = {}
    for layer in ("raw", "ods", "dwd", "ads", "lineage", "tmp"):
        try:
            resp = client.list_objects_v2(Bucket=lake_bucket, Prefix=f"{layer}/", MaxKeys=1)
            layer_counts[layer] = int(resp.get("KeyCount", 0))
        except (ClientError, BotoCoreError) as error:
            return _status_payload(
                "lake",
                "FAIL",
                error=f"list {layer}/ failed: {error}",
                lake_bucket=lake_bucket,
            )
    return _status_payload(
        "lake",
        "PASS",
        lake_bucket=lake_bucket,
        endpoint=endpoint,
        layer_keycounts=layer_counts,
    )


def _check_redis() -> dict[str, Any]:
    redis_url = os.environ.get("ROBOT_DH_REDIS_URL")
    if not redis_url:
        return _status_payload("redis", "SKIP", reason="ROBOT_DH_REDIS_URL is not configured")
    try:
        client = redis.Redis.from_url(redis_url)
        ping_response = client.ping()
        return _status_payload("redis", "PASS", redis_url=redis_url, ping=bool(ping_response))
    except Exception as error:  # pragma: no cover - exercised in failure paths
        return _status_payload("redis", "FAIL", error=str(error), redis_url=redis_url)


def _overall_status(results: Iterable[dict[str, Any]]) -> str:
    statuses = {item["status"] for item in results}
    if "FAIL" in statuses:
        return "FAIL"
    return "PASS"


def run_infra_doctor(*, checks: list[str] | None = None, db_uri: str | None = None) -> dict[str, Any]:
    selected_checks = checks or ["db", "s3", "redis", "lake"]
    runners = {
        "db": lambda: _check_db(db_uri),
        "s3": _check_s3,
        "redis": _check_redis,
        "lake": _check_lake,
    }
    results = [runners[name]() for name in selected_checks]
    return {
        "status": _overall_status(results),
        "checked": selected_checks,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "results": results,
    }


def render_doctor_human(payload: dict[str, Any]) -> str:
    lines = [f"infra doctor: {payload['status']}"]
    for result in payload.get("results", []):
        summary_parts = [f"[{result['status']}] {result['name']}"]
        if "backend" in result:
            summary_parts.append(f"backend={result['backend']}")
        if "error" in result:
            summary_parts.append(f"error={result['error']}")
        if "reason" in result:
            summary_parts.append(f"reason={result['reason']}")
        if result["name"] == "s3" and result["status"] == "PASS":
            summary_parts.append(f"endpoint={result['endpoint']}")
        if result["name"] == "lake" and result["status"] == "PASS":
            summary_parts.append(f"bucket={result['lake_bucket']}")
            counts = result.get("layer_keycounts", {})
            if counts:
                summary_parts.append(
                    "layers=" + ",".join(f"{k}:{v}" for k, v in counts.items())
                )
        lines.append(" ".join(summary_parts))
    return "\n".join(lines)