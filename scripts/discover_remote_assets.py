#!/usr/bin/env python3
"""
discover_remote_assets.py

WSL 侧替代云端脚本 /opt/robot-dh-infra/scripts/20_list_remote_assets.sh。

本地代理阻断 SSH 但数据面（MinIO 9000、PostgreSQL 5432、Redis 6379）仍直连时使用。
产出 JSON 清单：应用 MinIO 账号可见 bucket、各 bucket 顶层 prefix、
robot-lake/{raw,ods,dwd,ads,lineage,tmp}/ 存在性与部分布局、
robot-datasets/raw/{dataset_id}/{version}/ 存在性与样例列表。

凭据来自 ~/.config/robot-dh/robot-dh-lake.env（或进程环境变量）。

Usage:
  scripts/discover_remote_assets.py [--out docs/remote_assets_<ts>.local-discovered.json]
"""
from __future__ import annotations

import argparse
import datetime as _dt
import json
import os
import sys
from pathlib import Path
from typing import Any

import boto3
from botocore.client import BaseClient
from botocore.client import Config as BotoConfig

REPO_ROOT = Path(__file__).resolve().parent.parent
ENV_PATH = Path(os.environ.get("ROBOT_DH_LAKE_ENV", str(Path.home() / ".config/robot-dh/robot-dh-lake.env")))

LAKE_LAYERS = ("raw", "ods", "dwd", "ads", "lineage", "tmp")


def load_env_file(path: Path) -> None:
    if not path.is_file():
        return
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):]
        if "=" not in line:
            continue
        k, _, v = line.partition("=")
        k = k.strip()
        v = v.strip().strip("'").strip('"')
        os.environ.setdefault(k, v)


def make_s3_client() -> BaseClient:
    endpoint = os.environ.get("ROBOT_DH_S3_ENDPOINT_URL")
    access = os.environ.get("ROBOT_DH_S3_ACCESS_KEY")
    secret = os.environ.get("ROBOT_DH_S3_SECRET_KEY")
    region = os.environ.get("ROBOT_DH_S3_REGION", "us-east-1")
    if not (endpoint and access and secret):
        raise SystemExit(
            "[discover] missing ROBOT_DH_S3_ENDPOINT_URL / ACCESS_KEY / SECRET_KEY in env"
        )
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access,
        aws_secret_access_key=secret,
        region_name=region,
        config=BotoConfig(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def list_top_level(s3: BaseClient, bucket: str, prefix: str = "") -> dict[str, Any]:
    """返回 common_prefixes（目录）与 sample_keys（前 <=20 个 key）。"""
    paginator = s3.get_paginator("list_objects_v2")
    common: list[str] = []
    samples: list[dict] = []
    total_objects = 0
    total_bytes = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []) or []:
            common.append(cp["Prefix"])
        for obj in page.get("Contents", []) or []:
            total_objects += 1
            total_bytes += int(obj.get("Size", 0))
            if len(samples) < 20:
                samples.append(
                    {
                        "key": obj["Key"],
                        "size": int(obj.get("Size", 0)),
                        "last_modified": obj.get("LastModified").isoformat()
                        if obj.get("LastModified")
                        else None,
                    }
                )
    return {
        "common_prefixes": sorted(common),
        "object_count": total_objects,
        "byte_count": total_bytes,
        "sample_objects": samples,
    }


def deep_scan_bucket(s3: BaseClient, bucket: str, max_objects: int = 200) -> dict[str, Any]:
    """扁平扫描整个 bucket，对象数上限 max_objects。"""
    paginator = s3.get_paginator("list_objects_v2")
    out: list[dict] = []
    total_objects = 0
    total_bytes = 0
    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get("Contents", []) or []:
            total_objects += 1
            total_bytes += int(obj.get("Size", 0))
            if len(out) < max_objects:
                out.append(
                    {
                        "key": obj["Key"],
                        "size": int(obj.get("Size", 0)),
                        "last_modified": obj.get("LastModified").isoformat()
                        if obj.get("LastModified")
                        else None,
                    }
                )
    return {"total_objects": total_objects, "total_bytes": total_bytes, "objects": out}


def discover_datasets(s3, data_bucket: str) -> list[dict]:
    """在 robot-datasets 下枚举 raw/{dataset_id}/{version}/ 元组。"""
    out: list[dict] = []
    raw_prefix = "raw/"
    top = list_top_level(s3, data_bucket, raw_prefix)
    for ds_prefix in top["common_prefixes"]:
        dataset_id = ds_prefix.removeprefix(raw_prefix).rstrip("/")
        versions = list_top_level(s3, data_bucket, ds_prefix)
        for ver_prefix in versions["common_prefixes"]:
            version = ver_prefix.removeprefix(ds_prefix).rstrip("/")
            ver_inv = list_top_level(s3, data_bucket, ver_prefix)
            sample = deep_scan_bucket_under_prefix(s3, data_bucket, ver_prefix, max_objects=50)
            out.append(
                {
                    "source": f"{data_bucket}/raw",
                    "dataset_id": dataset_id,
                    "version": version,
                    "prefix": ver_prefix,
                    "object_count": sample["total_objects"],
                    "byte_count": sample["total_bytes"],
                    "top_level": ver_inv["common_prefixes"],
                    "sample_objects": sample["objects"][:20],
                }
            )
        if not versions["common_prefixes"]:
            ver_inv = list_top_level(s3, data_bucket, ds_prefix)
            out.append(
                {
                    "source": f"{data_bucket}/raw",
                    "dataset_id": dataset_id,
                    "version": None,
                    "prefix": ds_prefix,
                    "object_count": ver_inv["object_count"],
                    "byte_count": ver_inv["byte_count"],
                    "sample_objects": ver_inv["sample_objects"],
                }
            )
    return out


def deep_scan_bucket_under_prefix(
    s3: BaseClient, bucket: str, prefix: str, max_objects: int = 200
) -> dict[str, Any]:
    paginator = s3.get_paginator("list_objects_v2")
    out: list[dict] = []
    total_objects = 0
    total_bytes = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []) or []:
            total_objects += 1
            total_bytes += int(obj.get("Size", 0))
            if len(out) < max_objects:
                out.append(
                    {
                        "key": obj["Key"],
                        "size": int(obj.get("Size", 0)),
                        "last_modified": obj.get("LastModified").isoformat()
                        if obj.get("LastModified")
                        else None,
                    }
                )
    return {"total_objects": total_objects, "total_bytes": total_bytes, "objects": out}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=None, help="output JSON path")
    parser.add_argument("--max-objects-per-bucket", type=int, default=200)
    args = parser.parse_args()

    load_env_file(ENV_PATH)
    s3 = make_s3_client()

    inventory: dict = {
        "discovered_at": _dt.datetime.now(_dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "discovered_by": "scripts/discover_remote_assets.py (WSL receiver-side)",
        "endpoint_url": os.environ.get("ROBOT_DH_S3_ENDPOINT_URL"),
        "expected_buckets": {
            "data": os.environ.get("ROBOT_DH_S3_DATA_BUCKET", "robot-datasets"),
            "artifact": os.environ.get("ROBOT_DH_S3_ARTIFACT_BUCKET", "robot-dh-artifacts"),
            "lake": os.environ.get("ROBOT_DH_S3_LAKE_BUCKET", "robot-lake"),
        },
        "buckets_visible": [],
        "lake_layers": {},
        "datasets": [],
        "artifacts_top_level": None,
    }

    resp = s3.list_buckets()
    visible = [b["Name"] for b in resp.get("Buckets", [])]
    inventory["buckets_visible"] = visible

    data_bucket = inventory["expected_buckets"]["data"]
    artifact_bucket = inventory["expected_buckets"]["artifact"]
    lake_bucket = inventory["expected_buckets"]["lake"]

    if lake_bucket in visible:
        for layer in LAKE_LAYERS:
            try:
                layer_inv = list_top_level(s3, lake_bucket, f"{layer}/")
                inventory["lake_layers"][layer] = layer_inv
            except Exception as e:
                inventory["lake_layers"][layer] = {"error": str(e)}
    else:
        inventory["lake_layers"] = {"_error": f"bucket {lake_bucket} not visible to this account"}

    if data_bucket in visible:
        inventory["datasets"] = discover_datasets(s3, data_bucket)
    else:
        inventory["datasets"] = [
            {"_error": f"bucket {data_bucket} not visible to this account"}
        ]

    if artifact_bucket in visible:
        inventory["artifacts_top_level"] = list_top_level(s3, artifact_bucket, "")
    else:
        inventory["artifacts_top_level"] = {"_error": f"bucket {artifact_bucket} not visible"}

    if args.out is None:
        ts = _dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        args.out = REPO_ROOT / f"docs/remote_assets_{ts}.local-discovered.json"

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(inventory, indent=2, sort_keys=False, ensure_ascii=False))

    print(f"[discover] wrote {args.out}")
    print(f"[discover] buckets_visible = {visible}")
    for layer, inv in inventory["lake_layers"].items():
        if isinstance(inv, dict) and "object_count" in inv:
            print(
                f"[discover] lake/{layer:<7} objects={inv['object_count']:>5}  "
                f"top_level={inv['common_prefixes'][:3]}"
            )
    for ds in inventory["datasets"]:
        if isinstance(ds, dict) and ds.get("dataset_id"):
            print(
                f"[discover] dataset {ds['dataset_id']:<24}  version={ds.get('version'):<20}  "
                f"objects={ds.get('object_count')}"
            )
    return 0


if __name__ == "__main__":
    sys.exit(main())
