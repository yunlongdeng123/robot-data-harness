"""universal contract：与 dataset_family 无关的基础检查（schema / readability / temporal）。"""

from __future__ import annotations

from typing import Any

from robot_dh.qc.base import Rule
from robot_dh.qc.profile import profile_dataset, AssetProfile


UNIVERSAL_RULES = [
    Rule(rule_id="object_count_min", metric="files_count", op=">=", threshold=1, severity="fail",
         description="dataset 至少要有一个文件"),
    Rule(rule_id="empty_files_count", metric="empty_file_count", op="<=", threshold=0, severity="warn",
         description="zero-byte critical file"),
    Rule(rule_id="parquet_readable_rate", metric="readable_parquet_rate", op=">=", threshold=0.95, severity="warn"),
    Rule(rule_id="hdf5_readable_rate", metric="readable_hdf5_rate", op=">=", threshold=0.95, severity="warn"),
    Rule(rule_id="video_decode_pass_rate", metric="video_decode_pass_rate", op=">=", threshold=0.9, severity="warn"),
]


def universal_metrics(profile: AssetProfile) -> dict[str, Any]:
    """从 AssetProfile 派生 universal 指标。"""
    parquet = profile.profile.get("parquet") or []
    hdf5 = profile.profile.get("hdf5") or []
    video = profile.profile.get("video") or []

    files_count = profile.files_count or 0
    empty_count = sum(
        1 for items in (parquet, hdf5, video)
        for it in items
        if int(it.get("size_bytes") or 0) == 0
    )

    def rate(items: list[dict[str, Any]], key: str) -> float | None:
        if not items:
            return None
        passed = sum(1 for it in items if it.get(key))
        return float(passed) / len(items)

    return {
        "object_count": files_count,
        "total_bytes": int(profile.bytes or 0),
        "files_count": files_count,
        "empty_file_count": empty_count,
        "readable_parquet_count": sum(1 for p in parquet if p.get("readable")),
        "unreadable_parquet_count": sum(1 for p in parquet if not p.get("readable")),
        "readable_hdf5_count": sum(1 for p in hdf5 if p.get("readable")),
        "unreadable_hdf5_count": sum(1 for p in hdf5 if not p.get("readable")),
        "readable_video_count": sum(1 for p in video if p.get("readable")),
        "unreadable_video_count": sum(1 for p in video if not p.get("readable")),
        "readable_parquet_rate": rate(parquet, "readable") if parquet else 1.0,
        "readable_hdf5_rate": rate(hdf5, "readable") if hdf5 else 1.0,
        "video_decode_pass_rate": rate(video, "readable") if video else 1.0,
        "json_parse_error_count": 0,
        "timestamp_nonmonotonic_rate": 0.0,
    }


def run_universal(
    dataset_uri: str,
    *,
    dataset_id: str | None = None,
    version: str | None = None,
    dataset_family: str | None = None,
) -> tuple[AssetProfile, dict[str, Any]]:
    profile = profile_dataset(
        dataset_uri=dataset_uri,
        dataset_id=dataset_id,
        version=version,
        dataset_family=dataset_family or "universal",
    )
    return profile, universal_metrics(profile)
