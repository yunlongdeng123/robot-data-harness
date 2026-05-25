"""quality filter：threshold / status / family / min_episode_length 过滤。"""

from __future__ import annotations

from typing import Any

import pandas as pd


def build_quality_filter(
    *,
    quality_threshold: float = 80.0,
    excluded_status: list[str] | None = None,
    split: tuple[float, float, float] = (0.8, 0.1, 0.1),
    family_filter: list[str] | None = None,
    min_episode_length: int | None = None,
    exclude_failed_contract: bool = True,
) -> dict[str, Any]:
    rules: list[dict[str, Any]] = []
    rules.append({"rule": "quality_score >= threshold", "threshold": quality_threshold})
    rules.append({"rule": "status not in excluded", "excluded": list(excluded_status or ["FAIL"])})
    if family_filter:
        rules.append({"rule": "dataset_family in allowed", "allowed": list(family_filter)})
    if min_episode_length is not None:
        rules.append({"rule": "num_samples >= min", "min": int(min_episode_length)})
    if exclude_failed_contract:
        rules.append({"rule": "qc_status != FAIL"})
    return {
        "quality_threshold": float(quality_threshold),
        "excluded_status": list(excluded_status or ["FAIL"]),
        "split": {"train": float(split[0]), "val": float(split[1]), "test": float(split[2])},
        "family_filter": list(family_filter or []),
        "min_episode_length": int(min_episode_length) if min_episode_length is not None else None,
        "exclude_failed_contract": bool(exclude_failed_contract),
        "rules": rules,
    }


def apply_quality_filter(
    df: pd.DataFrame,
    *,
    quality_threshold: float = 80.0,
    excluded_status: list[str] | None = None,
    family_filter: list[str] | None = None,
    min_episode_length: int | None = None,
    exclude_failed_contract: bool = True,
) -> pd.DataFrame:
    excluded = set(excluded_status or ["FAIL"])
    out = df.copy()

    if "quality_score" in out.columns:
        out = out[out["quality_score"].fillna(-1.0) >= float(quality_threshold)]
    if "quality_status" in out.columns:
        out = out[~out["quality_status"].isin(excluded)]
    if family_filter and "dataset_family" in out.columns:
        out = out[out["dataset_family"].isin(family_filter)]
    if min_episode_length is not None and "num_samples" in out.columns:
        out = out[out["num_samples"] >= int(min_episode_length)]
    if exclude_failed_contract and "qc_status" in out.columns:
        out = out[out["qc_status"] != "FAIL"]
    return out.reset_index(drop=True)
