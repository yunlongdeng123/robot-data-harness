"""train / val / test split：按 dataset_id 分层，按比例切。"""

from __future__ import annotations

import hashlib
from typing import Any

import pandas as pd


SPLIT_NAMES = ("train", "val", "test")


def _hash_to_unit_interval(key: str) -> float:
    h = hashlib.sha256(key.encode()).digest()
    n = int.from_bytes(h[:8], "big")
    return n / 2**64


def build_split(
    df: pd.DataFrame,
    *,
    split: tuple[float, float, float] = (0.8, 0.1, 0.1),
    key_columns: tuple[str, ...] = ("dataset_id", "episode_id"),
    seed: str = "v1",
) -> pd.DataFrame:
    """根据 (dataset_id, episode_id) hash 决定每条记录落到 train/val/test。"""
    if abs(sum(split) - 1.0) > 1e-6:
        raise ValueError(f"split must sum to 1.0; got {split}")
    out = df.copy()
    if out.empty:
        out["split"] = pd.Series(dtype="object")
        return out

    train_p, val_p, _test_p = split
    val_cum = train_p + val_p

    def _assign(row: pd.Series) -> str:
        key_parts = [str(row.get(k, "")) for k in key_columns] + [seed]
        u = _hash_to_unit_interval("|".join(key_parts))
        if u < train_p:
            return "train"
        if u < val_cum:
            return "val"
        return "test"

    out["split"] = out.apply(_assign, axis=1)
    return out
