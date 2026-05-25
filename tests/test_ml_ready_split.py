"""ml-ready split：比例与稳定性。"""

from __future__ import annotations

import pandas as pd
import pytest

from robot_dh.ml_ready.split import build_split


def test_split_proportions_close_to_target() -> None:
    rows = [
        {"dataset_id": "demo", "episode_id": f"ep_{i}", "x": i}
        for i in range(1000)
    ]
    df = pd.DataFrame(rows)
    out = build_split(df, split=(0.8, 0.1, 0.1), seed="v1")
    counts = out["split"].value_counts()
    assert {"train", "val", "test"}.issubset(counts.index)
    assert abs(counts["train"] / len(out) - 0.8) < 0.05
    assert abs(counts["val"] / len(out) - 0.1) < 0.03
    assert abs(counts["test"] / len(out) - 0.1) < 0.03


def test_split_stable_for_same_keys() -> None:
    df = pd.DataFrame([{"dataset_id": "demo", "episode_id": f"e{i}"} for i in range(50)])
    out1 = build_split(df, split=(0.8, 0.1, 0.1), seed="v1")
    out2 = build_split(df, split=(0.8, 0.1, 0.1), seed="v1")
    assert (out1["split"].values == out2["split"].values).all()


def test_split_invalid_proportions() -> None:
    df = pd.DataFrame([{"dataset_id": "demo", "episode_id": "x"}])
    with pytest.raises(ValueError):
        build_split(df, split=(0.5, 0.3, 0.3))
