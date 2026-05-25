"""partition planner：local demo / fake LeRobot / fake robomimic 切片。"""

from __future__ import annotations

from pathlib import Path

import h5py
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from robot_dh.partition import detect_dataset_family, plan_dataset_partitions
from robot_dh.partition.models import PartitionPlan


def _write_parquet(path: Path, n_rows: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame({"x": list(range(n_rows)), "y": [float(i) for i in range(n_rows)]})
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), path)


def _write_hdf5(path: Path, n_groups: int = 2) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with h5py.File(path, "w") as f:
        for i in range(n_groups):
            g = f.create_group(f"data/demo_{i}")
            g.create_dataset("actions", data=[[1.0]] * 10)


def test_detect_family_robomimic(tmp_path: Path) -> None:
    root = tmp_path / "robomimic_scale30/v1"
    _write_hdf5(root / "low_dim.hdf5")
    _write_hdf5(root / "image.hdf5")
    plan = plan_dataset_partitions(
        dataset_uri=root.as_posix(),
        dataset_id="robomimic_scale30",
        version="v1",
        target_partition_size_gb=1e-9,  # 强制每个文件一个 partition
    )
    assert plan.dataset_family == "robomimic"
    assert plan.partition_type == "hdf5_file"
    assert len(plan.partitions) == 2
    assert sum(p.input_bytes for p in plan.partitions) == plan.total_input_bytes


def test_detect_family_lerobot(tmp_path: Path) -> None:
    root = tmp_path / "droid_lerobot_scale30/v1"
    _write_parquet(root / "data" / "chunk-001.parquet", 100)
    _write_parquet(root / "data" / "chunk-002.parquet", 200)
    (root / "videos").mkdir(parents=True, exist_ok=True)
    (root / "videos" / "ep_0.mp4").write_bytes(b"\x00\x00")
    plan = plan_dataset_partitions(
        dataset_uri=root.as_posix(),
        dataset_id="droid_lerobot_scale30",
        version="v1",
        target_partition_size_gb=1e-9,
    )
    assert plan.dataset_family in ("lerobot", "droid"), plan.dataset_family
    assert plan.partition_type == "parquet_file"
    assert len(plan.partitions) >= 2


def test_local_demo_returns_single_partition(tmp_path: Path) -> None:
    """endpose.pt demo -> single partition 兜底（family=demo）。"""
    root = tmp_path / "demo_button_press/v1"
    root.mkdir(parents=True, exist_ok=True)
    (root / "endpose.pt").write_bytes(b"\x00" * 16)
    (root / "meta.yaml").write_text("dataset_id: demo\nversion: v1\n")
    plan = plan_dataset_partitions(
        dataset_uri=root.as_posix(),
        dataset_id="demo",
        version="v1",
        target_partition_size_gb=1.0,
    )
    assert plan.dataset_family == "demo"
    assert plan.partition_type == "single"
    assert len(plan.partitions) == 1


def test_partition_plan_round_trip(tmp_path: Path) -> None:
    root = tmp_path / "robomimic/v1"
    _write_hdf5(root / "x.hdf5")
    plan = plan_dataset_partitions(
        dataset_uri=root.as_posix(),
        dataset_id="robomimic",
        version="v1",
        family_hint="robomimic",
    )
    payload = plan.to_dict()
    plan2 = PartitionPlan.from_dict(payload)
    assert plan2.dataset_id == plan.dataset_id
    assert len(plan2.partitions) == len(plan.partitions)
