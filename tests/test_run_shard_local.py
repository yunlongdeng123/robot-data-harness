from __future__ import annotations

import json
from pathlib import Path

from robot_dh.data.synthetic import generate_demo_dataset
from robot_dh.lake.manifest import MANIFEST_FILENAME
from robot_dh.sharding.planner import plan_etl
from robot_dh.sharding.shard_runner import run_shard


def _make_raw_dataset(root: Path, dataset_id: str, version: str) -> Path:
    target = root / "raw" / dataset_id / version
    generate_demo_dataset(
        output_dir=target,
        duration_sec=2.0,
        fps=10,
        num_buttons=2,
        num_presses=4,
    )
    return target


def test_run_shard_executes_etl_for_each_dataset(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("ROBOT_DH_DB_URI", f"sqlite:///{tmp_path}/registry.db")
    _make_raw_dataset(tmp_path, "alpha", "v1")
    _make_raw_dataset(tmp_path, "beta", "v1")
    lake_root = (tmp_path / "lake").as_posix()

    plan = plan_etl(
        root_uri=tmp_path.as_posix(),
        lake_root=lake_root,
        target_shard_size_gb=1.0,
        max_shards=1,
    )

    work_dir = tmp_path / "shard_0"
    summary = run_shard(
        plan=plan,
        shard_id=0,
        work_dir=work_dir,
        max_workers=1,
    )

    assert summary.total == 2
    assert summary.succeeded == 2
    assert summary.failed == 0
    assert summary.status == "OK"
    assert (Path(lake_root) / "ods" / "alpha" / "v1" / MANIFEST_FILENAME).is_file()
    assert (Path(lake_root) / "dwd" / "beta" / "v1" / MANIFEST_FILENAME).is_file()

    local_summary = work_dir / "shard_summary.json"
    assert local_summary.is_file()
    payload = json.loads(local_summary.read_text())
    assert payload["total"] == 2

    perf_files = list((work_dir / "perf").rglob("*_perf.json"))
    assert perf_files


def test_run_shard_unknown_shard_skips(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("ROBOT_DH_DB_URI", f"sqlite:///{tmp_path}/registry.db")
    _make_raw_dataset(tmp_path, "alpha", "v1")
    lake_root = (tmp_path / "lake").as_posix()

    plan = plan_etl(
        root_uri=tmp_path.as_posix(),
        lake_root=lake_root,
        target_shard_size_gb=1.0,
        max_shards=1,
    )
    work_dir = tmp_path / "shard_99"
    summary = run_shard(plan=plan, shard_id=99, work_dir=work_dir)
    assert summary.status == "SKIPPED"
    assert summary.total == 0
    assert (work_dir / "shard_summary.json").is_file()
