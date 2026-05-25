"""normalize resume：第一次产出 checkpoint；重跑 SKIP / RESUMED；--force 重跑。"""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
import pytest

from robot_dh.data.synthetic import generate_demo_dataset
from robot_dh.etl.normalize import normalize_dataset
from robot_dh.lake.manifest import MANIFEST_FILENAME
from robot_dh.progress.checkpoint import CHECKPOINT_FILENAME, load_checkpoint


@pytest.fixture
def synthetic_dataset(tmp_path: Path) -> Path:
    return generate_demo_dataset(
        output_dir=tmp_path / "raw" / "demo",
        duration_sec=2.0,
        fps=30,
        num_buttons=2,
        num_presses=4,
    )


def test_first_run_writes_checkpoint_and_manifest(synthetic_dataset: Path, tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("ROBOT_DH_DB_URI", f"sqlite:///{tmp_path}/registry.db")
    out = tmp_path / "lake/ods/demo/v1"
    res = normalize_dataset(
        dataset_uri=synthetic_dataset.as_posix(),
        output_uri=out.as_posix(),
        dataset_id="demo",
        version="v1",
        heartbeat_interval_sec=0.0,
        progress_log_interval_sec=0.0,
    )
    assert res.status == "OK"
    assert (out / MANIFEST_FILENAME).is_file()
    assert (out / CHECKPOINT_FILENAME).is_file()
    ckpt = load_checkpoint(out.as_posix())
    assert ckpt is not None
    assert "write_manifest" in ckpt.completed_steps


def test_second_run_skips_when_manifest_exists(synthetic_dataset: Path, tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("ROBOT_DH_DB_URI", f"sqlite:///{tmp_path}/registry.db")
    out = tmp_path / "lake/ods/demo/v1"
    normalize_dataset(
        dataset_uri=synthetic_dataset.as_posix(),
        output_uri=out.as_posix(),
        dataset_id="demo", version="v1",
        heartbeat_interval_sec=0.0, progress_log_interval_sec=0.0,
    )
    pose_first = pq.read_table(out / "pose.parquet").to_pandas()
    res2 = normalize_dataset(
        dataset_uri=synthetic_dataset.as_posix(),
        output_uri=out.as_posix(),
        dataset_id="demo", version="v1",
    )
    assert res2.status == "SKIPPED"
    pose_second = pq.read_table(out / "pose.parquet").to_pandas()
    assert pose_first.equals(pose_second)


def test_resume_after_manifest_deleted_does_not_rewrite_pose(synthetic_dataset: Path, tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("ROBOT_DH_DB_URI", f"sqlite:///{tmp_path}/registry.db")
    out = tmp_path / "lake/ods/demo/v1"
    normalize_dataset(
        dataset_uri=synthetic_dataset.as_posix(),
        output_uri=out.as_posix(),
        dataset_id="demo", version="v1",
    )
    # 删除 manifest，模拟"normalize 跑成功一半但没写 manifest"
    (out / MANIFEST_FILENAME).unlink()
    pose_mtime_before = (out / "pose.parquet").stat().st_mtime_ns

    res = normalize_dataset(
        dataset_uri=synthetic_dataset.as_posix(),
        output_uri=out.as_posix(),
        dataset_id="demo", version="v1",
        resume=True,
    )
    assert res.status == "RESUMED"
    pose_mtime_after = (out / "pose.parquet").stat().st_mtime_ns
    assert pose_mtime_before == pose_mtime_after, "resume should not rewrite pose.parquet"
    assert (out / MANIFEST_FILENAME).is_file()


def test_materialize_input_uses_stable_cache_when_resume(monkeypatch, tmp_path: Path) -> None:
    """resume=True 时 _materialize_input 应该写到稳定的 cache_root 而不是 tempdir。

    覆盖 v1_6_bridgedata_v2_normalize_adapter_request.md §4.C：避免 step 容器重启后
    重新下载 227 MiB raw shard。这里只测路径解析；S3 行为由 mock 覆盖。
    """
    from robot_dh.etl.normalize import _resolve_input_dir

    cache_root = tmp_path / "input-cache"
    work_dir = tmp_path / "tmp-workdir"
    work_dir.mkdir()

    fresh = _resolve_input_dir(
        "s3://bucket/raw/foo/v1", work_dir, resume=False, cache_root=cache_root
    )
    assert fresh == work_dir / "input"

    cached = _resolve_input_dir(
        "s3://bucket/raw/foo/v1", work_dir, resume=True, cache_root=cache_root
    )
    assert cached.parent == cache_root
    # 同 URI 必须 hash 到相同目录，跨进程才能复用
    cached2 = _resolve_input_dir(
        "s3://bucket/raw/foo/v1", work_dir, resume=True, cache_root=cache_root
    )
    assert cached == cached2

    different_uri = _resolve_input_dir(
        "s3://bucket/raw/bar/v1", work_dir, resume=True, cache_root=cache_root
    )
    assert different_uri != cached


def test_force_overwrites_manifest(synthetic_dataset: Path, tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("ROBOT_DH_DB_URI", f"sqlite:///{tmp_path}/registry.db")
    out = tmp_path / "lake/ods/demo/v1"
    res1 = normalize_dataset(
        dataset_uri=synthetic_dataset.as_posix(),
        output_uri=out.as_posix(),
        dataset_id="demo", version="v1",
    )
    job1 = res1.job_id

    res2 = normalize_dataset(
        dataset_uri=synthetic_dataset.as_posix(),
        output_uri=out.as_posix(),
        dataset_id="demo", version="v1",
        force=True,
    )
    assert res2.status == "OK"
    assert res2.job_id != job1
