from __future__ import annotations

from pathlib import Path

from robot_dh.sharding.planner import _discover_s3_dataset_prefixes, plan_etl


class _FakePaginator:
    def __init__(self, keys: list[str]) -> None:
        self.keys = keys
        self.calls: list[tuple[str, str]] = []

    def paginate(self, *, Bucket: str, Prefix: str) -> list[dict[str, list[dict[str, str]]]]:
        self.calls.append((Bucket, Prefix))
        return [{"Contents": [{"Key": key} for key in self.keys if key.startswith(Prefix)]}]


class _FakeS3Client:
    def __init__(self, keys: list[str]) -> None:
        self.paginator = _FakePaginator(keys)

    def get_paginator(self, name: str) -> _FakePaginator:
        assert name == "list_objects_v2"
        return self.paginator


class _FakeS3Store:
    def __init__(self, keys: list[str]) -> None:
        self.client = _FakeS3Client(keys)


def _make_raw_dataset(root: Path, dataset_id: str, version: str, size_bytes: int) -> None:
    target = root / "raw" / dataset_id / version
    target.mkdir(parents=True, exist_ok=True)
    (target / "endpose.pt").write_bytes(b"\0" * size_bytes)
    (target / "meta.yaml").write_text("dataset_id: " + dataset_id + "\nversion: " + version + "\n")


def test_plan_etl_discovers_and_partitions(tmp_path: Path) -> None:
    _make_raw_dataset(tmp_path, "alpha", "v1", 4 * 1024 * 1024)
    _make_raw_dataset(tmp_path, "beta", "v1", 2 * 1024 * 1024)
    _make_raw_dataset(tmp_path, "gamma", "v1", 6 * 1024 * 1024)

    plan = plan_etl(
        root_uri=tmp_path.as_posix(),
        lake_root="local-lake",
        target_shard_size_gb=0.005,
        max_shards=2,
    )
    assert plan.total_datasets == 3
    # meta.yaml 也算在 input_bytes 内，因此总字节数稍大于 endpose.pt 之和
    assert plan.total_bytes >= 12 * 1024 * 1024
    assert plan.total_bytes < 13 * 1024 * 1024
    assert len(plan.shards) == 2
    placed = sum(len(s.datasets) for s in plan.shards)
    assert placed == 3
    for shard in plan.shards:
        assert shard.total_bytes == sum(d.input_bytes for d in shard.datasets)


def test_plan_etl_include_exclude(tmp_path: Path) -> None:
    _make_raw_dataset(tmp_path, "alpha_scale30", "v1", 1024)
    _make_raw_dataset(tmp_path, "beta_sample", "v1", 1024)
    _make_raw_dataset(tmp_path, "gamma_scale30_sample", "v1", 1024)

    plan = plan_etl(
        root_uri=tmp_path.as_posix(),
        lake_root="local",
        include_patterns=["*scale30*"],
        exclude_patterns=["*sample*"],
        target_shard_size_gb=1.0,
        max_shards=1,
    )
    discovered_ids = sorted(d.dataset_id for shard in plan.shards for d in shard.datasets)
    assert discovered_ids == ["alpha_scale30"]


def test_plan_etl_empty_root_produces_single_shard(tmp_path: Path) -> None:
    (tmp_path / "raw").mkdir()
    plan = plan_etl(root_uri=tmp_path.as_posix(), lake_root="local")
    assert plan.total_datasets == 0
    assert len(plan.shards) == 1
    assert plan.shards[0].datasets == []


def test_s3_discovery_accepts_raw_root_without_double_raw() -> None:
    store = _FakeS3Store(
        [
            "raw/droid_lerobot_scale30/v1/endpose.pt",
            "raw/robomimic_scale30/v1/meta.yaml",
        ]
    )

    datasets = _discover_s3_dataset_prefixes(store, "s3://robot-datasets/raw")

    assert store.client.paginator.calls == [("robot-datasets", "raw/")]
    assert datasets == [
        ("droid_lerobot_scale30", "v1", "s3://robot-datasets/raw/droid_lerobot_scale30/v1/"),
        ("robomimic_scale30", "v1", "s3://robot-datasets/raw/robomimic_scale30/v1/"),
    ]
