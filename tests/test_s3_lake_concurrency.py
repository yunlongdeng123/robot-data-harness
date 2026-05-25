"""S3LakeStore 并发下载 / 跳过已存在文件的本地化测试。

完全不连真实 S3；通过 ``unittest.mock`` 注入一个 fake client 即可。
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from robot_dh.lake.store import S3LakeStore


@pytest.fixture
def store() -> S3LakeStore:
    s = S3LakeStore(
        endpoint_url="http://fake:9000",
        access_key="ak",
        secret_key="sk",
        region_name="us-east-1",
    )
    return s


def _install_fake_client(store: S3LakeStore, contents: list[dict]) -> MagicMock:
    """覆盖 store.client，使其返回固定的 list_objects_v2 page 和 stub download_file。"""
    fake = MagicMock()
    page = {"Contents": contents}
    paginator = MagicMock()
    paginator.paginate.return_value = [page]
    fake.get_paginator.return_value = paginator

    def _head(**kwargs):  # noqa: ARG001
        # 目录式 prefix 必然 404，避免 download_dir 误把 prefix 当单 object
        raise ClientError(
            {"Error": {"Code": "404", "Message": "NoSuchKey"},
             "ResponseMetadata": {"HTTPStatusCode": 404}},
            "HeadObject",
        )

    fake.head_object.side_effect = _head

    def _download_file(bucket: str, key: str, target: str, Config=None) -> None:
        Path(target).write_bytes(b"x" * 16)

    fake.download_file.side_effect = _download_file
    store.client = fake
    return fake


def test_download_dir_uses_concurrent_pool_and_writes_files(store, tmp_path: Path) -> None:
    contents = [
        {"Key": "raw/foo/a.parquet", "Size": 16},
        {"Key": "raw/foo/b.parquet", "Size": 16},
        {"Key": "raw/foo/sub/c.parquet", "Size": 16},
    ]
    fake = _install_fake_client(store, contents)
    out = store.download_dir("s3://bucket/raw/foo/", tmp_path / "dl")
    files = sorted((tmp_path / "dl").rglob("*.parquet"))
    assert [f.relative_to(tmp_path / "dl").as_posix() for f in files] == [
        "a.parquet", "b.parquet", "sub/c.parquet",
    ]
    # 三次 download_file 调用，无论顺序
    assert fake.download_file.call_count == 3


def test_download_dir_skips_files_with_matching_size(store, tmp_path: Path) -> None:
    target_dir = tmp_path / "dl"
    target_dir.mkdir()
    (target_dir / "a.parquet").write_bytes(b"x" * 16)
    # 第二个文件 size mismatch -> 仍会下载
    (target_dir / "b.parquet").write_bytes(b"x" * 8)

    contents = [
        {"Key": "raw/foo/a.parquet", "Size": 16},
        {"Key": "raw/foo/b.parquet", "Size": 16},
    ]
    fake = _install_fake_client(store, contents)
    store.download_dir("s3://bucket/raw/foo/", target_dir)
    # 只下了 b.parquet
    assert fake.download_file.call_count == 1
    args, kwargs = fake.download_file.call_args
    assert args[1].endswith("b.parquet")


def test_download_dir_exclude_prefixes_skips_videos(store, tmp_path: Path) -> None:
    """v1.6.7：lerobot v2 normalize 跳过 ``videos/`` 把 18.4 GiB 减到 ~14 GiB。"""
    contents = [
        {"Key": "raw/droid/data/chunk-000/file-001.parquet", "Size": 16},
        {"Key": "raw/droid/data/chunk-000/file-002.parquet", "Size": 16},
        {"Key": "raw/droid/meta/info.json", "Size": 32},
        {"Key": "raw/droid/videos/chunk-000/cam.mp4", "Size": 1024},
        {"Key": "raw/droid/videos/chunk-000/cam_top.mp4", "Size": 1024},
    ]
    fake = _install_fake_client(store, contents)
    store.download_dir(
        "s3://bucket/raw/droid/", tmp_path / "dl",
        exclude_prefixes=("videos/",),
    )
    assert fake.download_file.call_count == 3
    keys_downloaded = [c.args[1] for c in fake.download_file.call_args_list]
    assert all("videos/" not in k for k in keys_downloaded)
    assert any(k.endswith("info.json") for k in keys_downloaded)


def test_download_dir_include_prefixes_allowlist(store, tmp_path: Path) -> None:
    contents = [
        {"Key": "raw/droid/data/file-001.parquet", "Size": 16},
        {"Key": "raw/droid/meta/info.json", "Size": 16},
        {"Key": "raw/droid/videos/cam.mp4", "Size": 16},
    ]
    fake = _install_fake_client(store, contents)
    store.download_dir(
        "s3://bucket/raw/droid/", tmp_path / "dl",
        include_prefixes=("data/",),
    )
    assert fake.download_file.call_count == 1
    assert fake.download_file.call_args.args[1].endswith("file-001.parquet")


def test_download_dir_progress_log_every(store, tmp_path: Path, caplog) -> None:
    """progress_log_every=2 且 4 个文件 → 至少能命中 2 / 4 两次进度行。"""
    import logging

    contents = [
        {"Key": f"raw/x/file-{i:03d}.parquet", "Size": 16} for i in range(4)
    ]
    _install_fake_client(store, contents)
    with caplog.at_level(logging.INFO, logger="robot_dh.lake.store"):
        store.download_dir(
            "s3://bucket/raw/x/", tmp_path / "dl",
            progress_log_every=2,
        )
    progress_lines = [r for r in caplog.records if "progress=" in r.getMessage()]
    assert len(progress_lines) >= 2, [r.getMessage() for r in caplog.records]


def test_download_dir_wallclock_progress_log(store, tmp_path: Path, caplog, monkeypatch) -> None:
    """v1.6.8 (fvx5z F3)：除了"按 N 文件触发"，wall-clock 触发也必须出进度行。

    测试方法：把 ``ROBOT_DH_DOWNLOAD_PROGRESS_INTERVAL_SEC`` 设成 0.0001s，
    第一个文件完成时就足够触发；progress_log_every 故意调大到 999（按文件不会触发），
    确认还能看到 ``progress=`` 行——即 wall-clock 路径独立生效。
    """
    import logging

    contents = [
        {"Key": f"raw/x/file-{i:03d}.parquet", "Size": 16} for i in range(3)
    ]
    _install_fake_client(store, contents)
    monkeypatch.setenv("ROBOT_DH_DOWNLOAD_PROGRESS_INTERVAL_SEC", "0.0001")

    def _slow_download_file(bucket: str, key: str, target: str, Config=None) -> None:
        import time
        time.sleep(0.001)
        Path(target).write_bytes(b"x" * 16)

    store.client.download_file.side_effect = _slow_download_file

    with caplog.at_level(logging.INFO, logger="robot_dh.lake.store"):
        store.download_dir(
            "s3://bucket/raw/x/", tmp_path / "dl",
            progress_log_every=999,
        )
    progress_lines = [r for r in caplog.records if "progress=" in r.getMessage()]
    assert progress_lines, (
        "wall-clock progress should fire at least once when "
        "ROBOT_DH_DOWNLOAD_PROGRESS_INTERVAL_SEC is small enough; got %s"
        % [r.getMessage() for r in caplog.records]
    )


def test_max_pool_connections_scales_with_concurrency(monkeypatch) -> None:
    """连接池上限至少 max(32, concurrency * transfer_threads)，避免 botocore 默认 10 撞墙。"""
    monkeypatch.setenv("ROBOT_DH_S3_DOWNLOAD_CONCURRENCY", "8")
    monkeypatch.setenv("ROBOT_DH_S3_TRANSFER_THREADS", "8")
    monkeypatch.delenv("ROBOT_DH_S3_MAX_POOL_CONNECTIONS", raising=False)
    s = S3LakeStore(
        endpoint_url="http://fake:9000",
        access_key="ak",
        secret_key="sk",
        region_name="us-east-1",
    )
    assert s.max_pool_connections >= 32
    assert s.client.meta.config.max_pool_connections == s.max_pool_connections


def test_max_pool_connections_respects_env_override(monkeypatch) -> None:
    monkeypatch.setenv("ROBOT_DH_S3_MAX_POOL_CONNECTIONS", "128")
    s = S3LakeStore(
        endpoint_url="http://fake:9000",
        access_key="ak",
        secret_key="sk",
        region_name="us-east-1",
    )
    assert s.max_pool_connections == 128
    assert s.client.meta.config.max_pool_connections == 128


def test_download_one_writes_via_part_file(store, tmp_path: Path) -> None:
    """确认单文件下载先写 .part 再 rename，避免半文件污染。"""
    target = tmp_path / "x.parquet"

    seen: dict[str, str] = {}

    def _download_file(bucket: str, key: str, dst: str, Config=None) -> None:
        seen["dst"] = dst
        Path(dst).write_bytes(b"abc")

    fake = MagicMock()
    fake.download_file.side_effect = _download_file
    store.client = fake
    store._download_one("bucket", "raw/x.parquet", target)
    assert target.is_file()
    assert seen["dst"].endswith(".part")
    # rename 后 .part 应消失
    assert not (target.with_suffix(target.suffix + ".part")).exists()
