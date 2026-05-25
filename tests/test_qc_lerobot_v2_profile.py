"""LeRobot v2 lazy profile：用 monkeypatch 把 s3fs 替换成 in-memory fake，
验证 detect / profile 路径不下载视频、并发 footer、droid_metrics 正确消费。
"""

from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


class _FakeS3FS:
    """非常薄的 s3fs.S3FileSystem 替身；只实现 ls/find/open/info/exists。

    内部状态是一棵 ``dict[str, bytes]``，key 是 ``bucket/key`` 形式。
    """

    def __init__(self, blobs: dict[str, bytes]) -> None:
        self._blobs = dict(blobs)

    def _children(self, prefix: str) -> list[str]:
        prefix = prefix.rstrip("/") + "/"
        return [k for k in self._blobs if k.startswith(prefix)]

    def exists(self, path: str) -> bool:
        return path in self._blobs or bool(self._children(path))

    def info(self, path: str) -> dict[str, Any]:
        if path not in self._blobs:
            raise FileNotFoundError(path)
        return {"size": len(self._blobs[path]), "type": "file"}

    def ls(self, path: str, detail: bool = True) -> list[dict[str, Any]]:
        children = self._children(path)
        if not children and path not in self._blobs:
            raise FileNotFoundError(path)
        result: list[dict[str, Any]] = []
        for key in sorted(children):
            result.append({"name": key, "size": len(self._blobs[key]), "type": "file"})
        return result

    def find(self, path: str, detail: bool = True) -> dict[str, dict[str, Any]]:
        children = self._children(path)
        if not children and path not in self._blobs:
            return {} if detail else []
        return {
            name: {"size": len(self._blobs[name]), "type": "file"}
            for name in sorted(children)
        }

    def open(self, path: str, mode: str = "rb"):  # noqa: A003
        if path not in self._blobs:
            raise FileNotFoundError(path)
        return io.BytesIO(self._blobs[path])


def _make_parquet_bytes() -> bytes:
    buf = io.BytesIO()
    df = pa.table(
        {
            "episode_index": [0, 0, 0],
            "frame_index": [0, 1, 2],
            "action": [[0.0] * 7, [0.1] * 7, [0.2] * 7],
            "observation.state": [[0.0] * 7] * 3,
            "language_instruction": ["pick"] * 3,
        }
    )
    pq.write_table(df, buf)
    return buf.getvalue()


@pytest.fixture()
def fake_lerobot_v2_bucket(monkeypatch: pytest.MonkeyPatch) -> dict[str, bytes]:
    """构造一个最小 lerobot v2 layout：3 个 chunk parquet + 6 个 mp4 + meta/*。

    返回 blobs（``bucket/key`` -> bytes），方便测试断言。
    """
    bucket = "robot-datasets"
    base = f"{bucket}/raw/droid_lerobot_scale30/v1"
    blobs: dict[str, bytes] = {}
    blobs[f"{base}/meta/info.json"] = json.dumps(
        {"total_episodes": 17, "total_frames": 1234, "fps": 30, "codebase_version": "v2.0"}
    ).encode()
    blobs[f"{base}/meta/stats.json"] = json.dumps({"action": {"mean": [0.0] * 7}}).encode()
    blobs[f"{base}/_manifest.json"] = json.dumps({"files": ["data/chunk-000/file-000.parquet"] * 3}).encode()
    pq_bytes = _make_parquet_bytes()
    for i in range(3):
        blobs[f"{base}/data/chunk-000/file-{i:03d}.parquet"] = pq_bytes
    for i in range(6):
        blobs[f"{base}/videos/chunk-000/wrist/episode_{i:03d}.mp4"] = b"\x00" * 4096

    fs = _FakeS3FS(blobs)
    # patch 三处使用点：detect / read json / list / open / find
    monkeypatch.setattr("robot_dh.qc.lerobot_v2.get_s3fs", lambda: fs)
    monkeypatch.setattr("robot_dh.qc.lerobot_v2._probe_parquet_footer", _probe_parquet_with_fake(fs))
    return blobs


def _probe_parquet_with_fake(fs: _FakeS3FS):
    """fake _probe_parquet_footer：从 fake fs 取 bytes 喂给 pyarrow，绕开真实 s3fs 调用。"""
    import hashlib

    from robot_dh.lake.s3_fs import split_s3_uri

    def _probe(s3_uri: str) -> dict[str, Any]:
        bucket, key = split_s3_uri(s3_uri)
        path = f"{bucket}/{key}"
        try:
            data = fs.open(path).read()
            pf = pq.ParquetFile(io.BytesIO(data))
            schema = pf.schema_arrow
            names = list(schema.names)
            return {
                "uri": s3_uri,
                "readable": True,
                "size_bytes": len(data),
                "row_count": int(pf.metadata.num_rows),
                "num_row_groups": int(pf.num_row_groups),
                "schema_columns": names,
                "schema_hash": hashlib.sha256("|".join(names).encode()).hexdigest(),
            }
        except Exception as err:  # noqa: BLE001
            return {"uri": s3_uri, "readable": False, "error": str(err), "error_type": type(err).__name__}

    return _probe


def test_detect_lerobot_v2_returns_true_when_info_json_exists(
    fake_lerobot_v2_bucket: dict[str, bytes],
) -> None:
    from robot_dh.qc.lerobot_v2 import detect_lerobot_v2

    assert detect_lerobot_v2("s3://robot-datasets/raw/droid_lerobot_scale30/v1") is True


def test_detect_lerobot_v2_returns_false_when_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    from robot_dh.qc.lerobot_v2 import detect_lerobot_v2

    fs = _FakeS3FS({})
    monkeypatch.setattr("robot_dh.qc.lerobot_v2.get_s3fs", lambda: fs)
    assert detect_lerobot_v2("s3://robot-datasets/raw/nonexistent/v1") is False


def test_profile_lerobot_v2_does_not_download_videos(
    fake_lerobot_v2_bucket: dict[str, bytes],
) -> None:
    from robot_dh.qc.lerobot_v2 import profile_lerobot_v2

    profile = profile_lerobot_v2(
        dataset_uri="s3://robot-datasets/raw/droid_lerobot_scale30/v1",
        dataset_id="droid_lerobot_scale30",
        version="v1",
        dataset_family="droid",
        max_parquet_samples=2,
    )
    assert profile.asset_format == "lerobot_v2_parquet_video"
    # 视频文件计数正确（6 个），但 probe 不打开
    assert profile.profile["files_overview"]["video"] == 6
    assert profile.profile["video"] == []
    # parquet 抽样 2 个，全部 readable
    assert len(profile.profile["parquet"]) == 2
    assert all(p["readable"] for p in profile.profile["parquet"])
    # lerobot_v2 字段冒泡出来给 droid_metrics 用
    lv2 = profile.profile["lerobot_v2"]
    assert lv2["episodes_count"] == 17
    assert lv2["frames_count"] == 1234
    assert lv2["fps"] == 30
    assert lv2["chunk_files_total"] == 3
    assert lv2["video_files_total"] == 6


def test_droid_metrics_consumes_lerobot_v2_fields(
    fake_lerobot_v2_bucket: dict[str, bytes],
) -> None:
    from robot_dh.qc.droid import droid_metrics
    from robot_dh.qc.lerobot_v2 import profile_lerobot_v2

    profile = profile_lerobot_v2(
        dataset_uri="s3://robot-datasets/raw/droid_lerobot_scale30/v1",
        dataset_id="droid_lerobot_scale30",
        version="v1",
        dataset_family="droid",
        max_parquet_samples=2,
    )
    metrics = droid_metrics(profile)
    assert metrics["num_episodes"] == 17
    assert metrics["num_frames"] == 1234
    assert metrics["chunk_files_total"] == 3
    assert metrics["num_videos"] == 6
    # lazy 路径不下载视频，video_decode_pass_rate 不应因为 video=[] 被 0
    assert metrics["video_decode_pass_rate"] == 1.0
    assert metrics["action_column_coverage"] == 1.0


def test_profile_dataset_routes_lerobot_v2_to_lazy_path(
    fake_lerobot_v2_bucket: dict[str, bytes], monkeypatch: pytest.MonkeyPatch
) -> None:
    """profile_dataset 入口对 lerobot v2 dataset 必须命中 lazy 分支，不走 _list_files。"""
    from robot_dh.qc import profile as profile_mod

    called: dict[str, int] = {"list_files": 0, "lazy": 0}

    def _spy_list(*args: Any, **kwargs: Any) -> list[Any]:
        called["list_files"] += 1
        return []

    orig_lazy = profile_mod.profile_lerobot_v2

    def _spy_lazy(*args: Any, **kwargs: Any):
        called["lazy"] += 1
        return orig_lazy(*args, **kwargs)

    monkeypatch.setattr(profile_mod, "_list_files", _spy_list)
    monkeypatch.setattr(profile_mod, "profile_lerobot_v2", _spy_lazy)

    profile = profile_mod.profile_dataset(
        dataset_uri="s3://robot-datasets/raw/droid_lerobot_scale30/v1",
        dataset_id="droid_lerobot_scale30",
        version="v1",
        dataset_family="droid",
    )
    assert called["lazy"] == 1
    assert called["list_files"] == 0
    assert profile.episodes_count == 17
