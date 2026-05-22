"""可选 S3 数据湖集成测试。

仅当环境变量齐备时启用：
    ROBOT_DH_TEST_S3_ENDPOINT_URL
    ROBOT_DH_TEST_S3_ACCESS_KEY
    ROBOT_DH_TEST_S3_SECRET_KEY
    ROBOT_DH_TEST_S3_LAKE_BUCKET（须已存在；测试写入随机前缀下）

否则跳过整个模块。测试写入 `s3://<lake_bucket>/tmp/test/<random>/`，结束后删除。
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest

S3_VARS = [
    "ROBOT_DH_TEST_S3_ENDPOINT_URL",
    "ROBOT_DH_TEST_S3_ACCESS_KEY",
    "ROBOT_DH_TEST_S3_SECRET_KEY",
    "ROBOT_DH_TEST_S3_LAKE_BUCKET",
]

pytestmark = pytest.mark.skipif(
    any(not os.environ.get(v) for v in S3_VARS),
    reason="S3 lake integration env not configured",
)


@pytest.fixture
def s3_env(monkeypatch) -> None:
    monkeypatch.setenv("ROBOT_DH_S3_ENDPOINT_URL", os.environ["ROBOT_DH_TEST_S3_ENDPOINT_URL"])
    monkeypatch.setenv("ROBOT_DH_S3_ACCESS_KEY", os.environ["ROBOT_DH_TEST_S3_ACCESS_KEY"])
    monkeypatch.setenv("ROBOT_DH_S3_SECRET_KEY", os.environ["ROBOT_DH_TEST_S3_SECRET_KEY"])
    monkeypatch.setenv(
        "ROBOT_DH_S3_REGION", os.environ.get("ROBOT_DH_TEST_S3_REGION", "us-east-1")
    )
    monkeypatch.setenv("ROBOT_DH_S3_LAKE_BUCKET", os.environ["ROBOT_DH_TEST_S3_LAKE_BUCKET"])
    return None


@pytest.fixture
def s3_prefix(s3_env) -> None:
    bucket = os.environ["ROBOT_DH_TEST_S3_LAKE_BUCKET"]
    prefix = f"tmp/test-{uuid.uuid4().hex[:12]}"
    uri = f"s3://{bucket}/{prefix}"
    yield uri
    try:
        from robot_dh.lake.store import S3LakeStore

        store = S3LakeStore.from_env()
        for obj_uri in store.list(uri):
            from robot_dh.lake.uri import parse_uri

            parsed = parse_uri(obj_uri)
            store.client.delete_object(Bucket=parsed.bucket, Key=parsed.key)
    except Exception:
        pass


def test_s3_lake_store_upload_download_round_trip(s3_prefix: str, tmp_path: Path) -> None:
    from robot_dh.lake.store import S3LakeStore

    store = S3LakeStore.from_env()
    (tmp_path / "a.txt").write_text("hello-A")
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "b.txt").write_text("hello-B")

    store.upload_dir(tmp_path, s3_prefix)

    listed = store.list(s3_prefix)
    keys = sorted([uri.rsplit("/", 1)[-1] for uri in listed])
    assert "a.txt" in keys
    assert "b.txt" in keys

    out_dir = tmp_path / "_out"
    store.download_dir(s3_prefix, out_dir)
    assert (out_dir / "a.txt").read_text() == "hello-A"
    assert (out_dir / "sub" / "b.txt").read_text() == "hello-B"


def test_s3_normalize_then_features_end_to_end(s3_prefix: str, tmp_path: Path, monkeypatch) -> None:
    """本地合成 raw，上传 S3，再 normalize + features 写回数据湖。"""
    from robot_dh.data.synthetic import generate_demo_dataset
    from robot_dh.etl.features import build_features
    from robot_dh.etl.normalize import normalize_dataset
    from robot_dh.lake.store import S3LakeStore
    from robot_dh.lake.uri import join_uri

    raw_local = generate_demo_dataset(
        output_dir=tmp_path / "raw_local",
        duration_sec=3.0,
        fps=30,
        num_buttons=2,
        num_presses=4,
    )

    store = S3LakeStore.from_env()
    raw_uri = join_uri(s3_prefix, "raw", "s3demo", "v1")
    store.upload_dir(raw_local, raw_uri)

    ods_uri = join_uri(s3_prefix, "ods", "s3demo", "v1")
    monkeypatch.setenv("ROBOT_DH_DB_URI", f"sqlite:///{tmp_path}/registry.db")
    norm = normalize_dataset(
        dataset_uri=raw_uri,
        output_uri=ods_uri,
        dataset_id="s3demo",
        version="v1",
    )
    assert norm.num_samples > 0
    assert store.exists(join_uri(ods_uri, "pose.parquet"))
    assert store.exists(join_uri(ods_uri, "_manifest.json"))

    dwd_uri = join_uri(s3_prefix, "dwd", "s3demo", "v1")
    feat = build_features(input_uri=ods_uri, output_uri=dwd_uri)
    assert feat.num_press_events >= 0
    assert store.exists(join_uri(dwd_uri, "pose_feature.parquet"))
    assert store.exists(join_uri(dwd_uri, "_manifest.json"))
