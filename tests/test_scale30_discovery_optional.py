"""可选 scale30 S3 发现测试；走 ROBOT_DH_TEST_S3_RAW_ROOT 与 ROBOT_DH_S3_* 一组环境变量。"""

from __future__ import annotations

import os

import pytest

from robot_dh.sharding.planner import plan_etl


_S3_ROOT = os.environ.get("ROBOT_DH_TEST_S3_RAW_ROOT") or os.environ.get("ROBOT_DH_TEST_S3_ROOT")
_S3_ENDPOINT = os.environ.get("ROBOT_DH_S3_ENDPOINT_URL")
_S3_AK = os.environ.get("ROBOT_DH_S3_ACCESS_KEY")
_S3_SK = os.environ.get("ROBOT_DH_S3_SECRET_KEY")

pytestmark = pytest.mark.skipif(
    not (_S3_ROOT and _S3_ENDPOINT and _S3_AK and _S3_SK),
    reason="ROBOT_DH_TEST_S3_RAW_ROOT (and ROBOT_DH_S3_* env) not set",
)


def test_scale30_discovery_can_run() -> None:
    plan = plan_etl(
        root_uri=_S3_ROOT,
        lake_root=_S3_ROOT.replace("/raw", ""),
        target_shard_size_gb=5.0,
        max_shards=4,
        include_patterns=["*scale30*"],
    )
    # 远端可能无 scale30：仍应得到结构合法的 plan。
    assert plan.plan_id
    assert plan.total_datasets >= 0
    assert len(plan.shards) >= 1
