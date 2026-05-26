"""v1.7：LocalRuntimeConfig 解析与 env 优先级。"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from robot_dh.local_runtime import LocalRuntimeConfig, load_runtime_config


@pytest.fixture(autouse=True)
def _isolated_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for k in (
        "ROBOT_DH_LOCAL_DATA_ROOT",
        "ROBOT_DH_K8S_LOCAL_DATA_ROOT",
        "ROBOT_DH_DEV_DATA_ROOT",
        "ROBOT_DH_DEV_LAKE_ROOT",
        "ROBOT_DH_INPUT_CACHE_DIR",
    ):
        monkeypatch.delenv(k, raising=False)


def test_defaults_when_no_env_or_config(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    # 切到一个没有 configs/devscale_runtime.yaml 的临时目录，强制走默认值。
    monkeypatch.chdir(tmp_path)
    cfg = load_runtime_config()
    assert cfg.k8s_data_root == "/mnt/local-data/robot-dh-local"
    assert cfg.host_data_root == "/mnt/d/robot-dh-local"
    assert cfg.raw_root == "/mnt/local-data/robot-dh-local/raw"
    assert cfg.lake_root == "/mnt/local-data/robot-dh-local/lake"
    assert cfg.cache_root == "/mnt/local-data/robot-dh-local/cache"
    assert cfg.workdir_root.endswith("argo-workdir")
    assert cfg.raw_uri == "file:///mnt/local-data/robot-dh-local/raw"
    assert cfg.input_cache_dir == "/mnt/local-data/robot-dh-local/cache/input-cache"


def test_env_overrides_paths(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ROBOT_DH_LOCAL_DATA_ROOT", "/mnt/e/robot-dh-local")
    monkeypatch.setenv("ROBOT_DH_K8S_LOCAL_DATA_ROOT", "/mnt/local-data/robot-dh-local-test")
    cfg = load_runtime_config()
    assert cfg.host_data_root == "/mnt/e/robot-dh-local"
    assert cfg.k8s_data_root == "/mnt/local-data/robot-dh-local-test"
    assert cfg.raw_root == "/mnt/local-data/robot-dh-local-test/raw"


def test_to_env_round_trips(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)
    cfg = load_runtime_config()
    env = cfg.to_env()
    assert env["ROBOT_DH_DEV_DATA_ROOT"].startswith("file:///")
    assert env["ROBOT_DH_DEV_LAKE_ROOT"].startswith("file:///")
    # 不应有空键
    assert all(v for v in env.values())


def test_raw_uri_for_dataset(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)
    cfg = load_runtime_config()
    uri = cfg.raw_uri_for("droid_lerobot_dev1g", "v1")
    assert uri == "file:///mnt/local-data/robot-dh-local/raw/droid_lerobot_dev1g/v1"


def test_load_yaml_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    cfg_path = tmp_path / "runtime.yaml"
    cfg_path.write_text(
        """
version: 1
runtime:
  host_data_root: /mnt/d/robot-dh-local
  k8s_data_root: /mnt/local-data/robot-dh-local
  raw_subdir: raw
  lake_subdir: lake
  cache_subdir: cache
  workdir_subdir: cache/argo-workdir
  manifests_subdir: manifests
  logs_subdir: logs
limits:
  devscale_total_max_bytes: 1500000000
  devscale_total_max_files: 2000
devscale_datasets:
  - droid_lerobot_dev1g
""",
        encoding="utf-8",
    )
    cfg = load_runtime_config(config_path=cfg_path)
    assert cfg.devscale_total_max_bytes == 1500000000
    assert cfg.devscale_total_max_files == 2000
    assert cfg.devscale_dataset_ids == ("droid_lerobot_dev1g",)
