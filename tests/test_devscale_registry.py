"""v1.7：devscale 注册表解析、路径重定向、runtime doctor / verify。"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from robot_dh.local_runtime import (
    LocalRuntimeConfig,
    load_devscale_registry,
    load_runtime_config,
    runtime_doctor,
    verify_local_datasets,
)


def _write_devscale_yaml(tmp_path: Path) -> Path:
    p = tmp_path / "devscale_datasets.yaml"
    p.write_text(
        """
version: 1
total_max_bytes: 3000000000
datasets:
  - dataset_id: fake_dev
    family: droid
    version: v1
    source_uri: s3://robot-datasets/raw/fake_scale30/v1
    target_local_uri: file:///mnt/d/robot-dh-local/raw/fake_dev/v1
    max_bytes: 100000
    include:
      - meta/**
""",
        encoding="utf-8",
    )
    return p


def test_load_devscale_registry_redirects_to_runtime_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ROBOT_DH_LOCAL_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("ROBOT_DH_K8S_LOCAL_DATA_ROOT", str(tmp_path / "k8s-mount"))
    cfg = load_runtime_config()
    reg = load_devscale_registry(
        config_path=_write_devscale_yaml(tmp_path), runtime_config=cfg,
    )
    assert len(reg.datasets) == 1
    ds = reg.datasets[0]
    assert ds.dataset_id == "fake_dev"
    # yaml 里写的 /mnt/d/robot-dh-local 应被替换为 cfg.k8s_data_root
    assert ds.target_local_path == str(tmp_path / "k8s-mount" / "raw" / "fake_dev" / "v1")
    assert ds.target_local_uri.startswith("file://")


def test_runtime_doctor_reports_missing_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "data"
    root.mkdir()
    for sub in ("raw/fake_dev/v1", "lake", "cache/input-cache", "cache/argo-workdir",
                "manifests", "logs"):
        (root / sub).mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("ROBOT_DH_LOCAL_DATA_ROOT", str(root))
    monkeypatch.setenv("ROBOT_DH_K8S_LOCAL_DATA_ROOT", str(root))
    devscale_yaml = tmp_path / "devscale_datasets.yaml"
    devscale_yaml.write_text(
        f"""
version: 1
total_max_bytes: 3000000000
datasets:
  - dataset_id: fake_dev
    family: droid
    version: v1
    source_uri: s3://x/y/z
    target_local_uri: file://{root}/raw/fake_dev/v1
    max_bytes: 100000
""",
        encoding="utf-8",
    )
    cfg = load_runtime_config()
    report = runtime_doctor(
        runtime_config=cfg,
        devscale_config_path=devscale_yaml,
    )
    assert report.status == "fail"
    assert any("manifest_missing" in i for i in report.issues)


def test_runtime_doctor_passes_when_manifest_ok(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "data"
    raw = root / "raw" / "fake_dev" / "v1"
    raw.mkdir(parents=True)
    for sub in ("lake", "cache/input-cache", "cache/argo-workdir",
                "manifests", "logs"):
        (root / sub).mkdir(parents=True, exist_ok=True)
    (raw / "file_a.bin").write_bytes(b"0" * 1024)
    (raw / "_manifest.json").write_text(
        json.dumps({
            "dataset_id": "fake_dev",
            "family": "droid",
            "version": "v1",
            "source_uri": "s3://x/y/z",
            "local_uri": f"file://{raw}",
            "status": "ok",
            "size_bytes": 1024,
            "file_count": 1,
            "files": [{"dst_path": str(raw / "file_a.bin"), "size_bytes": 1024, "rel_key": "file_a.bin"}],
        })
    )
    monkeypatch.setenv("ROBOT_DH_LOCAL_DATA_ROOT", str(root))
    monkeypatch.setenv("ROBOT_DH_K8S_LOCAL_DATA_ROOT", str(root))
    devscale_yaml = tmp_path / "devscale_datasets.yaml"
    devscale_yaml.write_text(
        f"""
version: 1
total_max_bytes: 3000000000
datasets:
  - dataset_id: fake_dev
    family: droid
    version: v1
    source_uri: s3://x/y/z
    target_local_uri: file://{raw}
    max_bytes: 100000
""",
        encoding="utf-8",
    )
    cfg = load_runtime_config()
    report = runtime_doctor(
        runtime_config=cfg,
        devscale_config_path=devscale_yaml,
    )
    assert report.status == "ok", report.issues

    reg = load_devscale_registry(config_path=devscale_yaml, runtime_config=cfg)
    verify = verify_local_datasets(registry=reg)
    assert verify.status == "ok"
    assert verify.totals["present_files"] == 1


def test_verify_remaps_host_dst_path_to_container_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """plan/manifest 写主机路径 /mnt/d/...，verify 跑在容器 root 下也要正确比对。

    保护 v1.7 local Argo verify-devscale-data step：sync 工具在 WSL 主机端跑、
    manifest 里 dst_path 是 /mnt/d/robot-dh-local/...；verify 在容器内挂载点
    是 /mnt/local-data/robot-dh-local/... —— 直接 Path(dst_path).exists() 必然
    全 missing。修复后必须按 rel_key + ds.target_local_path 拼路径。
    """
    host_root = "/mnt/d/robot-dh-local"
    container_root = tmp_path / "k8s-mount"
    raw = container_root / "raw" / "fake_dev" / "v1"
    (raw / "v1.5" / "can" / "ph").mkdir(parents=True)
    (raw / "v1.5" / "can" / "ph" / "low_dim_v15.hdf5").write_bytes(b"x" * 4096)
    for sub in ("lake", "cache/input-cache", "cache/argo-workdir", "manifests", "logs"):
        (container_root / sub).mkdir(parents=True, exist_ok=True)

    # manifest 故意写主机绝对路径，模拟在 WSL 主机端 sync 工具生成的产物
    host_dst = f"{host_root}/raw/fake_dev/v1/v1.5/can/ph/low_dim_v15.hdf5"
    (raw / "_manifest.json").write_text(
        json.dumps({
            "dataset_id": "fake_dev",
            "family": "robomimic",
            "version": "v1",
            "source_uri": "s3://x/y/z",
            "local_uri": f"file://{host_root}/raw/fake_dev/v1",
            "status": "ok",
            "size_bytes": 4096,
            "file_count": 1,
            "files": [
                {"dst_path": host_dst, "size_bytes": 4096, "status": "ok"},
            ],
        })
    )

    monkeypatch.setenv("ROBOT_DH_LOCAL_DATA_ROOT", str(container_root))
    monkeypatch.setenv("ROBOT_DH_K8S_LOCAL_DATA_ROOT", str(container_root))
    devscale_yaml = tmp_path / "devscale_datasets.yaml"
    devscale_yaml.write_text(
        f"""
version: 1
total_max_bytes: 3000000000
datasets:
  - dataset_id: fake_dev
    family: robomimic
    version: v1
    source_uri: s3://x/y/z
    target_local_uri: file://{host_root}/raw/fake_dev/v1
    max_bytes: 100000
""",
        encoding="utf-8",
    )
    cfg = load_runtime_config()
    reg = load_devscale_registry(config_path=devscale_yaml, runtime_config=cfg)
    # target_local_path 必须被重映射到容器 root
    assert reg.datasets[0].target_local_path == str(raw)

    verify = verify_local_datasets(registry=reg)
    # 关键断言：host-path dst_path 没让 verify 误判 missing
    assert verify.status == "ok", verify.datasets
    assert verify.totals["present_files"] == 1
    assert verify.totals["missing_files"] == 0
    assert verify.datasets[0]["missing_files"] == []
    # rel_key 反推应得到 "v1.5/can/ph/low_dim_v15.hdf5"，便于调试时排查路径错位
    # （这里通过 wrong_size 为空 + present_files=1 间接断言；rel_key 自身不暴露到顶层）


def test_verify_remaps_uses_rel_key_when_present(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """plan.json 里有 rel_key 时，verify 应优先用 rel_key 拼路径，
    避免 basename 折叠（droid 12 个文件里有 2 个同名 file-000.parquet）。"""
    container_root = tmp_path / "k8s-mount"
    raw = container_root / "raw" / "fake_dev" / "v1"
    (raw / "data" / "chunk-000").mkdir(parents=True)
    (raw / "meta" / "episodes" / "chunk-000").mkdir(parents=True)
    (raw / "data" / "chunk-000" / "file-000.parquet").write_bytes(b"a" * 100)
    (raw / "meta" / "episodes" / "chunk-000" / "file-000.parquet").write_bytes(b"b" * 200)
    for sub in ("lake", "cache/input-cache", "cache/argo-workdir", "manifests", "logs"):
        (container_root / sub).mkdir(parents=True, exist_ok=True)
    # manifest 存在即可；具体 files 走 plan_path
    (raw / "_manifest.json").write_text('{"files": []}')

    plan_path = container_root / "manifests" / "devscale_plan.json"
    host_root = "/mnt/d/robot-dh-local"
    plan_path.write_text(
        json.dumps({
            "schema_version": 1,
            "generated_at": "2026-01-01T00:00:00Z",
            "datasets": [{
                "dataset_id": "fake_dev",
                "family": "droid",
                "version": "v1",
                "files": [
                    {"rel_key": "data/chunk-000/file-000.parquet",
                     "dst_path": f"{host_root}/raw/fake_dev/v1/data/chunk-000/file-000.parquet",
                     "size_bytes": 100},
                    {"rel_key": "meta/episodes/chunk-000/file-000.parquet",
                     "dst_path": f"{host_root}/raw/fake_dev/v1/meta/episodes/chunk-000/file-000.parquet",
                     "size_bytes": 200},
                ],
            }],
        })
    )
    monkeypatch.setenv("ROBOT_DH_LOCAL_DATA_ROOT", str(container_root))
    monkeypatch.setenv("ROBOT_DH_K8S_LOCAL_DATA_ROOT", str(container_root))
    devscale_yaml = tmp_path / "devscale_datasets.yaml"
    devscale_yaml.write_text(
        f"""
version: 1
total_max_bytes: 3000000000
datasets:
  - dataset_id: fake_dev
    family: droid
    version: v1
    source_uri: s3://x/y/z
    target_local_uri: file://{host_root}/raw/fake_dev/v1
    max_bytes: 100000
""",
        encoding="utf-8",
    )
    cfg = load_runtime_config()
    reg = load_devscale_registry(config_path=devscale_yaml, runtime_config=cfg)
    verify = verify_local_datasets(registry=reg, plan_path=plan_path)
    assert verify.status == "ok", verify.datasets
    assert verify.totals["present_files"] == 2
    assert verify.totals["missing_files"] == 0


def test_runtime_doctor_flags_total_over_limit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "data"
    raw = root / "raw" / "fake_dev" / "v1"
    raw.mkdir(parents=True)
    for sub in ("lake", "cache/input-cache", "cache/argo-workdir", "manifests", "logs"):
        (root / sub).mkdir(parents=True, exist_ok=True)
    (raw / "big.bin").write_bytes(b"x" * 100_000)
    (raw / "_manifest.json").write_text(
        json.dumps({"dataset_id": "fake_dev", "family": "droid", "version": "v1",
                    "source_uri": "s3://x/y/z", "local_uri": f"file://{raw}",
                    "status": "ok", "size_bytes": 100000, "file_count": 1,
                    "files": []})
    )
    monkeypatch.setenv("ROBOT_DH_LOCAL_DATA_ROOT", str(root))
    monkeypatch.setenv("ROBOT_DH_K8S_LOCAL_DATA_ROOT", str(root))
    devscale_yaml = tmp_path / "devscale_datasets.yaml"
    devscale_yaml.write_text(
        f"""
version: 1
total_max_bytes: 1000
datasets:
  - dataset_id: fake_dev
    family: droid
    version: v1
    source_uri: s3://x/y/z
    target_local_uri: file://{raw}
    max_bytes: 100000
""",
        encoding="utf-8",
    )
    cfg = load_runtime_config()
    report = runtime_doctor(runtime_config=cfg, devscale_config_path=devscale_yaml)
    assert any("devscale_total_over_limit" in i for i in report.issues)
    # allow_over_limit=True 时不再 FAIL
    report_ok = runtime_doctor(
        runtime_config=cfg, devscale_config_path=devscale_yaml,
        allow_over_limit=True,
    )
    assert not any("devscale_total_over_limit" in i for i in report_ok.issues)
