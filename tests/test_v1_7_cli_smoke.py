"""v1.7：robot-dh CLI 新增子命令端到端 smoke 测试（无远端依赖）。"""

from __future__ import annotations

import io
import json
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

import h5py
import pytest

from robot_dh.cli import _main_impl as cli_main


def _run_cli(args: list[str], *, env_overrides: dict[str, str] | None = None) -> tuple[int, str]:
    buf_out = io.StringIO()
    buf_err = io.StringIO()
    with redirect_stdout(buf_out), redirect_stderr(buf_err):
        code = cli_main(args)
    return code, buf_out.getvalue()


def test_cli_adapter_list(monkeypatch: pytest.MonkeyPatch) -> None:
    code, out = _run_cli(["adapter", "list"])
    assert code == 0
    payload = json.loads(out)
    assert "families" in payload
    assert "robomimic" in payload["families"]


def test_cli_adapter_detect_uses_id_prefix(tmp_path: Path) -> None:
    code, out = _run_cli([
        "adapter", "detect",
        "--dataset-uri", str(tmp_path),
        "--dataset-id", "bridgedata_v2_dev",
    ])
    assert code == 0
    payload = json.loads(out)
    assert payload["family"] == "bridge"


def test_cli_adapter_probe_universal(tmp_path: Path) -> None:
    (tmp_path / "a.parquet").write_bytes(b"\x00" * 10)
    (tmp_path / "b.hdf5").write_bytes(b"\x00" * 20)
    code, out = _run_cli([
        "adapter", "probe",
        "--dataset-uri", str(tmp_path),
        "--family", "universal",
    ])
    assert code == 0
    payload = json.loads(out)
    assert payload["family"] == "universal"
    assert payload["parquet_files"] == 1
    assert payload["hdf5_files"] == 1


def test_cli_runtime_heartbeat_check_no_records(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    events_dir = tmp_path / "events"
    events_dir.mkdir()
    code, out = _run_cli([
        "runtime", "heartbeat", "check",
        "--events-dir", str(events_dir),
        "--workflow-name", "wf-empty",
        "--warn-after-sec", "60",
        "--stale-after-sec", "300",
    ])
    assert code == 0
    payload = json.loads(out)
    assert payload["status"] == "ok"
    assert payload["rows"] == []


def test_cli_local_datasets_list(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ROBOT_DH_LOCAL_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("ROBOT_DH_K8S_LOCAL_DATA_ROOT", str(tmp_path))
    runtime_yaml = tmp_path / "runtime.yaml"
    runtime_yaml.write_text("version: 1\n", encoding="utf-8")
    devscale_yaml = tmp_path / "devscale.yaml"
    devscale_yaml.write_text(
        f"""
version: 1
total_max_bytes: 3000000000
datasets:
  - dataset_id: x_dev
    family: droid
    version: v1
    source_uri: s3://b/k/v1
    target_local_uri: file://{tmp_path}/raw/x_dev/v1
""",
        encoding="utf-8",
    )
    code, out = _run_cli([
        "local", "datasets", "list",
        "--config", str(runtime_yaml),
        "--devscale-config", str(devscale_yaml),
    ])
    assert code == 0
    payload = json.loads(out)
    assert payload["datasets"][0]["dataset_id"] == "x_dev"
