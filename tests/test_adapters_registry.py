"""v1.7：adapter registry detect / list / yaml overrides。"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from robot_dh.adapters import (
    detect_adapter,
    get_adapter,
    list_adapters,
    load_adapter_registry,
)
from robot_dh.lake.uri import to_file_uri


def test_list_adapters_returns_builtin_set() -> None:
    fams = list_adapters()
    assert set(fams) >= {"droid", "robomimic", "bridge", "universal"}


def test_detect_with_dataset_id_prefix(tmp_path: Path) -> None:
    uri = to_file_uri(tmp_path)  # 空目录
    assert detect_adapter(uri, dataset_id="robomimic_dev1g").family == "robomimic"
    assert detect_adapter(uri, dataset_id="droid_lerobot_dev1g").family == "droid"
    assert detect_adapter(uri, dataset_id="bridgedata_v2_dev").family == "bridge"


def test_detect_with_layout_markers_droid(tmp_path: Path) -> None:
    (tmp_path / "meta").mkdir()
    (tmp_path / "meta" / "info.json").write_text(json.dumps({"total_episodes": 0}))
    (tmp_path / "data" / "chunk-000").mkdir(parents=True)
    (tmp_path / "data" / "chunk-000" / "file-000.parquet").write_bytes(b"PAR1")
    result = detect_adapter(to_file_uri(tmp_path))
    assert result.family == "droid"
    assert any("meta/info.json" in m for m in result.matched_markers)


def test_detect_falls_back_to_universal_when_empty(tmp_path: Path) -> None:
    res = detect_adapter(to_file_uri(tmp_path))
    assert res.family == "universal"
    assert res.confidence <= 0.2


def test_yaml_overrides_loaded(tmp_path: Path) -> None:
    p = tmp_path / "dataset_adapters.yaml"
    p.write_text(
        """
version: 1
adapters:
  - family: bridge
    normalize_options:
      direct_parquet_read: false
    qc_options:
      probe_timeout_sec: 999
""",
        encoding="utf-8",
    )
    reg = load_adapter_registry(config_path=p)
    qc = reg.qc_options_for("bridge")
    assert qc["probe_timeout_sec"] == 999
    norm = reg.normalize_options_for("bridge", to_file_uri(tmp_path))
    # 用户 yaml override 必须覆盖默认 True
    assert norm["direct_parquet_read"] is False


def test_universal_probe_on_local_path(tmp_path: Path) -> None:
    (tmp_path / "x.parquet").write_bytes(b"\x00" * 32)
    (tmp_path / "y.hdf5").write_bytes(b"\x00" * 16)
    (tmp_path / "z.mp4").write_bytes(b"\x00" * 8)
    adapter = get_adapter("universal")
    result = adapter.probe(to_file_uri(tmp_path))
    assert result.status == "OK"
    assert result.parquet_files == 1
    assert result.hdf5_files == 1
    assert result.video_files == 1
    assert result.bytes_total == 32 + 16 + 8
