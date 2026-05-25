"""normalize 心跳与 progress 日志写入。"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from robot_dh.data.synthetic import generate_demo_dataset
from robot_dh.etl.normalize import normalize_dataset


def test_normalize_writes_heartbeat_jsonl(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("ROBOT_DH_DB_URI", f"sqlite:///{tmp_path}/registry.db")
    monkeypatch.setenv("ROBOT_DH_EVENTS_DIR", (tmp_path / "events").as_posix())
    dataset_dir = generate_demo_dataset(
        output_dir=tmp_path / "raw" / "demo",
        duration_sec=2.0,
        fps=30,
        num_buttons=2,
        num_presses=4,
    )
    out = tmp_path / "lake/ods/demo/v1"
    res = normalize_dataset(
        dataset_uri=dataset_dir.as_posix(),
        output_uri=out.as_posix(),
        dataset_id="demo",
        version="v1",
        heartbeat_interval_sec=0.0,
        progress_log_interval_sec=0.0,
    )
    assert res.status == "OK"
    files = list((tmp_path / "events").glob("heartbeats_*.jsonl"))
    assert len(files) == 1
    lines = [json.loads(l) for l in files[0].read_text().splitlines()]
    phases = {p.get("phase") for p in lines}
    # 至少包含核心子阶段
    expected = {
        "normalize.materialize_input",
        "normalize.load_bundles",
        "normalize.upload_outputs",
        "normalize.write_manifest",
    }
    missing = expected - phases
    assert not missing, f"missing phases in heartbeats: {missing}; got {phases}"
