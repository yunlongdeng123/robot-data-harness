"""v1.9 推理 benchmark 测试：输出 CSV / JSON / HTML。"""

from __future__ import annotations

import csv
import io
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from robot_dh.inference import run_benchmark
from robot_dh.models import ModelRegistry, ModelSpec


@pytest.fixture()
def setup(monkeypatch, tmp_path):
    monkeypatch.setenv("ROBOT_DH_DB_URI", f"sqlite:///{tmp_path/'b.db'}")
    monkeypatch.setenv("ROBOT_DH_AI_EVENTS_DIR", str(tmp_path / "events"))
    d = tmp_path / "ml-ready/demo/v1"
    d.mkdir(parents=True)
    pq.write_table(pa.table({"episode_id": [f"e{i}" for i in range(10)]}), (d / "train.parquet").as_posix())
    ModelRegistry().register(ModelSpec(model_id="mc", model_name="MC", model_type="caption", backend="mock"))
    return tmp_path, f"file://{d.as_posix()}"


def test_benchmark_outputs(setup) -> None:
    tmp_path, input_uri = setup
    out = tmp_path / "bench"
    res = run_benchmark(
        input_uri=input_uri, model_id="mc", output_uri=f"file://{out.as_posix()}",
        concurrency_list=[1, 2], batch_size_list=[2, 4], limit=10,
    )
    assert len(res.combos) == 4
    for name in ("benchmark_report.json", "benchmark_report.html", "benchmark_results.csv"):
        assert (out / name).exists()
    payload = json.loads((out / "benchmark_report.json").read_text())
    assert payload["combo_count"] == 4
    assert payload["best_combo"] is not None
    # CSV 行数 = 组合数 + 表头
    csv_rows = list(csv.DictReader(io.StringIO((out / "benchmark_results.csv").read_text())))
    assert len(csv_rows) == 4
    for r in csv_rows:
        assert r["total_samples"] == "10"
    html = (out / "benchmark_report.html").read_text()
    assert "<table" in html and "Inference Benchmark" in html


def test_benchmark_writes_pg_rows(setup) -> None:
    tmp_path, input_uri = setup
    out = tmp_path / "bench2"
    run_benchmark(input_uri=input_uri, model_id="mc", output_uri=f"file://{out.as_posix()}",
                  concurrency_list=[1], batch_size_list=[5], limit=10)
    from robot_dh.ai_tasks.store import resolve_optional_engine
    from robot_dh.warehouse.models import InferenceBenchmarkRunRow
    from sqlalchemy import select
    from sqlalchemy.orm import Session

    engine = resolve_optional_engine(None)
    with Session(engine, future=True) as session:
        rows = session.execute(select(InferenceBenchmarkRunRow)).scalars().all()
    assert len(rows) == 1
    assert rows[0].model_id == "mc"
    assert rows[0].total_samples == 10
