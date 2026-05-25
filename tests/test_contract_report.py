"""contract_report.json / .html 渲染。"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from robot_dh.qc.contracts import run_contract, write_report


def test_report_json_html_serializable(tmp_path: Path) -> None:
    root = tmp_path / "demo/v1"
    root.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame({"action": [1.0] * 5})
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), root / "x.parquet")
    report, profile = run_contract(
        dataset_uri=root.as_posix(),
        dataset_family="universal",
        dataset_id="demo",
        version="v1",
    )
    out = tmp_path / "out"
    write_report(report=report, profile=profile, output_uri=out.as_posix())
    data = json.loads((out / "contract_report.json").read_text())
    assert data["contract_id"] == "universal_v1"
    assert data["dataset_id"] == "demo"
    assert "metrics" in data and "rules" in data
    html = (out / "contract_report.html").read_text()
    assert "QC contract report" in html
    assert "universal_v1" in html
