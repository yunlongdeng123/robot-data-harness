"""universal contract：基本可读性 / 空文件检查。"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from robot_dh.qc.contracts import run_contract, write_report


def _write_parquet(path: Path, n_rows: int = 10) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame({"x": list(range(n_rows)), "action": [1.0] * n_rows})
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), path)


def test_universal_pass(tmp_path: Path) -> None:
    root = tmp_path / "demo/v1"
    _write_parquet(root / "data.parquet")
    report, profile = run_contract(
        dataset_uri=root.as_posix(),
        dataset_family="universal",
        dataset_id="demo",
        version="v1",
    )
    assert report.status == "PASS", report.failed_rules
    assert profile.files_count == 1
    assert profile.bytes is not None and profile.bytes > 0


def test_universal_warn_on_empty_file(tmp_path: Path) -> None:
    root = tmp_path / "demo/v1"
    _write_parquet(root / "data.parquet")
    (root / "empty.parquet").write_bytes(b"")
    report, _profile = run_contract(
        dataset_uri=root.as_posix(),
        dataset_family="universal",
        dataset_id="demo",
        version="v1",
    )
    # 空 parquet -> readable_parquet_rate < 0.95 (warn) + empty_file_count > 0 (warn)
    assert report.status in ("WARN", "FAIL")


def test_write_report_creates_artifacts(tmp_path: Path) -> None:
    root = tmp_path / "demo/v1"
    _write_parquet(root / "data.parquet")
    report, profile = run_contract(
        dataset_uri=root.as_posix(),
        dataset_family="universal",
        dataset_id="demo",
        version="v1",
    )
    out = tmp_path / "qc_out"
    artifacts = write_report(report=report, profile=profile, output_uri=out.as_posix())
    assert (out / "contract_report.json").is_file()
    assert (out / "contract_report.html").is_file()
    assert (out / "asset_profile.json").is_file()
    assert "report_uri" in artifacts
