from __future__ import annotations

import json
import time
from pathlib import Path

from robot_dh.perf.profiler import EtlProfiler
from robot_dh.perf.writer import write_perf_json


def test_perf_profiler_measures_phases(tmp_path: Path) -> None:
    with EtlProfiler(
        job_id="job-1",
        dataset_id="demo",
        version="v1",
        phase="normalize",
        input_uri="s3://bucket/raw/demo",
        output_uri=tmp_path.as_posix(),
    ) as prof:
        with prof.measure_download():
            time.sleep(0.02)
        with prof.measure_upload():
            time.sleep(0.02)
        time.sleep(0.02)
        prof.set_io(input_bytes=1024, output_bytes=2048, input_rows=10, output_rows=10)
        prof.add_metric("note", "ok")

    record = prof.record
    assert record.status == "OK"
    assert record.duration_sec > 0
    assert record.download_duration_sec >= 0.01
    assert record.upload_duration_sec >= 0.01
    assert record.compute_duration_sec >= 0.0
    assert record.input_bytes == 1024
    assert record.output_bytes == 2048
    assert record.metrics["note"] == "ok"


def test_perf_profiler_writes_json(tmp_path: Path) -> None:
    with EtlProfiler(
        job_id="job-x",
        dataset_id="demo",
        version="v1",
        phase="build_features",
    ) as prof:
        prof.set_io(input_bytes=1, output_bytes=2, input_rows=3, output_rows=4)

    target = write_perf_json(prof.record, tmp_path)
    assert target.is_file()
    payload = json.loads(target.read_text())
    assert payload["phase"] == "build_features"
    assert payload["status"] == "OK"
    assert payload["input_bytes"] == 1


def test_perf_profiler_records_failure(tmp_path: Path) -> None:
    try:
        with EtlProfiler(
            job_id="job-fail",
            dataset_id="demo",
            version="v1",
            phase="normalize",
        ) as prof:
            raise RuntimeError("boom")
    except RuntimeError:
        pass
    assert prof.record.status == "FAIL"
    assert "boom" in (prof.record.error_message or "")
