from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from robot_dh.lake.manifest import (
    MANIFEST_FILENAME,
    MANIFEST_SCHEMA_VERSION,
    CodeInfo,
    JobInfo,
    ManifestBuilder,
    collect_file_stats,
    compute_file_sha256,
    read_manifest,
    write_manifest,
)
from robot_dh.lake.store import LocalLakeStore
from robot_dh.lake.uri import join_uri


def test_compute_file_sha256(tmp_path: Path) -> None:
    file = tmp_path / "data.txt"
    file.write_bytes(b"hello world")
    expected = hashlib.sha256(b"hello world").hexdigest()
    assert compute_file_sha256(file) == expected


def test_collect_file_stats_parquet_row_count(tmp_path: Path) -> None:
    table = pa.Table.from_pandas(pd.DataFrame({"x": [1, 2, 3]}), preserve_index=False)
    pq.write_table(table, tmp_path / "rows.parquet")
    (tmp_path / "other.txt").write_text("hi")

    stats = collect_file_stats(tmp_path, tmp_path.as_posix())
    by_name = {s["path"]: s for s in stats}
    assert by_name["rows.parquet"]["row_count"] == 3
    assert by_name["rows.parquet"]["format"] == "parquet"
    assert by_name["other.txt"]["row_count"] is None
    assert by_name["other.txt"]["size_bytes"] > 0
    for s in stats:
        assert s["uri"].endswith(s["path"])
        assert s["checksum_sha256"]


def test_manifest_builder_to_dict_round_trip(tmp_path: Path) -> None:
    (tmp_path / "pose.parquet").write_bytes(b"x")
    files = collect_file_stats(tmp_path, tmp_path.as_posix(), files=["pose.parquet"])
    builder = ManifestBuilder(
        dataset_id="demo",
        version="v1",
        layer="ods",
        output_uri=tmp_path.as_posix(),
        source_uris=["s3://robot-datasets/raw/demo/v1"],
        files=files,
        metrics={"num_samples": 1},
        job=JobInfo(
            job_id="abc",
            job_type="normalize",
            started_at="2026-05-21T00:00:00Z",
            finished_at="2026-05-21T00:00:01Z",
            duration_sec=1.0,
        ),
        code=CodeInfo(package_version="0.1.4"),
    )
    payload = builder.to_dict()
    assert payload["dataset_id"] == "demo"
    assert payload["version"] == "v1"
    assert payload["layer"] == "ods"
    assert payload["schema_version"] == MANIFEST_SCHEMA_VERSION
    assert payload["job"]["job_id"] == "abc"
    assert payload["code"]["package_version"] == "0.1.4"
    assert payload["files"][0]["path"] == "pose.parquet"


def test_write_and_read_manifest_local(tmp_path: Path) -> None:
    store = LocalLakeStore()
    out_uri = tmp_path.as_posix()
    (tmp_path / "pose.parquet").write_bytes(b"x")
    files = collect_file_stats(tmp_path, out_uri, files=["pose.parquet"])
    builder = ManifestBuilder(
        dataset_id="demo",
        version="v1",
        layer="ods",
        output_uri=out_uri,
        source_uris=["s3://robot-datasets/raw/demo/v1"],
        files=files,
        job=JobInfo("j", "normalize", "t0", "t1", 1.0),
        code=CodeInfo(package_version="0.1.4"),
    )
    manifest_uri = write_manifest(store, builder)
    assert manifest_uri == join_uri(out_uri, MANIFEST_FILENAME)
    assert (tmp_path / MANIFEST_FILENAME).is_file()

    loaded = read_manifest(store, out_uri)
    assert loaded["dataset_id"] == "demo"

    loaded_direct = read_manifest(store, manifest_uri)
    assert loaded_direct == loaded

    on_disk = json.loads((tmp_path / MANIFEST_FILENAME).read_text())
    assert {"dataset_id", "version", "layer", "created_at", "schema_version", "source_uris", "output_uri", "files"} <= set(on_disk)
