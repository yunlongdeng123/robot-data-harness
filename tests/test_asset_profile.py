"""asset profile：files / parquet probe / schema_hash。"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from robot_dh.qc.profile import profile_dataset


def test_profile_basic(tmp_path: Path) -> None:
    root = tmp_path / "demo/v1"
    root.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame({"a": list(range(20)), "b": [1.0] * 20})
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), root / "shard_001.parquet")
    profile = profile_dataset(
        dataset_uri=root.as_posix(),
        dataset_id="demo",
        version="v1",
        dataset_family="universal",
    )
    assert profile.files_count == 1
    assert profile.rows is not None and profile.rows >= 20
    assert profile.schema_hash is not None and len(profile.schema_hash) == 64
    parquet_probes = profile.profile.get("parquet") or []
    assert parquet_probes and parquet_probes[0]["readable"] is True
