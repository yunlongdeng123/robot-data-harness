"""ml-ready export 编排入口。

输入：
- input_root  : dwd 根（含 dwd/<dataset_id>/<version>/episode_feature.parquet）
- quality_root: ads quality 根（含 episode_quality_score.parquet / dataset_quality_summary.parquet）
- qc_root     : 可选，含 qc/<dataset_id>/<version>/contract_report.json
- output      : ml-ready dataset 输出根

输出（output 目录）：
- train.parquet / val.parquet / test.parquet
- dataset_card.json / dataset_card.md
- feature_schema.json
- quality_filter.json
- lineage.json
- _manifest.json
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from robot_dh.lake.manifest import (
    CodeInfo,
    JobInfo,
    ManifestBuilder,
    collect_file_stats,
    utcnow_iso,
    write_manifest,
)
from robot_dh.lake.store import LakeStore, S3LakeStore, create_lake_store
from robot_dh.lake.uri import is_s3_uri, join_uri, parse_uri
from robot_dh.ml_ready.dataset_card import (
    build_dataset_card_json,
    build_dataset_card_md,
)
from robot_dh.ml_ready.lineage import build_lineage
from robot_dh.ml_ready.quality_filter import (
    apply_quality_filter,
    build_quality_filter,
)
from robot_dh.ml_ready.schema import build_feature_schema
from robot_dh.ml_ready.split import build_split

LOG = logging.getLogger(__name__)


@dataclass(slots=True)
class MlReadyResult:
    dataset_id: str
    version: str
    output_uri: str
    train_uri: str
    val_uri: str
    test_uri: str
    dataset_card_uri: str
    feature_schema_uri: str
    quality_filter_uri: str
    lineage_uri: str
    manifest_uri: str
    num_train: int
    num_val: int
    num_test: int
    job_id: str
    duration_sec: float
    metrics: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset_id": self.dataset_id,
            "version": self.version,
            "output_uri": self.output_uri,
            "train_uri": self.train_uri,
            "val_uri": self.val_uri,
            "test_uri": self.test_uri,
            "dataset_card_uri": self.dataset_card_uri,
            "feature_schema_uri": self.feature_schema_uri,
            "quality_filter_uri": self.quality_filter_uri,
            "lineage_uri": self.lineage_uri,
            "manifest_uri": self.manifest_uri,
            "num_train": self.num_train,
            "num_val": self.num_val,
            "num_test": self.num_test,
            "job_id": self.job_id,
            "duration_sec": self.duration_sec,
            "metrics": self.metrics,
        }


def _list_files(store: LakeStore, root: str) -> list[str]:
    return store.list(root)


def _read_parquet_uri(store: LakeStore, uri: str) -> pd.DataFrame:
    """读取本地 / S3 上的 parquet -> DataFrame。"""
    if is_s3_uri(uri):
        if not isinstance(store, S3LakeStore):
            raise RuntimeError("expected S3LakeStore")
        parsed = parse_uri(uri)
        import io

        resp = store.client.get_object(Bucket=parsed.bucket, Key=parsed.key)
        body = resp["Body"].read()
        return pq.read_table(pa.BufferReader(body)).to_pandas()
    return pq.read_table(parse_uri(uri).local_path).to_pandas()


def _gather_episode_feature(input_root: str) -> pd.DataFrame:
    """扫 input_root（dwd）下所有 episode_feature.parquet -> 合并 DataFrame。"""
    store = create_lake_store(input_root)
    out_frames: list[pd.DataFrame] = []
    for uri in _list_files(store, input_root):
        if uri.endswith("/episode_feature.parquet") or uri.endswith("\\episode_feature.parquet"):
            try:
                df = _read_parquet_uri(store, uri)
                out_frames.append(df)
            except Exception as err:
                LOG.warning("ml-ready: read episode_feature.parquet failed %s: %s", uri, err)
    if not out_frames:
        return pd.DataFrame()
    return pd.concat(out_frames, ignore_index=True)


def _gather_quality(quality_root: str | None) -> pd.DataFrame:
    if quality_root is None:
        return pd.DataFrame()
    store = create_lake_store(quality_root)
    out: list[pd.DataFrame] = []
    for uri in _list_files(store, quality_root):
        if uri.endswith("/episode_quality_score.parquet") or uri.endswith("\\episode_quality_score.parquet"):
            try:
                out.append(_read_parquet_uri(store, uri))
            except Exception as err:
                LOG.warning("ml-ready: read quality parquet failed %s: %s", uri, err)
    if not out:
        return pd.DataFrame()
    return pd.concat(out, ignore_index=True)


def _gather_qc_status(qc_root: str | None) -> pd.DataFrame:
    if qc_root is None:
        return pd.DataFrame()
    store = create_lake_store(qc_root)
    rows: list[dict[str, Any]] = []
    for uri in _list_files(store, qc_root):
        if not uri.endswith("/contract_report.json"):
            continue
        try:
            payload = store.read_json(uri)
            rows.append(
                {
                    "dataset_id": payload.get("dataset_id"),
                    "version": payload.get("version"),
                    "qc_status": payload.get("status"),
                    "qc_contract_id": payload.get("contract_id"),
                    "qc_run_id": payload.get("run_id"),
                }
            )
        except Exception as err:
            LOG.warning("ml-ready: read qc report failed %s: %s", uri, err)
    return pd.DataFrame(rows)


def _write_split(
    store: LakeStore,
    output_uri: str,
    df: pd.DataFrame,
    name: str,
    staging: Path,
) -> str:
    target_local = staging / f"{name}.parquet"
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), target_local)
    target_uri = join_uri(output_uri, f"{name}.parquet")
    store.upload_file(target_local, target_uri)
    return target_uri


def export_ml_ready(
    *,
    input_root: str,
    output_uri: str,
    quality_root: str | None = None,
    qc_root: str | None = None,
    dataset_id: str = "ml_ready",
    version: str = "v1",
    quality_threshold: float = 80.0,
    excluded_status: list[str] | None = None,
    split: tuple[float, float, float] = (0.8, 0.1, 0.1),
    family_filter: list[str] | None = None,
    min_episode_length: int | None = None,
    exclude_failed_contract: bool = True,
    job_id: str | None = None,
    known_limitations: list[str] | None = None,
) -> MlReadyResult:
    """汇总 dwd / ads / qc，按 quality 过滤后切 train/val/test。"""
    job_id = job_id or f"ml-ready-{uuid.uuid4().hex[:10]}"
    started = time.time()
    started_iso = utcnow_iso()
    LOG.info("ml-ready START job_id=%s output=%s", job_id, output_uri)

    df_features = _gather_episode_feature(input_root)
    df_quality = _gather_quality(quality_root)
    df_qc = _gather_qc_status(qc_root)

    if df_features.empty:
        raise ValueError(
            f"ml-ready export: no episode_feature.parquet under {input_root}; "
            "did you run build-features upstream?"
        )

    df = df_features.copy()
    if not df_quality.empty:
        join_cols = [c for c in ("episode_id", "dataset_id", "version") if c in df.columns and c in df_quality.columns]
        df = df.merge(df_quality, on=join_cols, how="left", suffixes=("", "_quality"))
    if not df_qc.empty:
        join_cols = [c for c in ("dataset_id", "version") if c in df.columns and c in df_qc.columns]
        df = df.merge(df_qc, on=join_cols, how="left")

    if "dataset_family" not in df.columns:
        df["dataset_family"] = df["dataset_id"].astype(str)
    if "selected_features_json" not in df.columns:
        # 把核心特征列打包成 json 字符串方便训练侧消费
        feature_keys = [
            c for c in (
                "max_velocity_mps", "quat_max_norm_error", "detected_press_count",
                "cluster_silhouette", "duration_sec", "num_samples",
            ) if c in df.columns
        ]
        df["selected_features_json"] = df[feature_keys].apply(lambda r: json.dumps(r.to_dict(), default=str), axis=1)
    if "row_count" not in df.columns:
        df["row_count"] = df.get("num_samples", 1)
    if "source_dwd_uri" not in df.columns:
        df["source_dwd_uri"] = df.apply(
            lambda r: f"{input_root.rstrip('/')}/{r.get('dataset_id', '')}/{r.get('version', 'v1')}/episode_feature.parquet",
            axis=1,
        )
    if "quality_score" not in df.columns:
        df["quality_score"] = 100.0  # 无 quality 数据时不过滤

    qf = build_quality_filter(
        quality_threshold=quality_threshold,
        excluded_status=excluded_status,
        split=split,
        family_filter=family_filter,
        min_episode_length=min_episode_length,
        exclude_failed_contract=exclude_failed_contract,
    )
    df_filtered = apply_quality_filter(
        df,
        quality_threshold=quality_threshold,
        excluded_status=excluded_status,
        family_filter=family_filter,
        min_episode_length=min_episode_length,
        exclude_failed_contract=exclude_failed_contract,
    )

    df_split = build_split(df_filtered, split=split)

    train_df = df_split[df_split["split"] == "train"]
    val_df = df_split[df_split["split"] == "val"]
    test_df = df_split[df_split["split"] == "test"]

    out_store = create_lake_store(output_uri)
    import tempfile

    with tempfile.TemporaryDirectory(prefix="robot-dh-mlready-") as tmp_str:
        staging = Path(tmp_str)
        train_uri = _write_split(out_store, output_uri, train_df, "train", staging)
        val_uri = _write_split(out_store, output_uri, val_df, "val", staging)
        test_uri = _write_split(out_store, output_uri, test_df, "test", staging)

        feature_schema = build_feature_schema(df_split)
        feature_schema_uri = join_uri(output_uri, "feature_schema.json")
        out_store.write_json(feature_schema_uri, feature_schema)

        qf_uri = join_uri(output_uri, "quality_filter.json")
        out_store.write_json(qf_uri, qf)

        families = sorted({str(x) for x in df_split.get("dataset_family", []) if pd.notna(x)})
        lineage = build_lineage(
            dataset_id=dataset_id,
            version=version,
            output_uri=output_uri,
            input_root=input_root,
            quality_root=quality_root,
            qc_root=qc_root,
            train_uri=train_uri,
            val_uri=val_uri,
            test_uri=test_uri,
        )
        lineage_uri = join_uri(output_uri, "lineage.json")
        out_store.write_json(lineage_uri, lineage)

        card = build_dataset_card_json(
            dataset_id=dataset_id,
            version=version,
            source_roots={"input_root": input_root, "quality_root": quality_root, "qc_root": qc_root},
            output_uri=output_uri,
            num_train=len(train_df),
            num_val=len(val_df),
            num_test=len(test_df),
            dataset_families=families,
            quality_policy=qf,
            lineage_uri=lineage_uri,
            schema_uri=feature_schema_uri,
            known_limitations=known_limitations,
        )
        card_json_uri = join_uri(output_uri, "dataset_card.json")
        card_md_uri = join_uri(output_uri, "dataset_card.md")
        out_store.write_json(card_json_uri, card)
        out_store.write_text(card_md_uri, build_dataset_card_md(card))

        # manifest
        # 把 staging 上传过的文件 + 小元数据共同登记
        manifest_files = []
        for name, target_uri in (
            ("train.parquet", train_uri),
            ("val.parquet", val_uri),
            ("test.parquet", test_uri),
        ):
            local = staging / name
            manifest_files.append({
                "path": name,
                "uri": target_uri,
                "format": "parquet",
                "size_bytes": int(local.stat().st_size) if local.is_file() else 0,
                "row_count": int(pq.ParquetFile(str(local)).metadata.num_rows) if local.is_file() else None,
                "checksum_sha256": None,
            })
        for name, uri_meta in (
            ("dataset_card.json", card_json_uri),
            ("dataset_card.md", card_md_uri),
            ("feature_schema.json", feature_schema_uri),
            ("quality_filter.json", qf_uri),
            ("lineage.json", lineage_uri),
        ):
            manifest_files.append({"path": name, "uri": uri_meta, "format": name.rsplit(".", 1)[-1], "size_bytes": 0, "row_count": None, "checksum_sha256": None})

        elapsed = time.time() - started
        finished_iso = utcnow_iso()
        builder = ManifestBuilder(
            dataset_id=dataset_id,
            version=version,
            layer="ml_ready",
            output_uri=output_uri,
            source_uris=[input_root] + ([quality_root] if quality_root else []) + ([qc_root] if qc_root else []),
            files=manifest_files,
            metrics={
                "num_train": len(train_df),
                "num_val": len(val_df),
                "num_test": len(test_df),
                "quality_threshold": float(quality_threshold),
                "duration_sec": elapsed,
            },
            job=JobInfo(
                job_id=job_id,
                job_type="ml_ready_export",
                started_at=started_iso,
                finished_at=finished_iso,
                duration_sec=elapsed,
            ),
            code=CodeInfo(package_version=_package_version()),
        )
        manifest_uri = write_manifest(out_store, builder)

    elapsed = time.time() - started
    LOG.info("ml-ready END job_id=%s status=OK duration=%.2fs", job_id, elapsed)
    return MlReadyResult(
        dataset_id=dataset_id,
        version=version,
        output_uri=output_uri,
        train_uri=train_uri,
        val_uri=val_uri,
        test_uri=test_uri,
        dataset_card_uri=card_json_uri,
        feature_schema_uri=feature_schema_uri,
        quality_filter_uri=qf_uri,
        lineage_uri=lineage_uri,
        manifest_uri=manifest_uri,
        num_train=len(train_df),
        num_val=len(val_df),
        num_test=len(test_df),
        job_id=job_id,
        duration_sec=elapsed,
        metrics={"families": families, "filter_kept": len(df_filtered), "filter_input": len(df)},
    )


def _package_version() -> str:
    try:
        from robot_dh import __version__
        return str(__version__)
    except Exception:
        return "unknown"
