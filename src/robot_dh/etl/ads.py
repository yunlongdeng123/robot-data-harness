"""build-ads：dwd -> ads/quality。

输入：多个 dwd/{dataset_id}/{version}/episode_feature.parquet（及可选 v1.3 registry：validator_results、gate_results）。
输出：dataset_quality_summary、validator_failure_stats、episode_quality_score.parquet 与 _manifest.json。
"""

from __future__ import annotations

import logging
import tempfile
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from robot_dh.etl.lineage import LineageEvent, write_lineage_events
from robot_dh.lake.manifest import (
    CodeInfo,
    JobInfo,
    ManifestBuilder,
    collect_file_stats,
    utcnow_iso,
    write_manifest,
)
from robot_dh.lake.schema import (
    DATASET_QUALITY_SUMMARY_SCHEMA,
    EPISODE_QUALITY_SCORE_SCHEMA,
    VALIDATOR_FAILURE_STATS_SCHEMA,
)
from robot_dh.lake.store import LakeStore, create_lake_store
from robot_dh.lake.uri import is_s3_uri, join_uri, parse_uri
from robot_dh.warehouse.service import WarehouseService

LOG = logging.getLogger(__name__)

DEFAULT_ADS_CFG: dict[str, Any] = {
    "score": {
        "base": 100.0,
        "max_velocity_penalty": {
            "threshold_mps": 2.5,
            "per_unit_over": 5.0,
            "cap": 30.0,
        },
        "quat_error_penalty": {
            "threshold": 1.0e-3,
            "per_unit_over": 200.0,
            "cap": 30.0,
        },
        "press_count_penalty": {
            "target": 25,
            "tolerance": 5,
            "per_unit_off": 1.0,
            "cap": 20.0,
        },
        "cluster_silhouette_penalty": {
            "good": 0.5,
            "zero_below": -0.1,
            "per_unit_under": 40.0,
            "cap": 20.0,
        },
    }
}


def _load_ads_config(config_path: Path | None) -> dict[str, Any]:
    cfg = dict(DEFAULT_ADS_CFG)
    if config_path is None:
        return cfg
    if not config_path.is_file():
        raise FileNotFoundError(f"ads config not found: {config_path}")
    with config_path.open("r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}
    section = (raw.get("etl") or {}).get("ads") or {}
    if "score" in section and isinstance(section["score"], dict):
        merged_score = dict(cfg["score"])
        for k, v in section["score"].items():
            if isinstance(v, dict) and isinstance(merged_score.get(k), dict):
                merged_score[k] = {**merged_score[k], **v}
            else:
                merged_score[k] = v
        cfg["score"] = merged_score
    return cfg


@dataclass(slots=True)
class AdsResult:
    output_uri: str
    manifest_uri: str
    job_id: str
    duration_job_sec: float
    num_episodes: int
    num_datasets: int
    files: list[dict[str, Any]] = field(default_factory=list)


def _discover_dwd_episode_features(store: LakeStore, dwd_root_uri: str) -> list[tuple[str, str, str]]:
    """返回 (dataset_id, version, episode_feature_uri) 列表。"""
    discovered: list[tuple[str, str, str]] = []
    if is_s3_uri(dwd_root_uri):
        prefix = parse_uri(dwd_root_uri).key.rstrip("/") + "/"
    else:
        prefix = str(parse_uri(dwd_root_uri).local_path).rstrip("/") + "/"

    for uri in store.list(dwd_root_uri):
        if not uri.endswith("/episode_feature.parquet"):
            continue
        if is_s3_uri(uri):
            tail = parse_uri(uri).key
        else:
            tail = uri
        if not tail.startswith(prefix):
            continue
        rel = tail[len(prefix):]
        parts = rel.split("/")
        if len(parts) < 3:
            continue
        dataset_id, version = parts[0], parts[1]
        discovered.append((dataset_id, version, uri))
    return discovered


def _read_episode_features(local_dir: Path, sources: list[tuple[str, str, Path]]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for ds, ver, local_path in sources:
        if not local_path.is_file():
            continue
        df = pq.read_table(local_path).to_pandas()
        if df.empty:
            continue
        df["dataset_id"] = df["dataset_id"].astype(str)
        df["version"] = df["version"].astype(str)
        df["dataset_id"] = df["dataset_id"].where(df["dataset_id"] != "", ds)
        df["version"] = df["version"].where(df["version"] != "", ver)
        frames.append(df)
    if not frames:
        return pd.DataFrame(columns=[f.name for f in EPISODE_QUALITY_SCORE_SCHEMA])
    return pd.concat(frames, axis=0, ignore_index=True)


def _compute_episode_quality_score(episode_df: pd.DataFrame, cfg: dict[str, Any]) -> pd.DataFrame:
    if episode_df.empty:
        return pd.DataFrame(columns=[f.name for f in EPISODE_QUALITY_SCORE_SCHEMA])
    s = cfg["score"]
    base = float(s["base"])

    mv = episode_df["max_velocity_mps"].astype(float).to_numpy()
    vp = s["max_velocity_penalty"]
    mv_pen = np.clip((mv - float(vp["threshold_mps"])) * float(vp["per_unit_over"]), 0.0, float(vp["cap"]))

    qe = episode_df["quat_max_norm_error"].astype(float).to_numpy()
    qep = s["quat_error_penalty"]
    qe_pen = np.clip((qe - float(qep["threshold"])) * float(qep["per_unit_over"]), 0.0, float(qep["cap"]))

    pc = episode_df["detected_press_count"].astype(int).to_numpy()
    pcp = s["press_count_penalty"]
    pc_pen = np.clip((np.abs(pc - int(pcp["target"])) - int(pcp["tolerance"])) * float(pcp["per_unit_off"]), 0.0, float(pcp["cap"]))

    sil = episode_df["cluster_silhouette"].astype(float).fillna(float(s["cluster_silhouette_penalty"]["zero_below"])).to_numpy()
    silp = s["cluster_silhouette_penalty"]
    sil_pen = np.clip((float(silp["good"]) - sil) * float(silp["per_unit_under"]), 0.0, float(silp["cap"]))

    score = base - mv_pen - qe_pen - pc_pen - sil_pen
    score = np.clip(score, 0.0, base)
    status = np.where(score >= 80.0, "PASS", np.where(score >= 50.0, "WARN", "FAIL"))

    out = pd.DataFrame(
        {
            "episode_id": episode_df["episode_id"].astype(str),
            "dataset_id": episode_df["dataset_id"].astype(str),
            "version": episode_df["version"].astype(str),
            "quality_score": score.astype(np.float64),
            "quality_status": status.astype(str),
            "max_velocity_mps": episode_df["max_velocity_mps"].astype(np.float64),
            "quat_max_norm_error": episode_df["quat_max_norm_error"].astype(np.float64),
            "detected_press_count": episode_df["detected_press_count"].astype(np.int64),
            "cluster_silhouette": episode_df["cluster_silhouette"].astype("float64", errors="ignore"),
        },
        columns=[f.name for f in EPISODE_QUALITY_SCORE_SCHEMA],
    )
    return out


def _compute_dataset_summary(
    episode_quality: pd.DataFrame,
    episode_features: pd.DataFrame,
) -> pd.DataFrame:
    if episode_quality.empty:
        return pd.DataFrame(columns=[f.name for f in DATASET_QUALITY_SUMMARY_SCHEMA])

    con = duckdb.connect(":memory:")
    con.register("eq", episode_quality)
    con.register("ef", episode_features)
    rows = con.execute(
        """
        SELECT
          eq.dataset_id,
          eq.version,
          COUNT(*) AS num_episodes,
          AVG(eq.quality_score) AS avg_quality_score,
          SUM(CASE WHEN eq.quality_status = 'PASS' THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS pass_rate,
          AVG(eq.max_velocity_mps) AS avg_max_velocity_mps,
          AVG(eq.cluster_silhouette) AS avg_cluster_silhouette,
          SUM(eq.detected_press_count) AS total_press_count
        FROM eq
        GROUP BY eq.dataset_id, eq.version
        ORDER BY eq.dataset_id, eq.version
        """
    ).fetchdf()
    con.close()
    rows["updated_at"] = utcnow_iso()
    rows["num_episodes"] = rows["num_episodes"].astype(np.int64)
    rows["total_press_count"] = rows["total_press_count"].astype(np.int64)
    rows = rows[[f.name for f in DATASET_QUALITY_SUMMARY_SCHEMA]]
    return rows


def _compute_validator_failure_stats(db_uri: str | None) -> pd.DataFrame:
    """若存在 v1.3 validator_results 表则聚合；否则返回空表。"""
    from sqlalchemy import inspect, text

    try:
        from robot_dh.registry import get_engine, resolve_db_uri

        engine = get_engine(resolve_db_uri(db_uri))
        existing = set(inspect(engine).get_table_names())
        if "validator_results" not in existing:
            return pd.DataFrame(columns=[f.name for f in VALIDATOR_FAILURE_STATS_SCHEMA])
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT validator_name, status FROM validator_results"
                )
            ).all()
    except Exception as err:  # noqa: BLE001
        LOG.warning("validator_failure_stats: DB unavailable (%s); writing empty table", err)
        return pd.DataFrame(columns=[f.name for f in VALIDATOR_FAILURE_STATS_SCHEMA])

    if not rows:
        return pd.DataFrame(columns=[f.name for f in VALIDATOR_FAILURE_STATS_SCHEMA])

    df = pd.DataFrame(rows, columns=["validator_name", "status"])
    grouped = df.groupby("validator_name")["status"]
    out = grouped.agg(
        total_runs="count",
        fail_count=lambda s: int((s == "FAIL").sum()),
        warn_count=lambda s: int((s == "WARN").sum()),
    ).reset_index()
    out["failure_rate"] = (out["fail_count"] / out["total_runs"]).astype(float)
    out["updated_at"] = utcnow_iso()
    out["total_runs"] = out["total_runs"].astype(np.int64)
    out["fail_count"] = out["fail_count"].astype(np.int64)
    out["warn_count"] = out["warn_count"].astype(np.int64)
    out = out[[f.name for f in VALIDATOR_FAILURE_STATS_SCHEMA]]
    return out


def _write_ads_parquets(
    local_dir: Path,
    dataset_summary: pd.DataFrame,
    validator_stats: pd.DataFrame,
    episode_quality: pd.DataFrame,
) -> None:
    pq.write_table(
        pa.Table.from_pandas(dataset_summary, schema=DATASET_QUALITY_SUMMARY_SCHEMA, preserve_index=False),
        local_dir / "dataset_quality_summary.parquet",
    )
    pq.write_table(
        pa.Table.from_pandas(validator_stats, schema=VALIDATOR_FAILURE_STATS_SCHEMA, preserve_index=False),
        local_dir / "validator_failure_stats.parquet",
    )
    pq.write_table(
        pa.Table.from_pandas(episode_quality, schema=EPISODE_QUALITY_SCORE_SCHEMA, preserve_index=False),
        local_dir / "episode_quality_score.parquet",
    )


def _package_version() -> str:
    try:
        from robot_dh import __version__

        return str(__version__)
    except Exception:
        return "unknown"


def build_ads(
    *,
    input_root_uri: str,
    output_uri: str,
    config_path: Path | None = None,
    job_id: str | None = None,
    db_uri: str | None = None,
    warehouse: WarehouseService | None = None,
    lake_root_uri: str | None = None,
) -> AdsResult:
    """构建 ads/quality slice；input_root_uri 应指向 <lake>/dwd。"""
    if warehouse is None:
        warehouse = WarehouseService(soft=True, db_uri=db_uri)
    cfg = _load_ads_config(config_path)

    job_id = job_id or f"build-ads-{uuid.uuid4().hex[:12]}"
    started = time.time()
    started_iso = utcnow_iso()
    LOG.info("build-ads: job_id=%s input_root=%s output=%s", job_id, input_root_uri, output_uri)

    warehouse.record_etl_job_start(
        job_id=job_id,
        job_type="build_ads",
        input_uri=input_root_uri,
        output_uri=output_uri,
    )

    in_store = create_lake_store(input_root_uri)
    out_store = create_lake_store(output_uri)

    try:
        with tempfile.TemporaryDirectory(prefix="robot-dh-ads-") as tmp_str:
            tmp = Path(tmp_str)

            discovered = _discover_dwd_episode_features(in_store, input_root_uri)
            sources: list[tuple[str, str, Path]] = []
            for ds, ver, uri in discovered:
                local = tmp / "dwd_cache" / ds / ver / "episode_feature.parquet"
                local.parent.mkdir(parents=True, exist_ok=True)
                in_store.download_dir(uri, local.parent)
                sources.append((ds, ver, local))

            episode_features = _read_episode_features(tmp / "dwd_cache", sources)
            episode_quality = _compute_episode_quality_score(episode_features, cfg)
            dataset_summary = _compute_dataset_summary(episode_quality, episode_features)
            validator_stats = _compute_validator_failure_stats(db_uri)

            staging = tmp / "ads_quality"
            staging.mkdir(parents=True, exist_ok=True)
            _write_ads_parquets(staging, dataset_summary, validator_stats, episode_quality)

            uploaded = out_store.upload_dir(staging, output_uri)
            files = collect_file_stats(
                staging,
                output_uri,
                files=[
                    "dataset_quality_summary.parquet",
                    "validator_failure_stats.parquet",
                    "episode_quality_score.parquet",
                ],
            )

            for info in files:
                warehouse.record_lake_asset(
                    dataset_id="__shared__",
                    version="quality",
                    layer="ads",
                    asset_type=info["path"].replace(".parquet", "_parquet"),
                    uri=info["uri"],
                    format=info["format"],
                    size_bytes=info["size_bytes"],
                    row_count=info["row_count"],
                    checksum=info["checksum_sha256"],
                )

            for ds, ver, uri in discovered:
                warehouse.record_lineage_edge(
                    source_uri=uri,
                    target_uri=output_uri,
                    job_id=job_id,
                    job_type="build_ads",
                )

            if not dataset_summary.empty:
                for _, row in dataset_summary.iterrows():
                    warehouse.record_quality_snapshot(
                        dataset_id=str(row["dataset_id"]),
                        version=str(row["version"]),
                        run_id=job_id,
                        quality_status="PASS" if float(row["pass_rate"]) >= 0.8 else ("WARN" if float(row["pass_rate"]) >= 0.5 else "FAIL"),
                        quality_score=float(row["avg_quality_score"]),
                        metrics={
                            "num_episodes": int(row["num_episodes"]),
                            "pass_rate": float(row["pass_rate"]),
                            "avg_max_velocity_mps": float(row["avg_max_velocity_mps"]),
                            "avg_cluster_silhouette": float(row["avg_cluster_silhouette"]) if pd.notna(row["avg_cluster_silhouette"]) else None,
                            "total_press_count": int(row["total_press_count"]),
                        },
                    )

            elapsed = time.time() - started
            metrics = {
                "rows_in": int(len(episode_features)),
                "rows_out": int(len(episode_quality)),
                "num_datasets": int(len(dataset_summary)),
                "duration_ms": int(elapsed * 1000),
            }
            warehouse.record_etl_job_finish(job_id=job_id, status="OK", metrics=metrics)

            if lake_root_uri:
                events = [
                    LineageEvent(
                        job_id=job_id,
                        job_type="build_ads",
                        source_uri=uri,
                        target_uri=output_uri,
                    )
                    for _, _, uri in discovered
                ]
                lineage_store = create_lake_store(lake_root_uri)
                write_lineage_events(lineage_store, lake_root_uri, events)

            finished_iso = utcnow_iso()
            builder = ManifestBuilder(
                dataset_id="__shared__",
                version="quality",
                layer="ads",
                output_uri=output_uri,
                source_uris=[uri for _, _, uri in discovered],
                files=files,
                metrics=metrics,
                job=JobInfo(
                    job_id=job_id,
                    job_type="build_ads",
                    started_at=started_iso,
                    finished_at=finished_iso,
                    duration_sec=elapsed,
                ),
                code=CodeInfo(package_version=_package_version()),
            )
            manifest_uri = write_manifest(out_store, builder)

            return AdsResult(
                output_uri=output_uri,
                manifest_uri=manifest_uri,
                job_id=job_id,
                duration_job_sec=elapsed,
                num_episodes=int(len(episode_quality)),
                num_datasets=int(len(dataset_summary)),
                files=files,
            )
    except Exception as err:
        warehouse.record_etl_job_finish(job_id=job_id, status="FAIL", error_message=str(err))
        raise
