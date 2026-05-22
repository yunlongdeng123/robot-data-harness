"""normalize：raw -> ods。

输入：数据集 URI（本地或 s3://...）；S3 时先下载到临时目录，再经 DatasetLoader 或 v1.4 raw 适配器读取。
输出：ods 层 URI，含 pose/video_meta/episode_meta.parquet 与 _manifest.json。

pose 等 schema 见 robot_dh.lake.schema。v1.3 路径 endpose.pt + meta.yaml [+ video.mp4] 仍为基准；
HuggingFace 风格 parquet/HDF5 经保守适配器提取常见 7D 末端位姿。
"""

from __future__ import annotations

import json
import logging
import tempfile
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from robot_dh.data.dataset import DatasetBundle
from robot_dh.data.loaders import DatasetLoader
from robot_dh.etl.lineage import LineageEvent, write_lineage_events
from robot_dh.lake.hf_adapter import (
    is_huggingface_dataset_dir,
    load_huggingface_dataset,
)
from robot_dh.lake.manifest import (
    JobInfo,
    CodeInfo,
    ManifestBuilder,
    collect_file_stats,
    utcnow_iso,
    write_manifest,
)
from robot_dh.lake.schema import (
    EPISODE_META_SCHEMA,
    POSE_SCHEMA,
    VIDEO_META_SCHEMA,
)
from robot_dh.lake.store import LakeStore, create_lake_store
from robot_dh.lake.uri import is_s3_uri, join_uri, parse_uri
from robot_dh.validators.quaternion import normalize_quaternions
from robot_dh.warehouse.service import WarehouseService

LOG = logging.getLogger(__name__)


@dataclass(slots=True)
class NormalizeResult:
    dataset_id: str
    version: str
    episode_id: str
    output_uri: str
    manifest_uri: str
    pose_uri: str
    video_meta_uri: str | None
    episode_meta_uri: str
    num_samples: int
    duration_sec: float
    source_uris: list[str]
    job_id: str
    duration_job_sec: float
    files: list[dict[str, Any]] = field(default_factory=list)


def _materialize_input(dataset_uri: str, work_dir: Path) -> Path:
    """将数据集落到本地磁盘并返回目录路径。"""
    parsed = parse_uri(dataset_uri)
    if parsed.is_s3:
        local = work_dir / "input"
        local.mkdir(parents=True, exist_ok=True)
        store = create_lake_store(dataset_uri)
        store.download_dir(dataset_uri, local)
        return local
    return Path(parsed.local_path).expanduser().resolve()


def _load_raw_bundles(
    input_dir: Path,
    *,
    dataset_id: str | None,
    version: str | None,
) -> list[DatasetBundle]:
    """加载 raw 输入为一个或多个 episode。"""

    if (input_dir / "endpose.pt").is_file():
        return [DatasetLoader().load(input_dir)]
    if is_huggingface_dataset_dir(input_dir):
        return load_huggingface_dataset(input_dir, dataset_id=dataset_id, version=version)
    raise FileNotFoundError(
        f"Unsupported raw dataset layout at {input_dir}. Expected endpose.pt or "
        "HuggingFace-style parquet/HDF5 files under data/."
    )


def _build_pose_table(bundle: DatasetBundle, dataset_id: str, version: str, episode_id: str) -> pa.Table:
    n = int(bundle.pose.shape[0])
    quat = bundle.quaternions
    normalized = normalize_quaternions(quat)
    quat_norm = np.linalg.norm(quat, axis=1)

    df = pd.DataFrame(
        {
            "episode_id": np.full(n, episode_id, dtype=object),
            "dataset_id": np.full(n, dataset_id, dtype=object),
            "version": np.full(n, version, dtype=object),
            "frame_idx": np.arange(n, dtype=np.int64),
            "timestamp_sec": bundle.timestamps.astype(np.float64),
            "x": bundle.xyz[:, 0].astype(np.float64),
            "y": bundle.xyz[:, 1].astype(np.float64),
            "z": bundle.xyz[:, 2].astype(np.float64),
            "qx": normalized[:, 0].astype(np.float64),
            "qy": normalized[:, 1].astype(np.float64),
            "qz": normalized[:, 2].astype(np.float64),
            "qw": normalized[:, 3].astype(np.float64),
            "quat_norm": quat_norm.astype(np.float64),
        }
    )
    return pa.Table.from_pandas(df, schema=POSE_SCHEMA, preserve_index=False)


def _build_pose_table_many(identity_bundles: list[tuple[DatasetBundle, str, str, str]]) -> pa.Table:
    tables = [
        _build_pose_table(bundle, dataset_id, version, episode_id)
        for bundle, dataset_id, version, episode_id in identity_bundles
    ]
    if not tables:
        raise ValueError("normalize produced no pose episodes")
    return pa.concat_tables(tables, promote_options="none")


def _build_video_meta_table(
    bundle: DatasetBundle,
    dataset_id: str,
    version: str,
    source_uri: str,
) -> pa.Table:
    video_uri = ""
    width = 0
    height = 0
    if bundle.video_path is not None:
        video_uri = join_uri(source_uri, bundle.video_path.name) if source_uri else bundle.video_path.as_posix()
        try:
            import cv2

            cap = cv2.VideoCapture(str(bundle.video_path))
            if cap.isOpened():
                width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
                height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
            cap.release()
        except Exception as err:  # noqa: BLE001
            LOG.warning("video size probe failed: %s", err)

    df = pd.DataFrame(
        [
            {
                "dataset_id": dataset_id,
                "version": version,
                "video_uri": video_uri,
                "fps": float(bundle.video_meta.fps),
                "frame_count": int(bundle.video_meta.frame_count),
                "duration_sec": float(bundle.video_meta.duration_sec),
                "width": int(width),
                "height": int(height),
            }
        ]
    )
    return pa.Table.from_pandas(df, schema=VIDEO_META_SCHEMA, preserve_index=False)


def _build_video_meta_table_many(
    identity_bundles: list[tuple[DatasetBundle, str, str, str]],
    source_uri: str,
) -> pa.Table:
    if len(identity_bundles) == 1:
        bundle, dataset_id, version, _episode_id = identity_bundles[0]
        return _build_video_meta_table(bundle, dataset_id, version, source_uri)

    dataset_id = identity_bundles[0][1]
    version = identity_bundles[0][2]
    frame_count = int(sum(bundle.pose.shape[0] for bundle, *_ in identity_bundles))
    duration_sec = float(
        sum(
            bundle.timestamps[-1] - bundle.timestamps[0] if bundle.timestamps.size > 1 else 0.0
            for bundle, *_ in identity_bundles
        )
    )
    fps_values = [float(bundle.video_meta.fps) for bundle, *_ in identity_bundles if bundle.video_meta.fps > 0]
    fps = float(np.median(fps_values)) if fps_values else 0.0
    df = pd.DataFrame(
        [
            {
                "dataset_id": dataset_id,
                "version": version,
                "video_uri": "",
                "fps": fps,
                "frame_count": frame_count,
                "duration_sec": duration_sec,
                "width": 0,
                "height": 0,
            }
        ]
    )
    return pa.Table.from_pandas(df, schema=VIDEO_META_SCHEMA, preserve_index=False)


def _build_episode_meta_table(
    bundle: DatasetBundle,
    dataset_id: str,
    version: str,
    episode_id: str,
    source_uri: str,
) -> pa.Table:
    duration_sec = float(bundle.timestamps[-1] - bundle.timestamps[0]) if bundle.timestamps.size > 1 else 0.0
    df = pd.DataFrame(
        [
            {
                "dataset_id": dataset_id,
                "version": version,
                "episode_id": episode_id,
                "num_samples": int(bundle.pose.shape[0]),
                "duration_sec": duration_sec,
                "source_uri": source_uri,
                "created_at": utcnow_iso(),
                "meta_json": json.dumps(bundle.meta, ensure_ascii=False, default=str),
            }
        ]
    )
    return pa.Table.from_pandas(df, schema=EPISODE_META_SCHEMA, preserve_index=False)


def _build_episode_meta_table_many(
    identity_bundles: list[tuple[DatasetBundle, str, str, str]],
    source_uri: str,
) -> pa.Table:
    tables = [
        _build_episode_meta_table(bundle, dataset_id, version, episode_id, source_uri)
        for bundle, dataset_id, version, episode_id in identity_bundles
    ]
    return pa.concat_tables(tables, promote_options="none")


def _resolve_identity(
    bundle: DatasetBundle,
    dataset_id_override: str | None,
    version_override: str | None,
) -> tuple[str, str, str]:
    """返回 (dataset_id, version, episode_id)。"""
    meta = bundle.meta or {}
    dataset_id = dataset_id_override or str(meta.get("dataset_id") or bundle.dataset_id or bundle.dataset_path.name)
    version = version_override or str(meta.get("version") or meta.get("dataset_version") or "v1")
    episode_id = str(meta.get("episode_id") or meta.get("episode") or dataset_id)
    return dataset_id, version, episode_id


def _package_version() -> str:
    try:
        from robot_dh import __version__

        return str(__version__)
    except Exception:
        return "unknown"


def normalize_dataset(
    *,
    dataset_uri: str,
    output_uri: str,
    dataset_id: str | None = None,
    version: str | None = None,
    job_id: str | None = None,
    warehouse: WarehouseService | None = None,
    lake_root_uri: str | None = None,
) -> NormalizeResult:
    """执行 raw -> ods 规范化，返回 NormalizeResult。

    Args:
        dataset_uri  : raw 数据集路径（本地目录 / s3://...）
        output_uri   : ods slice 路径（.../ods/{dataset_id}/{version}）
        dataset_id   : 覆盖身份（否则从 meta.yaml/目录名读取）
        version      : 覆盖版本（否则 meta.yaml 或默认 v1）
        job_id       : 可选 job_id，缺省自动生成
        warehouse    : 可选 WarehouseService，默认 soft 模式写 Postgres 元数据
        lake_root_uri: 可选，用于 lineage JSONL 写入（如 s3://robot-lake/）
    """
    if warehouse is None:
        warehouse = WarehouseService(soft=True)

    job_id = job_id or f"normalize-{uuid.uuid4().hex[:12]}"
    started = time.time()
    started_iso = utcnow_iso()
    LOG.info("normalize: job_id=%s dataset_uri=%s output_uri=%s", job_id, dataset_uri, output_uri)

    warehouse.record_etl_job_start(
        job_id=job_id,
        job_type="normalize",
        input_uri=dataset_uri,
        output_uri=output_uri,
    )

    out_store = create_lake_store(output_uri)
    try:
        with tempfile.TemporaryDirectory(prefix="robot-dh-normalize-") as tmp_str:
            tmp = Path(tmp_str)
            input_dir = _materialize_input(dataset_uri, tmp)
            bundles = _load_raw_bundles(input_dir, dataset_id=dataset_id, version=version)
            identity_bundles = [
                (bundle, *_resolve_identity(bundle, dataset_id, version))
                for bundle in bundles
            ]
            ds_id = identity_bundles[0][1]
            ver = identity_bundles[0][2]
            ep_id = identity_bundles[0][3]
            total_samples = int(sum(bundle.pose.shape[0] for bundle, *_ in identity_bundles))
            total_duration_sec = float(
                sum(
                    bundle.timestamps[-1] - bundle.timestamps[0] if bundle.timestamps.size > 1 else 0.0
                    for bundle, *_ in identity_bundles
                )
            )

            staging = tmp / "ods"
            staging.mkdir(parents=True, exist_ok=True)

            pose_table = _build_pose_table_many(identity_bundles)
            pq.write_table(pose_table, staging / "pose.parquet")

            video_table = _build_video_meta_table_many(identity_bundles, dataset_uri)
            pq.write_table(video_table, staging / "video_meta.parquet")

            episode_table = _build_episode_meta_table_many(identity_bundles, dataset_uri)
            pq.write_table(episode_table, staging / "episode_meta.parquet")

            uploaded = out_store.upload_dir(staging, output_uri)
            files = collect_file_stats(
                staging,
                output_uri,
                files=["pose.parquet", "video_meta.parquet", "episode_meta.parquet"],
            )

            elapsed = time.time() - started
            finished_iso = utcnow_iso()

            for info in files:
                warehouse.record_lake_asset(
                    dataset_id=ds_id,
                    version=ver,
                    layer="ods",
                    asset_type=info["path"].replace(".parquet", "_parquet"),
                    uri=info["uri"],
                    format=info["format"],
                    size_bytes=info["size_bytes"],
                    row_count=info["row_count"],
                    checksum=info["checksum_sha256"],
                )

            warehouse.record_lineage_edge(
                source_uri=dataset_uri,
                target_uri=output_uri,
                job_id=job_id,
                job_type="normalize",
            )
            for info in files:
                warehouse.record_lineage_edge(
                    source_uri=dataset_uri,
                    target_uri=info["uri"],
                    job_id=job_id,
                    job_type="normalize",
                )

            warehouse.upsert_dataset_version(
                dataset_id=ds_id,
                version=ver,
                ods_uri=output_uri,
                status="normalized",
            )

            metrics = {
                "rows_in": total_samples,
                "rows_out": int(pose_table.num_rows),
                "bytes_out": int(sum(info["size_bytes"] for info in files)),
                "duration_ms": int(elapsed * 1000),
            }
            warehouse.record_etl_job_finish(job_id=job_id, status="OK", metrics=metrics)

            if lake_root_uri:
                events = [
                    LineageEvent(
                        job_id=job_id,
                        job_type="normalize",
                        source_uri=dataset_uri,
                        target_uri=info["uri"],
                    )
                    for info in files
                ]
                lineage_store = create_lake_store(lake_root_uri)
                write_lineage_events(lineage_store, lake_root_uri, events)

            builder = ManifestBuilder(
                dataset_id=ds_id,
                version=ver,
                layer="ods",
                output_uri=output_uri,
                source_uris=[dataset_uri],
                files=files,
                metrics={
                    "num_samples": total_samples,
                    "num_episodes": len(identity_bundles),
                    "duration_sec": total_duration_sec,
                },
                job=JobInfo(
                    job_id=job_id,
                    job_type="normalize",
                    started_at=started_iso,
                    finished_at=finished_iso,
                    duration_sec=elapsed,
                ),
                code=CodeInfo(package_version=_package_version()),
            )
            manifest_uri = write_manifest(out_store, builder)

            return NormalizeResult(
                dataset_id=ds_id,
                version=ver,
                episode_id=ep_id,
                output_uri=output_uri,
                manifest_uri=manifest_uri,
                pose_uri=uploaded.get("pose.parquet", join_uri(output_uri, "pose.parquet")),
                video_meta_uri=uploaded.get("video_meta.parquet", join_uri(output_uri, "video_meta.parquet")),
                episode_meta_uri=uploaded.get("episode_meta.parquet", join_uri(output_uri, "episode_meta.parquet")),
                num_samples=total_samples,
                duration_sec=total_duration_sec,
                source_uris=[dataset_uri],
                job_id=job_id,
                duration_job_sec=elapsed,
                files=files,
            )
    except Exception as err:
        warehouse.record_etl_job_finish(job_id=job_id, status="FAIL", error_message=str(err))
        raise
