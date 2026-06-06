"""normalize：raw -> ods（v1.6 增强版）。

v1.6 新增：
- _checkpoint.json：步骤级 checkpoint，重跑时可避免重新下载/转换；
- HeartbeatReporter：normalize 各阶段周期性写心跳；
- 子阶段 EtlProfiler：substage metrics（materialize / load / build / upload / manifest）；
- resume / force / SKIP 三态：manifest 已存在直接 skip；checkpoint 完整则 skip 写入；--force 强制重跑。

输入：数据集 URI（本地或 s3://...）；S3 时先下载到临时目录。
输出：ods 层 URI，含 pose/video_meta/episode_meta.parquet + _manifest.json + _checkpoint.json。
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
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
    MANIFEST_FILENAME,
    ManifestBuilder,
    collect_file_stats,
    read_manifest,
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
from robot_dh.perf.profiler import EtlProfiler
from robot_dh.perf.writer import write_perf_json
from robot_dh.progress.checkpoint import (
    CHECKPOINT_FILENAME,
    Checkpoint,
    CheckpointFile,
    CheckpointStore,
)
from robot_dh.progress.heartbeat import HeartbeatReporter
from robot_dh.progress.progress_logger import ProgressLogger
from robot_dh.validators.quaternion import normalize_quaternions
from robot_dh.warehouse.service import WarehouseService

LOG = logging.getLogger(__name__)


# Step 名称（用于 checkpoint.completed_steps）
STEP_MATERIALIZE_INPUT = "materialize_input"
STEP_LOAD_BUNDLES = "load_bundles"
STEP_WRITE_POSE_PARQUET = "write_pose_parquet"
STEP_WRITE_VIDEO_META = "write_video_meta"
STEP_WRITE_EPISODE_META = "write_episode_meta"
STEP_UPLOAD_OUTPUTS = "upload_outputs"
STEP_WRITE_MANIFEST = "write_manifest"

EXPECTED_OUTPUT_FILES = ("pose.parquet", "video_meta.parquet", "episode_meta.parquet")


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
    status: str = "OK"  # OK / SKIPPED / RESUMED
    completed_steps: list[str] = field(default_factory=list)
    # v1.8 修复：携带 sub-stage profiler 采集的精细 metrics
    # （materialize / load / build / upload / manifest），由 cli 上层 perf_records_from_etl_run
    # 合并到 PerfRecord.metrics，保证单点写 PG，不再让 normalize 内部直接写库制造 RUNNING 孤儿。
    metrics: dict[str, Any] = field(default_factory=dict)


def _materialize_input(
    dataset_uri: str,
    work_dir: Path,
    *,
    resume: bool = False,
    cache_root: Path | None = None,
) -> Path:
    """把 raw 数据集物化到本地目录。

    v1.6.6：``resume=True`` 时优先把下载落到一个**跨进程稳定**的 cache 目录
    （默认 ``ROBOT_DH_INPUT_CACHE_DIR``，未设则用 ``/tmp/robot-dh/input-cache``）。
    路径用 ``hash(dataset_uri)`` 做 key，目录下若已存在等大小文件会被
    ``S3LakeStore.download_dir`` 跳过，从而把"上一次跑挂了 step 容器被清理"导致的
    227 MiB 重新下载砍掉，对应 v1.6 fhkvr 报告 §4.C / 验收第 4 项。

    v1.6.7：lerobot v2 layout（``meta/info.json`` 存在）下自动 ``exclude_prefixes
    =("videos/",)``，把 droid_lerobot_scale30 的 18.4 GiB 全量下载砍到 ~14 GiB
    （只下 ``data/`` chunk parquet + ``meta/`` info/stats）。视频不参与 ods
    pose/episode_meta 计算，只需要保留 raw URI 引用，等 ml-ready 阶段需要时再拉。
    并打开 ``progress_log_every=50`` 让 download_dir 中段每 50 个文件 LOG.info 一条
    进度，规避"static log 一行后静默几小时"。
    """
    parsed = parse_uri(dataset_uri)
    if parsed.is_s3:
        local = _resolve_input_dir(dataset_uri, work_dir, resume=resume, cache_root=cache_root)
        local.mkdir(parents=True, exist_ok=True)
        store = create_lake_store(dataset_uri)
        exclude = _materialize_exclude_prefixes(dataset_uri)
        if exclude:
            LOG.info(
                "materialize_input: lerobot v2 layout detected, skipping prefixes=%s",
                exclude,
            )
        store.download_dir(
            dataset_uri,
            local,
            exclude_prefixes=exclude,
            progress_log_every=50,
        )
        return local
    # v1.7：本地 file:// / 裸路径直接作为 input root，**不复制**整目录到 tmp。
    # 关键日志：v1.7 验收硬性要求 normalize 在本地路径下打出明确字样。
    resolved = Path(parsed.local_path).expanduser().resolve()
    LOG.info(
        "materialize_input: using local direct input, no download (dataset_uri=%s root=%s)",
        dataset_uri, resolved,
    )
    return resolved


def _materialize_exclude_prefixes(dataset_uri: str) -> tuple[str, ...] | None:
    """lerobot v2 layout（``meta/info.json``）下默认跳过 ``videos/``。

    可被 ``ROBOT_DH_NORMALIZE_EXCLUDE_PREFIXES`` 覆盖（逗号分隔；空字串关闭）。
    """
    raw = os.environ.get("ROBOT_DH_NORMALIZE_EXCLUDE_PREFIXES")
    if raw is not None:
        items = tuple(s.strip() for s in raw.split(",") if s.strip())
        return items or None
    try:
        from robot_dh.qc.lerobot_v2 import detect_lerobot_v2

        if is_s3_uri(dataset_uri) and detect_lerobot_v2(dataset_uri):
            return ("videos/",)
    except Exception as err:  # noqa: BLE001
        LOG.debug("materialize_input: lerobot v2 sniff failed: %s", err)
    return None


def _resolve_input_dir(
    dataset_uri: str,
    work_dir: Path,
    *,
    resume: bool,
    cache_root: Path | None,
) -> Path:
    if not resume:
        return work_dir / "input"
    root = cache_root
    if root is None:
        env_root = os.environ.get("ROBOT_DH_INPUT_CACHE_DIR")
        root = Path(env_root) if env_root else Path("/tmp/robot-dh/input-cache")
    digest = hashlib.sha256(dataset_uri.encode("utf-8")).hexdigest()[:16]
    return root.expanduser() / digest


def _load_raw_bundles(
    input_dir: Path,
    *,
    dataset_id: str | None,
    version: str | None,
) -> list[DatasetBundle]:
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


def _all_outputs_present(store: LakeStore, output_uri: str) -> bool:
    """检查 output_uri 下 pose/video/episode parquet 是否齐全。"""
    return all(store.exists(join_uri(output_uri, name)) for name in EXPECTED_OUTPUT_FILES)


def _load_existing_metrics(store: LakeStore, output_uri: str) -> tuple[int, float] | None:
    """从 output_uri/pose.parquet + episode_meta.parquet 推断 num_samples / duration_sec。"""
    try:
        with tempfile.TemporaryDirectory(prefix="robot-dh-resume-") as tmp_str:
            tmp = Path(tmp_str)
            store.download_dir(output_uri, tmp)
            pose = tmp / "pose.parquet"
            ep = tmp / "episode_meta.parquet"
            if not pose.is_file() or not ep.is_file():
                return None
            num = int(pq.ParquetFile(pose).metadata.num_rows)
            ep_df = pq.read_table(ep).to_pandas()
            duration = float(ep_df["duration_sec"].sum()) if "duration_sec" in ep_df.columns else 0.0
            return num, duration
    except Exception as err:
        LOG.warning("resume: failed to inspect existing outputs: %s", err)
        return None


def _try_resume(
    *,
    store: LakeStore,
    output_uri: str,
    dataset_uri: str,
    dataset_id: str | None,
    version: str | None,
    job_id: str,
    started: float,
    started_iso: str,
    warehouse: WarehouseService,
    lake_root_uri: str | None,
    checkpoint_store: CheckpointStore,
    existing_ckpt: Checkpoint | None,
) -> NormalizeResult | None:
    """如果远端/本地输出齐全且 checkpoint 满意，跳过重算重跑，仅补写 manifest。"""
    if not _all_outputs_present(store, output_uri):
        return None
    metrics = _load_existing_metrics(store, output_uri)
    if metrics is None:
        return None
    num_samples, duration_sec = metrics

    files = collect_outputs_to_files(store, output_uri)
    elapsed = time.time() - started
    finished_iso = utcnow_iso()

    ds_id = dataset_id or (existing_ckpt.dataset_id if existing_ckpt else "unknown")
    ver = version or (existing_ckpt.version if existing_ckpt else "v1")

    builder = ManifestBuilder(
        dataset_id=ds_id,
        version=ver,
        layer="ods",
        output_uri=output_uri,
        source_uris=[dataset_uri],
        files=files,
        metrics={
            "num_samples": num_samples,
            "num_episodes": 1,
            "duration_sec": duration_sec,
            "resumed": True,
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
    manifest_uri = write_manifest(store, builder)

    ckpt = existing_ckpt or Checkpoint(
        dataset_id=ds_id, version=ver, phase="normalize",
        source_uri=dataset_uri, output_uri=output_uri,
    )
    for step in (
        STEP_MATERIALIZE_INPUT,
        STEP_LOAD_BUNDLES,
        STEP_WRITE_POSE_PARQUET,
        STEP_WRITE_VIDEO_META,
        STEP_WRITE_EPISODE_META,
        STEP_UPLOAD_OUTPUTS,
        STEP_WRITE_MANIFEST,
    ):
        ckpt.mark_step(step)
    ckpt.status = "OK"
    checkpoint_store.save(ckpt)

    if lake_root_uri:
        try:
            lineage_store = create_lake_store(lake_root_uri)
            write_lineage_events(
                lineage_store,
                lake_root_uri,
                [
                    LineageEvent(
                        job_id=job_id,
                        job_type="normalize",
                        source_uri=dataset_uri,
                        target_uri=info["uri"],
                    )
                    for info in files
                ],
            )
        except Exception as err:
            LOG.warning("resume: lineage write failed: %s", err)

    warehouse.record_etl_job_finish(
        job_id=job_id,
        status="OK",
        metrics={
            "rows_in": num_samples,
            "rows_out": num_samples,
            "bytes_out": int(sum(info["size_bytes"] for info in files)),
            "duration_ms": int(elapsed * 1000),
            "resumed": True,
        },
    )

    return NormalizeResult(
        dataset_id=ds_id,
        version=ver,
        episode_id=ds_id,
        output_uri=output_uri,
        manifest_uri=manifest_uri,
        pose_uri=join_uri(output_uri, "pose.parquet"),
        video_meta_uri=join_uri(output_uri, "video_meta.parquet"),
        episode_meta_uri=join_uri(output_uri, "episode_meta.parquet"),
        num_samples=num_samples,
        duration_sec=duration_sec,
        source_uris=[dataset_uri],
        job_id=job_id,
        duration_job_sec=elapsed,
        files=files,
        status="RESUMED",
        completed_steps=list(ckpt.completed_steps),
    )


def collect_outputs_to_files(store: LakeStore, output_uri: str) -> list[dict[str, Any]]:
    """已存在的 output 文件 -> manifest files 列表（只用基本属性，避免重新计算 sha256）。"""
    out: list[dict[str, Any]] = []
    for name in EXPECTED_OUTPUT_FILES:
        uri = join_uri(output_uri, name)
        # 本地直接 stat；S3 用 head_object 也能取 size，但保守只填 0 + 不计 sha256。
        size_bytes = 0
        row_count: int | None = None
        if not is_s3_uri(uri):
            local = Path(parse_uri(uri).local_path)
            if local.is_file():
                size_bytes = int(local.stat().st_size)
                if name.endswith(".parquet"):
                    try:
                        row_count = int(pq.ParquetFile(str(local)).metadata.num_rows)
                    except Exception:
                        row_count = None
        out.append(
            {
                "path": name,
                "uri": uri,
                "format": name.rsplit(".", 1)[-1] if "." in name else "raw",
                "size_bytes": size_bytes,
                "row_count": row_count,
                "checksum_sha256": None,
            }
        )
    return out


def _emit_normalize_perf(prof: EtlProfiler, perf_dir: Path | None) -> Path | None:
    """v1.6 normalize 子阶段 perf 只落本地 JSON；不再直接写 PG。

    v1.8 修复：之前用 ``emit_perf_records`` 既写 JSON 又写 PG，加上调用点位于
    ``with EtlProfiler`` 块**内部**，导致写库时 ``prof.record.status`` 还停在
    初始的 ``"RUNNING"``、``finished_at=""``、``duration_sec=0``——一条永远不会
    收尾的孤儿。同一次 normalize 又被 cli 上层 ``perf_records_from_etl_run``
    写一条 OK 终态，PG 里出现 RUNNING + OK 双行，``etl_success_rate`` 被错算成
    50%/60%。这里只走 ``write_perf_json``（纯 JSON），让 PG 的单点写库由 cli
    上层 ``emit_perf_records`` 负责，且调用时机已经在 ``EtlProfiler.__exit__``
    之后，status / finished_at / duration_sec 都正确。
    """
    if perf_dir is None:
        return None
    perf_dir = perf_dir.expanduser().resolve()
    perf_dir.mkdir(parents=True, exist_ok=True)
    return write_perf_json(prof.record, perf_dir)


def normalize_dataset(
    *,
    dataset_uri: str,
    output_uri: str,
    dataset_id: str | None = None,
    version: str | None = None,
    job_id: str | None = None,
    warehouse: WarehouseService | None = None,
    lake_root_uri: str | None = None,
    resume: bool = True,
    force: bool = False,
    heartbeat_interval_sec: float = 30.0,
    progress_log_interval_sec: float = 30.0,
    workflow_name: str | None = None,
    step_name: str | None = None,
    perf_dir: Path | None = None,
    warehouse_v16: Any | None = None,
) -> NormalizeResult:
    """执行 raw -> ods 规范化（v1.6 增强：checkpoint / resume / heartbeat / sub-stage perf）。"""
    if warehouse is None:
        warehouse = WarehouseService(soft=True)

    job_id = job_id or f"normalize-{uuid.uuid4().hex[:12]}"
    started = time.time()
    started_iso = utcnow_iso()
    LOG.info(
        "normalize: job_id=%s dataset_uri=%s output_uri=%s resume=%s force=%s",
        job_id, dataset_uri, output_uri, resume, force,
    )

    out_store = create_lake_store(output_uri)
    checkpoint_store = CheckpointStore(output_uri=output_uri, store=out_store)

    # 1) manifest 已存在 + 不强制 -> SKIP（最早的退出路径，不写任何东西）
    manifest_uri_existing = join_uri(output_uri, MANIFEST_FILENAME)
    if not force and out_store.exists(manifest_uri_existing):
        LOG.info("normalize SKIP: %s already has manifest; pass force=True to rerun", output_uri)
        try:
            existing_manifest = read_manifest(out_store, output_uri)
        except Exception as err:
            LOG.warning("normalize: existing manifest unreadable, falling back to RESUMED: %s", err)
            existing_manifest = None
        if existing_manifest is not None:
            metrics = existing_manifest.get("metrics") or {}
            files = existing_manifest.get("files") or []
            ds_id = existing_manifest.get("dataset_id") or (dataset_id or "unknown")
            ver = existing_manifest.get("version") or (version or "v1")
            return NormalizeResult(
                dataset_id=ds_id,
                version=ver,
                episode_id=ds_id,
                output_uri=output_uri,
                manifest_uri=manifest_uri_existing,
                pose_uri=join_uri(output_uri, "pose.parquet"),
                video_meta_uri=join_uri(output_uri, "video_meta.parquet"),
                episode_meta_uri=join_uri(output_uri, "episode_meta.parquet"),
                num_samples=int(metrics.get("num_samples") or 0),
                duration_sec=float(metrics.get("duration_sec") or 0.0),
                source_uris=list(existing_manifest.get("source_uris") or [dataset_uri]),
                job_id=job_id,
                duration_job_sec=0.0,
                files=files,
                status="SKIPPED",
                completed_steps=[
                    STEP_MATERIALIZE_INPUT, STEP_LOAD_BUNDLES,
                    STEP_WRITE_POSE_PARQUET, STEP_WRITE_VIDEO_META, STEP_WRITE_EPISODE_META,
                    STEP_UPLOAD_OUTPUTS, STEP_WRITE_MANIFEST,
                ],
            )

    # 2) force：清掉旧 manifest + checkpoint（让本次跑成 fresh run）
    existing_ckpt: Checkpoint | None = None
    if resume and not force:
        existing_ckpt = checkpoint_store.load()

    warehouse.record_etl_job_start(
        job_id=job_id,
        job_type="normalize",
        input_uri=dataset_uri,
        output_uri=output_uri,
    )

    # 3) 尝试 partial resume：output 文件齐全，跳过下载/转换/上传，只补写 manifest。
    if resume and not force:
        attempted = _try_resume(
            store=out_store,
            output_uri=output_uri,
            dataset_uri=dataset_uri,
            dataset_id=dataset_id,
            version=version,
            job_id=job_id,
            started=started,
            started_iso=started_iso,
            warehouse=warehouse,
            lake_root_uri=lake_root_uri,
            checkpoint_store=checkpoint_store,
            existing_ckpt=existing_ckpt,
        )
        if attempted is not None:
            LOG.info("normalize RESUMED via existing outputs: %s", output_uri)
            return attempted

    # 4) fresh run（带 checkpoint + heartbeat + sub-stage profiler）
    ds_for_hb = dataset_id or "unknown"
    ver_for_hb = version or "v1"
    try:
        with EtlProfiler(
            job_id=job_id,
            dataset_id=ds_for_hb,
            version=ver_for_hb,
            phase="normalize",
            input_uri=dataset_uri,
            output_uri=output_uri,
        ) as prof, HeartbeatReporter(
            task_id=job_id,
            workflow_name=workflow_name,
            step_name=step_name,
            dataset_id=ds_for_hb,
            version=ver_for_hb,
            phase="normalize",
            interval_sec=heartbeat_interval_sec,
            warehouse_v16=warehouse_v16,
        ) as hb:
            with tempfile.TemporaryDirectory(prefix="robot-dh-normalize-") as tmp_str:
                tmp = Path(tmp_str)

                ckpt = Checkpoint(
                    dataset_id=ds_for_hb, version=ver_for_hb, phase="normalize",
                    source_uri=dataset_uri, output_uri=output_uri, status="RUNNING",
                )
                checkpoint_store.save(ckpt)

                # ---- materialize_input ----
                hb.update_phase("normalize.materialize_input")
                hb.emit(message="materialize_input.start")
                t0 = time.time()
                with prof.measure_download():
                    input_dir = _materialize_input(
                        dataset_uri,
                        tmp,
                        resume=(resume and not force),
                    )
                prof.add_metric("materialize_input_duration_sec", time.time() - t0)
                hb.emit(
                    message="materialize_input.input_dir",
                    metrics={"input_dir": str(input_dir)},
                )
                ckpt.mark_step(STEP_MATERIALIZE_INPUT)
                checkpoint_store.save(ckpt)
                hb.emit(message="materialize_input.done", metrics={"input_dir": str(input_dir)})

                # ---- load_bundles ----
                hb.update_phase("normalize.load_bundles")
                hb.emit(message="load_bundles.start")
                pl = ProgressLogger(
                    name="normalize.load_bundles",
                    total=None,
                    interval_sec=progress_log_interval_sec,
                )
                t0 = time.time()
                bundles = _load_raw_bundles(input_dir, dataset_id=dataset_id, version=version)
                prof.add_metric("load_bundles_duration_sec", time.time() - t0)
                pl.done(current=len(bundles))
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
                prof.add_metric("bundles_loaded", len(bundles))
                prof.add_metric("episodes_loaded", len(identity_bundles))
                ckpt.mark_step(STEP_LOAD_BUNDLES)
                ckpt.dataset_id = ds_id
                ckpt.version = ver
                ckpt.metrics["num_samples"] = total_samples
                checkpoint_store.save(ckpt)
                hb.emit(
                    message="load_bundles.done",
                    metrics={"bundles": len(bundles), "samples": total_samples},
                )

                staging = tmp / "ods"
                staging.mkdir(parents=True, exist_ok=True)

                # ---- build_pose_table + write parquet ----
                hb.update_phase("normalize.build_pose_table")
                hb.emit(message="build_pose_table.start")
                t0 = time.time()
                pose_table = _build_pose_table_many(identity_bundles)
                prof.add_metric("build_pose_table_duration_sec", time.time() - t0)
                t0 = time.time()
                pq.write_table(pose_table, staging / "pose.parquet")
                ckpt.mark_step(STEP_WRITE_POSE_PARQUET)
                ckpt.upsert_file(CheckpointFile(name="pose.parquet", status="STAGED"))
                checkpoint_store.save(ckpt)
                hb.emit(message="write_pose_parquet.done", metrics={"rows": int(pose_table.num_rows)})

                # ---- video_meta ----
                hb.update_phase("normalize.write_video_meta")
                video_table = _build_video_meta_table_many(identity_bundles, dataset_uri)
                pq.write_table(video_table, staging / "video_meta.parquet")
                ckpt.mark_step(STEP_WRITE_VIDEO_META)
                ckpt.upsert_file(CheckpointFile(name="video_meta.parquet", status="STAGED"))
                checkpoint_store.save(ckpt)
                hb.emit(message="write_video_meta.done")

                # ---- episode_meta ----
                hb.update_phase("normalize.write_episode_meta")
                episode_table = _build_episode_meta_table_many(identity_bundles, dataset_uri)
                pq.write_table(episode_table, staging / "episode_meta.parquet")
                ckpt.mark_step(STEP_WRITE_EPISODE_META)
                ckpt.upsert_file(CheckpointFile(name="episode_meta.parquet", status="STAGED"))
                checkpoint_store.save(ckpt)
                prof.add_metric("write_parquet_duration_sec", time.time() - t0)
                prof.add_metric("parquet_rows_written", int(pose_table.num_rows))
                hb.emit(message="write_episode_meta.done")

                # ---- upload_outputs ----
                hb.update_phase("normalize.upload_outputs")
                hb.emit(message="upload_outputs.start")
                t0 = time.time()
                with prof.measure_upload():
                    uploaded = out_store.upload_dir(staging, output_uri)
                prof.add_metric("upload_duration_sec", time.time() - t0)
                files = collect_file_stats(
                    staging,
                    output_uri,
                    files=list(EXPECTED_OUTPUT_FILES),
                )
                ckpt.mark_step(STEP_UPLOAD_OUTPUTS)
                for info in files:
                    ckpt.upsert_file(CheckpointFile(
                        name=info["path"], status="UPLOADED", uri=info["uri"],
                        row_count=info.get("row_count"), size_bytes=info.get("size_bytes"),
                        checksum_sha256=info.get("checksum_sha256"),
                    ))
                checkpoint_store.save(ckpt)
                hb.emit(message="upload_outputs.done", metrics={"file_count": len(files)})

                bytes_out = int(sum(info["size_bytes"] for info in files))
                prof.set_io(
                    input_rows=total_samples,
                    output_rows=int(pose_table.num_rows),
                    output_bytes=bytes_out,
                )
                prof.add_metric("s3_upload_bytes", bytes_out)

                # ---- warehouse rows ----
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

                # ---- write_manifest ----
                hb.update_phase("normalize.write_manifest")
                hb.emit(message="write_manifest.start")
                t0 = time.time()
                elapsed = time.time() - started
                finished_iso = utcnow_iso()
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
                prof.add_metric("manifest_duration_sec", time.time() - t0)
                ckpt.mark_step(STEP_WRITE_MANIFEST)
                ckpt.status = "OK"
                checkpoint_store.save(ckpt)
                hb.emit(message="write_manifest.done")

                metrics = {
                    "rows_in": total_samples,
                    "rows_out": int(pose_table.num_rows),
                    "bytes_out": bytes_out,
                    "duration_ms": int(elapsed * 1000),
                }
                warehouse.record_etl_job_finish(job_id=job_id, status="OK", metrics=metrics)

                # v1.8 修复：先把 result 攒着，**不要**在 with 块内 emit perf 到 PG。
                # EtlProfiler.__exit__ 还没跑，status/finished_at/duration 都是初始值。
                # 这里只快照 sub-stage metrics（s3_upload_bytes / manifest_duration_sec 等），
                # PG 的写入由 cli 上层 perf_records_from_etl_run 在 normalize 真正
                # 收尾后统一处理。
                result = NormalizeResult(
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
                    status="OK",
                    completed_steps=list(ckpt.completed_steps),
                    metrics=dict(prof.record.metrics),
                )
        # ↑ 走到这里 tempdir + heartbeat + EtlProfiler 都已经 __exit__：
        # prof.record.status == "OK"、finished_at 已设、duration_sec 已算。
        # 这时落 normalize_perf.json 才是"成品"状态。
        _emit_normalize_perf(prof, perf_dir)
        return result
    except Exception as err:
        warehouse.record_etl_job_finish(job_id=job_id, status="FAIL", error_message=str(err))
        raise
