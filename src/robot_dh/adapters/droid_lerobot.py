"""DroidLeRobotAdapter：识别 LeRobot v2 layout（DROID / lerobot/* 衍生集）。

layout 标志：
  - ``meta/info.json``               必需
  - ``data/chunk-*/file-*.parquet``  parquet shard
  - ``videos/<camera>/chunk-*/*.mp4``  视频；adapter 默认不读

probe：
  - 本地路径走 ``robot_dh.qc.parquet_probe.probe_parquet`` 抽样前 N 个 parquet
    + 读 ``meta/info.json`` 拿 fps / total_episodes / total_frames；
  - s3:// 走 ``robot_dh.qc.lerobot_v2.profile_lerobot_v2`` 已经做过的 lazy 路径
    （v1.6.6 落地，整 parquet/视频都不下载）。

normalize_options：
  - ``skip_videos=True``
  - ``include_prefixes=("data/", "meta/")``
  让 ``etl.normalize._materialize_input`` 在 s3:// 路径下也不再拉 ~10 GiB videos。
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from robot_dh.adapters.base import (
    DetectionResult,
    EpisodeRef,
    ProbeResult,
    RobotDatasetAdapter,
)
from robot_dh.lake.uri import is_local_uri, is_s3_uri, parse_uri


_REQUIRED_MARKERS = ("meta/info.json",)
_OPTIONAL_MARKERS = (
    "meta/episodes.jsonl",
    "meta/stats.json",
    "meta/tasks.jsonl",
)
_ID_PREFIXES = ("droid_lerobot", "lerobot/droid", "lerobot_droid", "droid")


class DroidLeRobotAdapter(RobotDatasetAdapter):
    family = "droid"

    def detect(
        self,
        dataset_uri: str,
        *,
        dataset_id: str | None = None,
        listings: list[str] | None = None,
    ) -> DetectionResult:
        rels = listings if listings is not None else self._list_relative_paths(dataset_uri)
        matched: list[str] = [m for m in _REQUIRED_MARKERS if m in rels]
        opts_matched: list[str] = [m for m in _OPTIONAL_MARKERS if m in rels]
        confidence = 0.0
        notes: list[str] = []
        prefix_hit: str | None = None
        if dataset_id:
            for pfx in _ID_PREFIXES:
                if dataset_id.lower().startswith(pfx):
                    prefix_hit = pfx
                    confidence += 0.4
                    break
        if matched:
            confidence += 0.6
            matched.extend(opts_matched)
        elif _has_lerobot_v2_marker_s3(dataset_uri):
            # s3:// 没拿到完整 listing 但 meta/info.json 存在 -> 接受
            matched.append("meta/info.json (s3 sniff)")
            confidence += 0.6
        else:
            notes.append("no meta/info.json found; falling back to id-prefix only")
        return DetectionResult(
            family=self.family,
            confidence=min(1.0, confidence),
            matched_markers=matched,
            matched_id_prefix=prefix_hit,
            notes=notes,
        )

    def probe(
        self,
        dataset_uri: str,
        *,
        sample_limit: int = 32,
        options: dict[str, Any] | None = None,
    ) -> ProbeResult:
        opts = dict(options or {})
        started = time.time()
        if is_s3_uri(dataset_uri):
            return self._probe_s3(dataset_uri, sample_limit=sample_limit, options=opts, started=started)
        return self._probe_local(dataset_uri, sample_limit=sample_limit, options=opts, started=started)

    def normalize_options(self, dataset_uri: str) -> dict[str, Any]:
        return {
            "skip_videos": True,
            "include_prefixes": ("data/", "meta/"),
            "video_metadata_only": True,
        }

    def list_episodes(
        self,
        dataset_uri: str,
        *,
        limit: int | None = None,
    ) -> list[EpisodeRef]:
        rels = self._list_relative_paths(dataset_uri)
        items: list[EpisodeRef] = []
        for rel in rels:
            if not rel.endswith(".parquet"):
                continue
            if "data/" not in rel and "data\\" not in rel:
                continue
            if limit is not None and len(items) >= limit:
                break
            file_uri = self._join_rel(dataset_uri, rel)
            items.append(
                EpisodeRef(
                    episode_id=f"droid-{Path(rel).stem}",
                    file_uri=file_uri,
                    rel_path=rel,
                    extra={"layout": "lerobot_v2"},
                )
            )
        return items

    # ---------------------------- helpers ----------------------------
    def _join_rel(self, base: str, rel: str) -> str:
        from robot_dh.adapters.base import _join_uri

        return _join_uri(base, rel)

    def _probe_local(
        self,
        dataset_uri: str,
        *,
        sample_limit: int,
        options: dict[str, Any],
        started: float,
    ) -> ProbeResult:
        root = Path(parse_uri(dataset_uri).local_path)
        if not root.exists():
            return ProbeResult(
                family=self.family,
                dataset_uri=dataset_uri,
                status="FAIL",
                errors=[{"error_type": "FileNotFoundError", "msg": str(root)}],
                duration_sec=time.time() - started,
                options=options,
            )

        meta_info = root / "meta" / "info.json"
        info_payload: dict[str, Any] = {}
        if meta_info.exists():
            try:
                info_payload = json.loads(meta_info.read_text(encoding="utf-8")) or {}
            except (json.JSONDecodeError, OSError) as err:
                info_payload = {"_error": f"{type(err).__name__}: {err}"}

        parquet_files = sorted(root.rglob("*.parquet"))
        video_files = sorted(root.rglob("*.mp4"))
        bytes_total = 0
        for f in root.rglob("*"):
            if f.is_file():
                try:
                    bytes_total += f.stat().st_size
                except OSError:
                    continue

        # 抽样 parquet schema：只读 footer，不读 row。
        from robot_dh.qc.parquet_probe import probe_parquet

        sample = parquet_files[:sample_limit]
        sampled_columns: list[str] = []
        errors: list[dict[str, Any]] = []
        for p in sample:
            res = probe_parquet(p)
            if not res.get("readable"):
                errors.append({"file": p.as_posix(), **{k: v for k, v in res.items() if k.startswith("error") or k.startswith("cause")}})
                continue
            sampled_columns.extend(res.get("schema_columns") or [])
        schema_summary = {
            "info_json": info_payload,
            "sampled_parquet": len(sample),
            "schema_columns_sample": sorted(set(sampled_columns))[:64],
        }
        status = "OK" if not errors else "WARN"
        return ProbeResult(
            family=self.family,
            dataset_uri=dataset_uri,
            status=status,
            files_count=sum(1 for _ in root.rglob("*") if _.is_file()),
            bytes_total=bytes_total,
            parquet_files=len(parquet_files),
            video_files=len(video_files),
            episodes_count=int(info_payload.get("total_episodes") or 0),
            schema_summary=schema_summary,
            errors=errors,
            duration_sec=time.time() - started,
            options=options,
        )

    def _probe_s3(
        self,
        dataset_uri: str,
        *,
        sample_limit: int,
        options: dict[str, Any],
        started: float,
    ) -> ProbeResult:
        # 走 v1.6.6 已有的 lazy 路径，不下载 video，不破坏既有契约。
        from robot_dh.qc.lerobot_v2 import detect_lerobot_v2, profile_lerobot_v2

        if not detect_lerobot_v2(dataset_uri):
            return ProbeResult(
                family=self.family,
                dataset_uri=dataset_uri,
                status="WARN",
                errors=[{"error_type": "NotLeRobotV2", "msg": "meta/info.json not found"}],
                duration_sec=time.time() - started,
                options=options,
            )
        profile = profile_lerobot_v2(dataset_uri=dataset_uri)
        files_overview = profile.profile.get("files_overview") or {}
        lerobot_v2 = profile.profile.get("lerobot_v2") or {}
        return ProbeResult(
            family=self.family,
            dataset_uri=dataset_uri,
            status=profile.status,
            files_count=int(profile.files_count),
            bytes_total=int(profile.bytes),
            parquet_files=int(files_overview.get("parquet") or 0),
            video_files=int(files_overview.get("video") or 0),
            episodes_count=int(lerobot_v2.get("episodes_count") or profile.episodes_count or 0),
            schema_summary={"lerobot_v2": lerobot_v2},
            errors=[],
            duration_sec=time.time() - started,
            options=options,
        )


def _has_lerobot_v2_marker_s3(dataset_uri: str) -> bool:
    if not is_s3_uri(dataset_uri):
        return False
    try:
        from robot_dh.qc.lerobot_v2 import detect_lerobot_v2

        return bool(detect_lerobot_v2(dataset_uri))
    except Exception:  # noqa: BLE001
        return False
