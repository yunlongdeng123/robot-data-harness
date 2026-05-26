"""BridgeDataAdapter：识别 BridgeData V2 parquet shard。

layout 标志：
  - ``data/*.parquet`` 至少一个；
  - 命名集 / 列名出现 ``episode_idx`` / ``step_idx`` / ``state.end_effector_pose``
    等 nested struct（v1.6.5 已经识别过）。

probe：
  - 本地：直接 ``pyarrow.parquet`` 读 footer + schema，按 ``episode_idx`` 抽样统计；
  - s3:// ：走 v1.6.7 ``probe_parquet_s3`` lazy 路径，但叠加一层硬 timeout
    （子线程 + ``concurrent.futures``），失败时输出 ``cause=REMOTE_PARQUET_TIMEOUT``。

参数：
  - ``probe_timeout_sec`` 默认 120
  - ``max_retries`` 默认 2
  - ``disable_remote_lazy`` 默认 False
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
from pathlib import Path
from typing import Any

from robot_dh.adapters.base import (
    DetectionResult,
    EpisodeRef,
    ProbeResult,
    RobotDatasetAdapter,
)
from robot_dh.lake.uri import is_local_uri, is_s3_uri, parse_uri

LOG = logging.getLogger(__name__)

_ID_PREFIXES = ("bridgedata_v2", "bridge_v2", "bridge")


class BridgeDataAdapter(RobotDatasetAdapter):
    family = "bridge"

    def detect(
        self,
        dataset_uri: str,
        *,
        dataset_id: str | None = None,
        listings: list[str] | None = None,
    ) -> DetectionResult:
        rels = listings if listings is not None else self._list_relative_paths(dataset_uri)
        matched: list[str] = [
            rel for rel in rels
            if rel.startswith("data/") and rel.endswith(".parquet")
        ][:3]
        confidence = 0.0
        prefix_hit: str | None = None
        if dataset_id:
            for pfx in _ID_PREFIXES:
                if dataset_id.lower().startswith(pfx):
                    prefix_hit = pfx
                    confidence += 0.4
                    break
        if matched:
            confidence += 0.6
        return DetectionResult(
            family=self.family,
            confidence=min(1.0, confidence),
            matched_markers=matched,
            matched_id_prefix=prefix_hit,
        )

    def normalize_options(self, dataset_uri: str) -> dict[str, Any]:
        return {
            "direct_parquet_read": is_local_uri(dataset_uri),
            "include_prefixes": ("data/", "meta/", "README"),
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
            if not (rel.startswith("data/") and rel.endswith(".parquet")):
                continue
            if limit is not None and len(items) >= limit:
                break
            file_uri = self._join_rel(dataset_uri, rel)
            items.append(EpisodeRef(
                episode_id=f"bridge-{Path(rel).stem}",
                file_uri=file_uri,
                rel_path=rel,
            ))
        return items

    def probe(
        self,
        dataset_uri: str,
        *,
        sample_limit: int = 32,
        options: dict[str, Any] | None = None,
    ) -> ProbeResult:
        opts = dict(options or {})
        probe_timeout_sec = float(opts.get("probe_timeout_sec", 120.0))
        max_retries = int(opts.get("max_retries", 2))
        disable_remote_lazy = bool(opts.get("disable_remote_lazy", False))
        started = time.time()

        if is_local_uri(dataset_uri):
            return self._probe_local(
                dataset_uri,
                sample_limit=sample_limit,
                options=opts,
                started=started,
            )
        return self._probe_s3(
            dataset_uri,
            sample_limit=sample_limit,
            probe_timeout_sec=probe_timeout_sec,
            max_retries=max_retries,
            disable_remote_lazy=disable_remote_lazy,
            options=opts,
            started=started,
        )

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
        from robot_dh.qc.parquet_probe import probe_parquet

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

        parquet_files = sorted((root / "data").rglob("*.parquet")) if (root / "data").exists() else sorted(root.rglob("*.parquet"))
        bytes_total = sum(f.stat().st_size for f in root.rglob("*") if f.is_file())
        sample = parquet_files[:sample_limit]
        per_file: list[dict[str, Any]] = []
        episodes_total = 0
        errors: list[dict[str, Any]] = []
        nested_columns: set[str] = set()
        for p in sample:
            res = probe_parquet(p)
            res["uri"] = p.as_posix()
            per_file.append(res)
            if not res.get("readable"):
                errors.append({"file": p.as_posix(), "error_type": res.get("error_type"), "cause_type": res.get("cause_type")})
                continue
            for c in res.get("nested_columns") or []:
                nested_columns.add(c)
            ep_lens = res.get("per_episode_lengths") or []
            episodes_total += len(ep_lens)

        status = "OK" if not errors else "WARN"
        return ProbeResult(
            family=self.family,
            dataset_uri=dataset_uri,
            status=status,
            files_count=sum(1 for _ in root.rglob("*") if _.is_file()),
            bytes_total=int(bytes_total),
            parquet_files=len(parquet_files),
            episodes_count=int(episodes_total),
            schema_summary={
                "sampled_parquet": len(sample),
                "nested_columns": sorted(nested_columns),
                "per_file": per_file,
            },
            errors=errors,
            duration_sec=time.time() - started,
            options=options,
        )

    def _probe_s3(
        self,
        dataset_uri: str,
        *,
        sample_limit: int,
        probe_timeout_sec: float,
        max_retries: int,
        disable_remote_lazy: bool,
        options: dict[str, Any],
        started: float,
    ) -> ProbeResult:
        # 远端 lazy probe 必须有硬 timeout。子线程 + future.result(timeout=...) 兜底；
        # 不是真的 cancel 任务（boto3 不响应中断），但能把控制流交回主线程。
        if disable_remote_lazy:
            return ProbeResult(
                family=self.family,
                dataset_uri=dataset_uri,
                status="FAIL",
                errors=[{
                    "error_type": "RemoteLazyDisabled",
                    "msg": "disable_remote_lazy=true; bridge probe only supports local file URI in this mode.",
                }],
                duration_sec=time.time() - started,
                options=options,
            )

        from robot_dh.qc.parquet_probe import probe_parquet_s3
        from robot_dh.qc.profile import _list_files

        try:
            files = _list_files(dataset_uri)
        except Exception as err:  # noqa: BLE001
            return ProbeResult(
                family=self.family,
                dataset_uri=dataset_uri,
                status="FAIL",
                errors=[{"error_type": type(err).__name__, "msg": str(err)}],
                duration_sec=time.time() - started,
                options=options,
            )
        parquet_files = [(u, s) for (u, s) in files if u.endswith(".parquet")]
        bytes_total = sum(s for _, s in files)

        sample = parquet_files[:sample_limit]
        per_file: list[dict[str, Any]] = []
        episodes_total = 0
        errors: list[dict[str, Any]] = []

        def _probe_with_timeout(uri: str) -> dict[str, Any]:
            attempt = 0
            last_err: dict[str, Any] | None = None
            while attempt <= max_retries:
                attempt += 1
                with ThreadPoolExecutor(max_workers=1) as ex:
                    fut = ex.submit(probe_parquet_s3, uri)
                    try:
                        res = fut.result(timeout=probe_timeout_sec)
                        res["uri"] = uri
                        res["attempts"] = attempt
                        return res
                    except FutureTimeout:
                        last_err = {
                            "uri": uri,
                            "readable": False,
                            "error_type": "TimeoutError",
                            "cause_type": "REMOTE_PARQUET_TIMEOUT",
                            "msg": f"probe exceeded {probe_timeout_sec:.0f}s after {attempt} attempt(s)",
                        }
                    except Exception as err:  # noqa: BLE001
                        last_err = {
                            "uri": uri,
                            "readable": False,
                            "error_type": type(err).__name__,
                            "msg": str(err),
                        }
            return last_err or {"uri": uri, "readable": False, "error_type": "UnknownError"}

        for uri, _size in sample:
            res = _probe_with_timeout(uri)
            per_file.append(res)
            if not res.get("readable"):
                errors.append({
                    "file": uri,
                    "error_type": res.get("error_type"),
                    "cause_type": res.get("cause_type"),
                })
                continue
            ep_lens = res.get("per_episode_lengths") or []
            episodes_total += len(ep_lens)

        status = "OK" if not errors else "WARN"
        return ProbeResult(
            family=self.family,
            dataset_uri=dataset_uri,
            status=status,
            files_count=len(files),
            bytes_total=int(bytes_total),
            parquet_files=len(parquet_files),
            episodes_count=int(episodes_total),
            schema_summary={"sampled_parquet": len(sample), "per_file": per_file},
            errors=errors,
            duration_sec=time.time() - started,
            options={**options, "probe_timeout_sec": probe_timeout_sec, "max_retries": max_retries},
        )
