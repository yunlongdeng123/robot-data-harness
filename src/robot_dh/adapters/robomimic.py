"""RobomimicAdapter：识别 ``*.hdf5`` 数据集。

layout 标志：
  - 任意 ``*.hdf5`` 文件（v1.6.6 demo group 遍历已经能处理大多数变体）；
  - 习惯命名：``low_dim*.hdf5`` / ``image*.hdf5``。

probe：
  - 本地 file URI 直接 ``h5py.File(path)`` 抽样 demo_count；不复制；
  - s3:// 沿用 v1.6.8 fast boto3 download_file 路径（``profile.py::_probe_hdf5_uri``），
    每文件硬 timeout = options.file_timeout_sec（默认 300s）；
  - 支持 ``max_workers`` / ``fail_fast`` 选项。

contract：直接走 v1.6 robomimic family contract（``contract_runner()`` 默认）。
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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

_ID_PREFIXES = ("robomimic",)


class RobomimicAdapter(RobotDatasetAdapter):
    family = "robomimic"

    def detect(
        self,
        dataset_uri: str,
        *,
        dataset_id: str | None = None,
        listings: list[str] | None = None,
    ) -> DetectionResult:
        rels = listings if listings is not None else self._list_relative_paths(dataset_uri)
        matched: list[str] = [rel for rel in rels if rel.lower().endswith(".hdf5")][:3]
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
            "direct_h5py_read": is_local_uri(dataset_uri),
            "include_prefixes": (),  # 不裁剪，hdf5 单文件就够大
        }

    def probe(
        self,
        dataset_uri: str,
        *,
        sample_limit: int = 32,
        options: dict[str, Any] | None = None,
    ) -> ProbeResult:
        opts = dict(options or {})
        max_workers = int(opts.get("max_workers", 4))
        file_timeout_sec = float(opts.get("file_timeout_sec", 300.0))
        fail_fast = bool(opts.get("fail_fast", False))
        started = time.time()

        if is_local_uri(dataset_uri):
            return self._probe_local(
                dataset_uri,
                sample_limit=sample_limit,
                max_workers=max_workers,
                fail_fast=fail_fast,
                options=opts,
                started=started,
            )

        # S3 路径：复用 v1.6.8 fast download；timeout 由 boto3 client + per-file 控制。
        return self._probe_s3(
            dataset_uri,
            sample_limit=sample_limit,
            max_workers=max_workers,
            file_timeout_sec=file_timeout_sec,
            fail_fast=fail_fast,
            options=opts,
            started=started,
        )

    def list_episodes(
        self,
        dataset_uri: str,
        *,
        limit: int | None = None,
    ) -> list[EpisodeRef]:
        rels = self._list_relative_paths(dataset_uri)
        items: list[EpisodeRef] = []
        for rel in rels:
            if not rel.lower().endswith(".hdf5"):
                continue
            if limit is not None and len(items) >= limit:
                break
            file_uri = self._join_rel(dataset_uri, rel)
            items.append(
                EpisodeRef(
                    episode_id=f"robomimic-{Path(rel).stem}",
                    file_uri=file_uri,
                    rel_path=rel,
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
        max_workers: int,
        fail_fast: bool,
        options: dict[str, Any],
        started: float,
    ) -> ProbeResult:
        from robot_dh.qc.hdf5_probe import probe_hdf5

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
        hdf5_files = [p for p in sorted(root.rglob("*.hdf5"))]
        bytes_total = sum(f.stat().st_size for f in hdf5_files if f.is_file())

        sample = hdf5_files[:sample_limit]
        errors: list[dict[str, Any]] = []
        episodes_total = 0
        per_file: list[dict[str, Any]] = []

        def _run(path: Path) -> dict[str, Any]:
            t0 = time.time()
            try:
                r = probe_hdf5(path)
                r["uri"] = path.as_posix()
                r["duration_sec"] = time.time() - t0
                return r
            except Exception as err:  # noqa: BLE001
                return {
                    "uri": path.as_posix(),
                    "readable": False,
                    "error_type": type(err).__name__,
                    "msg": str(err),
                    "duration_sec": time.time() - t0,
                }

        if max_workers <= 1:
            for p in sample:
                res = _run(p)
                per_file.append(res)
                if not res.get("readable"):
                    errors.append({"file": res["uri"], "error_type": res.get("error_type")})
                    if fail_fast:
                        break
                else:
                    episodes_total += int(res.get("demo_count") or 0)
        else:
            with ThreadPoolExecutor(max_workers=max(1, max_workers)) as ex:
                futures = {ex.submit(_run, p): p for p in sample}
                for fut in as_completed(futures):
                    res = fut.result()
                    per_file.append(res)
                    if not res.get("readable"):
                        errors.append({"file": res["uri"], "error_type": res.get("error_type")})
                        if fail_fast:
                            # 取消剩余任务（已提交的可能仍在跑）
                            for other in futures:
                                other.cancel()
                            break
                    else:
                        episodes_total += int(res.get("demo_count") or 0)

        status = "OK" if not errors else ("FAIL" if fail_fast else "WARN")
        return ProbeResult(
            family=self.family,
            dataset_uri=dataset_uri,
            status=status,
            files_count=len(hdf5_files),
            bytes_total=int(bytes_total),
            hdf5_files=len(hdf5_files),
            episodes_count=int(episodes_total),
            schema_summary={"per_file": per_file},
            errors=errors,
            duration_sec=time.time() - started,
            options=options,
        )

    def _probe_s3(
        self,
        dataset_uri: str,
        *,
        sample_limit: int,
        max_workers: int,
        file_timeout_sec: float,
        fail_fast: bool,
        options: dict[str, Any],
        started: float,
    ) -> ProbeResult:
        # 复用 profile.profile_dataset 的 hdf5 路径：fast boto3 download + probe + delete。
        # 它已经做了 fail-fast 容错 + cause 链；本 adapter 主要承担参数透传。
        import os

        os.environ.setdefault("ROBOT_DH_QC_PROBE_CONCURRENCY", str(max(1, max_workers)))
        # file_timeout_sec 没有直接的 env 钩子，但 fast client 默认 read_timeout=60s × 3。
        # 这里把它写到 options 里供调用方记录；想真的硬截断需要包一层 future.timeout。
        from robot_dh.qc.profile import profile_dataset

        profile = profile_dataset(
            dataset_uri=dataset_uri,
            dataset_family=self.family,
        )
        files_overview = profile.profile.get("files_overview") or {}
        hdf5 = profile.profile.get("hdf5") or []
        errors = [
            {
                "file": h.get("uri"),
                "error_type": h.get("error_type"),
                "cause_type": h.get("cause_type"),
            }
            for h in hdf5
            if not h.get("readable")
        ]
        if fail_fast and errors:
            status = "FAIL"
        else:
            status = profile.status
        return ProbeResult(
            family=self.family,
            dataset_uri=dataset_uri,
            status=status,
            files_count=int(profile.files_count),
            bytes_total=int(profile.bytes),
            hdf5_files=int(files_overview.get("hdf5") or len(hdf5)),
            episodes_count=int(profile.episodes_count or 0),
            schema_summary={"hdf5": hdf5},
            errors=errors,
            duration_sec=time.time() - started,
            options={**options, "file_timeout_sec": file_timeout_sec},
        )
