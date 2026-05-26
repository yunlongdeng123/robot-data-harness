"""verify_local_datasets：对照 devscale_plan.json + 各 dataset 的 _manifest.json
比对本地实际文件是否齐全 + size。

与 ``scripts/local_verify_devscale.sh`` 是同一职责，区别在于：
- 脚本版面向运维，写完整 verify report 落 ``manifests/devscale_verify_report.*``；
- 本模块面向 Python / CLI，返回结构化数据；不强制要求 plan 存在
  （若 plan 缺失，按 manifest 自检）。
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from robot_dh.local_runtime.devscale import DevscaleDataset, DevscaleRegistry


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass(slots=True)
class DatasetVerifyReport:
    generated_at: str
    datasets: list[dict[str, Any]] = field(default_factory=list)
    totals: dict[str, int] = field(default_factory=dict)
    status: str = "ok"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _resolve_dst(
    record: dict[str, Any],
    ds: DevscaleDataset,
) -> tuple[Path, str]:
    """把 plan/manifest 里记录的 ``dst_path`` 映射到当前 runtime 下的真实路径。

    plan/manifest 由 WSL 主机端的 sync 工具生成，里面 ``dst_path`` 经常是
    主机绝对路径（如 ``/mnt/d/robot-dh-local/raw/<id>/v1/...``）；但 verify
    可能跑在容器里，PVC 挂载点是 ``ds.target_local_path``。本函数：

    1. ``rel_key`` 存在 → 直接 ``target_local_path / rel_key``；
    2. 否则从 ``dst_path`` 中找 ``<dataset_id>/<version>`` 锚点，截后半段
       作为相对路径再拼；
    3. 兜底返回原 ``dst_path``（极端 case 下也至少能跑），rel 用 basename。

    返回 ``(容器视角 dst_path, rel_key 字符串)``。
    """
    rel = record.get("rel_key") or ""
    raw_dst = record.get("dst_path") or record.get("path") or ""
    raw_path = Path(raw_dst) if raw_dst else None

    if not rel and raw_path is not None:
        parts = raw_path.parts
        for i in range(len(parts) - 1):
            if parts[i] == ds.dataset_id and parts[i + 1] == ds.version:
                rel_parts = parts[i + 2:]
                if rel_parts:
                    rel = "/".join(rel_parts)
                break

    if rel:
        return Path(ds.target_local_path) / rel, rel
    if raw_path is not None:
        return raw_path, raw_path.name
    return Path(""), ""


def _verify_one(
    ds: DevscaleDataset,
    *,
    plan_files: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    target = Path(ds.target_local_path)
    manifest_path = Path(ds.manifest_path)
    manifest_present = manifest_path.exists()

    file_records: list[dict[str, Any]] = []
    if plan_files is not None:
        file_records = list(plan_files)
    elif manifest_present:
        try:
            m = json.loads(manifest_path.read_text(encoding="utf-8"))
            file_records = list(m.get("files") or [])
        except (json.JSONDecodeError, OSError):
            file_records = []

    missing: list[str] = []
    wrong_size: list[dict[str, Any]] = []
    present_count = 0
    present_bytes = 0

    if file_records:
        for f in file_records:
            dst_path, rel = _resolve_dst(f, ds)
            try:
                expected = int(f.get("size_bytes", 0))
            except (TypeError, ValueError):
                expected = 0
            if not dst_path.exists():
                missing.append(rel)
                continue
            try:
                actual = dst_path.stat().st_size
            except OSError:
                missing.append(rel)
                continue
            if expected and actual != expected:
                wrong_size.append({"rel_key": rel, "expected": expected, "actual": actual})
                continue
            present_count += 1
            present_bytes += actual
    else:
        # 没 plan 也没 manifest：跑目录扫描，给出参考数据但不算 fail。
        for f in target.rglob("*"):
            if f.is_file() and f.name != "_manifest.json":
                try:
                    present_bytes += f.stat().st_size
                except OSError:
                    continue
                present_count += 1

    status = "ok"
    if file_records:
        if missing or wrong_size or not manifest_present:
            status = "fail"
    elif not manifest_present:
        status = "warn"

    return {
        "dataset_id": ds.dataset_id,
        "family": ds.family,
        "version": ds.version,
        "target_local_path": str(target),
        "manifest_present": manifest_present,
        "plan_file_count": len(file_records),
        "present_files": present_count,
        "missing_files": missing,
        "wrong_size_files": wrong_size,
        "present_bytes": present_bytes,
        "status": status,
    }


def verify_local_datasets(
    *,
    registry: DevscaleRegistry,
    plan_path: str | Path | None = None,
) -> DatasetVerifyReport:
    """按 registry 校验每个 devscale dataset。

    若给了 plan_path（``manifests/devscale_plan.json``），按 plan 中
    ``datasets[*].files[]`` 校验 dst_path/size；否则回退到 dataset 目录下
    的 ``_manifest.json``。
    """
    plan_by_id: dict[str, list[dict[str, Any]]] | None = None
    if plan_path is not None:
        p = Path(plan_path)
        if p.exists():
            try:
                plan = json.loads(p.read_text(encoding="utf-8"))
                plan_by_id = {}
                for ds_plan in plan.get("datasets", []):
                    plan_by_id[ds_plan["dataset_id"]] = list(ds_plan.get("files", []))
            except (json.JSONDecodeError, OSError):
                plan_by_id = None

    datasets_report: list[dict[str, Any]] = []
    total_present = 0
    total_missing = 0
    total_wrong = 0
    total_bytes = 0
    fail = False
    for ds in registry.datasets:
        plan_files = plan_by_id.get(ds.dataset_id) if plan_by_id else None
        info = _verify_one(ds, plan_files=plan_files)
        datasets_report.append(info)
        total_present += info["present_files"]
        total_missing += len(info["missing_files"])
        total_wrong += len(info["wrong_size_files"])
        total_bytes += info["present_bytes"]
        if info["status"] == "fail":
            fail = True

    return DatasetVerifyReport(
        generated_at=_now_iso(),
        datasets=datasets_report,
        totals={
            "present_files": total_present,
            "missing_files": total_missing,
            "wrong_size_files": total_wrong,
            "present_bytes": total_bytes,
        },
        status="fail" if fail else "ok",
    )
