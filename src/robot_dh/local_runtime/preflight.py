"""runtime doctor：v1.7 本地数据运行时的健康检查。

不依赖 docker / kind / kubectl —— 仅检查 Python 进程视角下的：
  - local root 存在 + 可写
  - raw / lake / cache / manifests / logs 子目录存在
  - 每个 devscale dataset 的 _manifest.json 是否存在
  - 总大小是否在限额内
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from typing import Any

from robot_dh.local_runtime.devscale import (
    DevscaleDataset,
    DevscaleRegistry,
    load_devscale_registry,
)
from robot_dh.local_runtime.paths import LocalRuntimeConfig, load_runtime_config


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _check_dir(path: str, *, require_writable: bool) -> dict[str, Any]:
    p = Path(path)
    out: dict[str, Any] = {
        "path": str(p),
        "exists": p.exists(),
        "is_dir": p.is_dir() if p.exists() else False,
        "writable": False,
    }
    if not p.exists():
        return out
    if require_writable:
        probe = p / ".robot_dh_doctor_probe"
        try:
            probe.touch()
            probe.unlink(missing_ok=True)
            out["writable"] = True
        except OSError as err:
            out["writable"] = False
            out["error"] = f"{type(err).__name__}: {err}"
    else:
        out["writable"] = os.access(p, os.W_OK)
    return out


def _check_dataset(ds: DevscaleDataset) -> dict[str, Any]:
    path = Path(ds.target_local_path)
    manifest_path = Path(ds.manifest_path)
    out: dict[str, Any] = {
        "dataset_id": ds.dataset_id,
        "family": ds.family,
        "version": ds.version,
        "target_local_path": ds.target_local_path,
        "exists": path.exists(),
        "manifest_present": manifest_path.exists(),
        "manifest_status": None,
        "size_bytes": 0,
        "file_count": 0,
        "issues": [],
    }
    if not path.exists():
        out["issues"].append("target_dir_missing")
        return out

    total_size = 0
    file_count = 0
    for f in path.rglob("*"):
        if f.is_file() and f.name != "_manifest.json":
            try:
                total_size += f.stat().st_size
                file_count += 1
            except OSError:
                continue
    out["size_bytes"] = total_size
    out["file_count"] = file_count

    if ds.max_bytes is not None and total_size > ds.max_bytes:
        out["issues"].append(
            f"size_over_max_bytes({total_size}>{ds.max_bytes})"
        )

    if manifest_path.exists():
        try:
            m = json.loads(manifest_path.read_text(encoding="utf-8"))
            out["manifest_status"] = m.get("status")
            out["manifest_file_count"] = m.get("file_count")
            out["manifest_size_bytes"] = m.get("size_bytes")
            if m.get("status") not in ("ok", "OK"):
                out["issues"].append(f"manifest_status={m.get('status')}")
        except (json.JSONDecodeError, OSError) as err:
            out["issues"].append(f"manifest_unreadable:{type(err).__name__}")
    else:
        out["issues"].append("manifest_missing")

    return out


@dataclass(slots=True)
class RuntimeDoctorReport:
    generated_at: str
    runtime: dict[str, Any]
    directories: dict[str, dict[str, Any]]
    devscale: dict[str, Any]
    issues: list[str] = field(default_factory=list)
    status: str = "ok"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def runtime_doctor(
    *,
    runtime_config: LocalRuntimeConfig | None = None,
    devscale_config_path: str | Path = "configs/devscale_datasets.yaml",
    devscale_registry: DevscaleRegistry | None = None,
    allow_over_limit: bool = False,
) -> RuntimeDoctorReport:
    """检查本地运行时；返回结构化 report。

    若 ``devscale_registry`` 传入，则忽略 ``devscale_config_path``，便于测试用 mock。
    """
    cfg = runtime_config or load_runtime_config()

    directories: dict[str, dict[str, Any]] = {
        "host_data_root": _check_dir(cfg.host_data_root, require_writable=False),
        "k8s_data_root": _check_dir(cfg.k8s_data_root, require_writable=True),
        "raw_root": _check_dir(cfg.raw_root, require_writable=False),
        "lake_root": _check_dir(cfg.lake_root, require_writable=True),
        "cache_root": _check_dir(cfg.cache_root, require_writable=True),
        "workdir_root": _check_dir(cfg.workdir_root, require_writable=True),
        "manifests_root": _check_dir(cfg.manifests_root, require_writable=False),
        "logs_root": _check_dir(cfg.logs_root, require_writable=True),
    }

    issues: list[str] = []
    for name, info in directories.items():
        if not info["exists"]:
            issues.append(f"{name}_missing")
            continue
        if "writable" in info and not info["writable"] and name in {
            "k8s_data_root", "lake_root", "cache_root", "workdir_root", "logs_root",
        }:
            issues.append(f"{name}_not_writable")

    registry = devscale_registry
    devscale_section: dict[str, Any] = {
        "total_max_bytes": cfg.devscale_total_max_bytes,
        "datasets": [],
        "actual_total_size_bytes": 0,
    }
    if registry is None:
        try:
            registry = load_devscale_registry(
                config_path=devscale_config_path,
                runtime_config=cfg,
            )
        except FileNotFoundError as err:
            issues.append(f"devscale_config_missing:{err}")
        except ValueError as err:
            issues.append(f"devscale_config_invalid:{err}")

    if registry is not None:
        devscale_section["raw_yaml_path"] = registry.raw_yaml_path
        devscale_section["total_max_bytes"] = registry.total_max_bytes
        actual_total = 0
        for ds in registry.datasets:
            info = _check_dataset(ds)
            devscale_section["datasets"].append(info)
            actual_total += info["size_bytes"]
            if info["issues"]:
                issues.append(f"dataset:{ds.dataset_id}:{','.join(info['issues'])}")
        devscale_section["actual_total_size_bytes"] = actual_total
        if not allow_over_limit and actual_total > registry.total_max_bytes:
            issues.append(
                f"devscale_total_over_limit({actual_total}>{registry.total_max_bytes})"
            )

    status = "ok" if not issues else "fail"
    return RuntimeDoctorReport(
        generated_at=_now_iso(),
        runtime=cfg.to_dict(),
        directories=directories,
        devscale=devscale_section,
        issues=issues,
        status=status,
    )
