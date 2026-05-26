"""devscale dataset 注册表：读 configs/devscale_datasets.yaml。

每条 dataset 描述：
  - dataset_id / family / version
  - source_uri（远端 s3:// 或 hf://）
  - target_local_uri（file://...）
  - include / exclude / max_bytes / max_files

提供 :func:`load_devscale_registry` 与 :class:`DevscaleDataset` / :class:`DevscaleRegistry`。
不实际下载或验证（那是 scripts/local_*.sh 的事）；仅做声明 + 路径解析。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from robot_dh.lake.uri import parse_uri
from robot_dh.local_runtime.paths import LocalRuntimeConfig


@dataclass(frozen=True, slots=True)
class DevscaleDataset:
    dataset_id: str
    family: str
    version: str
    source_uri: str
    target_local_uri: str
    target_local_path: str
    max_bytes: int | None
    max_files: int | None
    include: tuple[str, ...] = field(default_factory=tuple)
    exclude: tuple[str, ...] = field(default_factory=tuple)

    @property
    def manifest_path(self) -> str:
        return str(Path(self.target_local_path) / "_manifest.json")

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset_id": self.dataset_id,
            "family": self.family,
            "version": self.version,
            "source_uri": self.source_uri,
            "target_local_uri": self.target_local_uri,
            "target_local_path": self.target_local_path,
            "max_bytes": self.max_bytes,
            "max_files": self.max_files,
            "include": list(self.include),
            "exclude": list(self.exclude),
            "manifest_path": self.manifest_path,
        }


@dataclass(frozen=True, slots=True)
class DevscaleRegistry:
    datasets: tuple[DevscaleDataset, ...]
    total_max_bytes: int
    raw_yaml_path: str

    def by_id(self, dataset_id: str) -> DevscaleDataset:
        for d in self.datasets:
            if d.dataset_id == dataset_id:
                return d
        raise KeyError(f"devscale dataset not registered: {dataset_id}")

    def families(self) -> set[str]:
        return {d.family for d in self.datasets}

    def to_dict(self) -> dict[str, Any]:
        return {
            "raw_yaml_path": self.raw_yaml_path,
            "total_max_bytes": self.total_max_bytes,
            "datasets": [d.to_dict() for d in self.datasets],
        }


_DEFAULT_HOST_PREFIX = "/mnt/d/robot-dh-local"


def _retarget_local(target_uri: str, config: LocalRuntimeConfig | None) -> tuple[str, str]:
    """把 yaml 里的默认 host 路径 (``/mnt/d/robot-dh-local/...``) 替换成
    实际 runtime root（容器内 ``/mnt/local-data/robot-dh-local/...``）。

    若 target 不在默认前缀下，保持原样（dev 自定义 layout）。返回 (uri, posix_path)。
    """
    parsed = parse_uri(target_uri)
    if not parsed.is_local:
        raise ValueError(f"devscale target_local_uri must be local: {target_uri}")
    local_path = parsed.local_path
    if config is not None and local_path.startswith(_DEFAULT_HOST_PREFIX):
        rel = local_path[len(_DEFAULT_HOST_PREFIX):].lstrip("/")
        # rel 形如 ``raw/<dataset_id>/<version>``；尝试映射到容器内根。
        local_path = str(Path(config.k8s_data_root) / rel) if rel else config.k8s_data_root
    posix = local_path.rstrip("/")
    if not posix.startswith("/"):
        posix = "/" + posix
    return f"file://{posix}", posix


def load_devscale_registry(
    *,
    config_path: str | Path = "configs/devscale_datasets.yaml",
    runtime_config: LocalRuntimeConfig | None = None,
) -> DevscaleRegistry:
    """读取 devscale 数据集清单 yaml，应用 runtime_config 的路径重定向。"""
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"devscale config not found: {path}")
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    raw_datasets = data.get("datasets") or []
    if not raw_datasets:
        raise ValueError(f"no datasets entries in {path}")

    total_max = int(data.get("total_max_bytes") or 3_000_000_000)

    items: list[DevscaleDataset] = []
    for d in raw_datasets:
        target_uri = str(d["target_local_uri"])
        local_uri, local_path = _retarget_local(target_uri, runtime_config)
        items.append(
            DevscaleDataset(
                dataset_id=str(d["dataset_id"]),
                family=str(d["family"]),
                version=str(d.get("version", "v1")),
                source_uri=str(d["source_uri"]),
                target_local_uri=local_uri,
                target_local_path=local_path,
                max_bytes=int(d["max_bytes"]) if d.get("max_bytes") is not None else None,
                max_files=int(d["max_files"]) if d.get("max_files") is not None else None,
                include=tuple(str(x) for x in (d.get("include") or [])),
                exclude=tuple(str(x) for x in (d.get("exclude") or [])),
            )
        )

    return DevscaleRegistry(
        datasets=tuple(items),
        total_max_bytes=total_max,
        raw_yaml_path=str(path.resolve()),
    )
