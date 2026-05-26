"""LocalRuntimeConfig：v1.7 本地路径口径。

容器内（kind node / pod）路径：
    /mnt/local-data/robot-dh-local/
        raw/                # devscale raw 镜像
        lake/{ods,dwd,ads,qc,ml-ready}
        cache/input-cache/  # normalize resume 复用
        cache/argo-workdir/ # Argo step 临时区
        manifests/          # devscale_plan.json 等
        logs/

WSL host 路径默认 ``/mnt/d/robot-dh-local``；可通过环境变量覆盖：

  ROBOT_DH_LOCAL_DATA_ROOT    -> host_data_root（也是 k8s_data_root 的默认）
  ROBOT_DH_K8S_LOCAL_DATA_ROOT-> 容器内挂载点（kind extraMounts 的 containerPath）
  ROBOT_DH_DEV_DATA_ROOT      -> raw_root（file:// URI）
  ROBOT_DH_DEV_LAKE_ROOT      -> lake_root（file:// URI）
  ROBOT_DH_INPUT_CACHE_DIR    -> cache_root/input-cache
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

DEFAULT_HOST_DATA_ROOT = "/mnt/d/robot-dh-local"
DEFAULT_K8S_DATA_ROOT = "/mnt/local-data/robot-dh-local"


@dataclass(frozen=True, slots=True)
class LocalRuntimeConfig:
    """v1.7 本地运行时路径配置。

    所有 ``*_root`` 都是**绝对 POSIX 路径**；``*_uri`` 是 ``file://...`` 形式。
    实例化后不修改；如需 override 路径，通过 :func:`load_runtime_config` 重新加载。
    """

    host_data_root: str
    k8s_data_root: str
    raw_root: str
    lake_root: str
    cache_root: str
    workdir_root: str
    manifests_root: str
    logs_root: str
    devscale_total_max_bytes: int = 3_000_000_000
    devscale_total_max_files: int = 5000
    devscale_dataset_ids: tuple[str, ...] = field(default_factory=tuple)

    # ----- 派生 URI -----
    @property
    def raw_uri(self) -> str:
        return _file_uri(self.raw_root)

    @property
    def lake_uri(self) -> str:
        return _file_uri(self.lake_root)

    @property
    def cache_uri(self) -> str:
        return _file_uri(self.cache_root)

    @property
    def input_cache_dir(self) -> str:
        return str(Path(self.cache_root) / "input-cache")

    def raw_uri_for(self, dataset_id: str, version: str = "v1") -> str:
        """devscale dataset 的本地 raw URI，供 Argo workflow 参数使用。"""
        return _file_uri(str(Path(self.raw_root) / dataset_id / version))

    def lake_uri_for(self, layer: str, dataset_id: str, version: str = "v1") -> str:
        return _file_uri(str(Path(self.lake_root) / layer / dataset_id / version))

    def to_env(self) -> dict[str, str]:
        """渲染成可 source 的环境变量；调用方按需 export。"""
        return {
            "ROBOT_DH_LOCAL_DATA_ROOT": self.host_data_root,
            "ROBOT_DH_K8S_LOCAL_DATA_ROOT": self.k8s_data_root,
            "ROBOT_DH_DEV_DATA_ROOT": self.raw_uri,
            "ROBOT_DH_DEV_LAKE_ROOT": self.lake_uri,
            "ROBOT_DH_INPUT_CACHE_DIR": self.input_cache_dir,
            "ROBOT_DH_ARGO_WORKDIR": self.workdir_root,
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "host_data_root": self.host_data_root,
            "k8s_data_root": self.k8s_data_root,
            "raw_root": self.raw_root,
            "lake_root": self.lake_root,
            "cache_root": self.cache_root,
            "workdir_root": self.workdir_root,
            "manifests_root": self.manifests_root,
            "logs_root": self.logs_root,
            "devscale_total_max_bytes": self.devscale_total_max_bytes,
            "devscale_total_max_files": self.devscale_total_max_files,
            "devscale_dataset_ids": list(self.devscale_dataset_ids),
            "raw_uri": self.raw_uri,
            "lake_uri": self.lake_uri,
            "cache_uri": self.cache_uri,
            "input_cache_dir": self.input_cache_dir,
        }


def _file_uri(abs_path: str) -> str:
    p = abs_path.rstrip("/")
    if not p.startswith("/"):
        p = "/" + p
    return f"file://{p}"


def _resolve_root(env_value: str | None, default: str) -> str:
    if env_value:
        return os.path.abspath(os.path.expanduser(env_value))
    return default


def load_runtime_config(
    *,
    config_path: str | Path | None = None,
    host_data_root: str | None = None,
    k8s_data_root: str | None = None,
) -> LocalRuntimeConfig:
    """加载 v1.7 本地运行时配置。

    优先级（高 -> 低）：
      1. 函数参数 ``host_data_root`` / ``k8s_data_root``；
      2. 环境变量 ``ROBOT_DH_LOCAL_DATA_ROOT`` / ``ROBOT_DH_K8S_LOCAL_DATA_ROOT``；
      3. ``config_path`` 指向的 yaml（默认 ``configs/devscale_runtime.yaml``，
         相对当前工作目录）；
      4. 内置默认值（``/mnt/d/robot-dh-local`` / ``/mnt/local-data/robot-dh-local``）。

    raw / lake / cache / workdir / manifests / logs 的子目录命名一律从 yaml 读，
    不允许通过 env 单独覆盖（保持目录契约稳定）。
    """
    yaml_data: dict[str, Any] = {}
    if config_path is None:
        candidate = Path("configs") / "devscale_runtime.yaml"
        if candidate.exists():
            config_path = candidate
    if config_path is not None:
        path = Path(config_path)
        if path.exists():
            yaml_data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}

    runtime_yaml: dict[str, Any] = yaml_data.get("runtime") or {}
    limits_yaml: dict[str, Any] = yaml_data.get("limits") or {}
    datasets_yaml = yaml_data.get("devscale_datasets") or []

    host = host_data_root or os.environ.get("ROBOT_DH_LOCAL_DATA_ROOT") or \
        runtime_yaml.get("host_data_root") or DEFAULT_HOST_DATA_ROOT
    k8s = k8s_data_root or os.environ.get("ROBOT_DH_K8S_LOCAL_DATA_ROOT") or \
        runtime_yaml.get("k8s_data_root") or DEFAULT_K8S_DATA_ROOT
    host = os.path.abspath(os.path.expanduser(host))
    k8s = os.path.abspath(os.path.expanduser(k8s))

    raw_sub = str(runtime_yaml.get("raw_subdir", "raw"))
    lake_sub = str(runtime_yaml.get("lake_subdir", "lake"))
    cache_sub = str(runtime_yaml.get("cache_subdir", "cache"))
    workdir_sub = str(runtime_yaml.get("workdir_subdir", "cache/argo-workdir"))
    manifests_sub = str(runtime_yaml.get("manifests_subdir", "manifests"))
    logs_sub = str(runtime_yaml.get("logs_subdir", "logs"))

    # CLI / 容器内代码默认面向 k8s_data_root（即容器内挂载点）；
    # 在 WSL host 上跑 CLI 时，用户通过 ROBOT_DH_K8S_LOCAL_DATA_ROOT=host
    # 把它指向同一目录，行为一致。
    base = k8s
    raw_root = str(Path(base) / raw_sub)
    lake_root = str(Path(base) / lake_sub)
    cache_root = str(Path(base) / cache_sub)
    workdir_root = str(Path(base) / workdir_sub)
    manifests_root = str(Path(base) / manifests_sub)
    logs_root = str(Path(base) / logs_sub)

    total_max_bytes = int(limits_yaml.get("devscale_total_max_bytes") or 3_000_000_000)
    total_max_files = int(limits_yaml.get("devscale_total_max_files") or 5000)

    return LocalRuntimeConfig(
        host_data_root=host,
        k8s_data_root=k8s,
        raw_root=raw_root,
        lake_root=lake_root,
        cache_root=cache_root,
        workdir_root=workdir_root,
        manifests_root=manifests_root,
        logs_root=logs_root,
        devscale_total_max_bytes=total_max_bytes,
        devscale_total_max_files=total_max_files,
        devscale_dataset_ids=tuple(str(x) for x in datasets_yaml),
    )
