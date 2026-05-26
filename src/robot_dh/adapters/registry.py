"""AdapterRegistry：把内置 adapter（droid / robomimic / bridge / universal）注册起来，
按 ``detect()`` 的 confidence 选最高分；持平时按 yaml 中的声明顺序。

配置文件：``configs/dataset_adapters.yaml``（仅用于覆盖默认参数，不增删 adapter）。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from robot_dh.adapters.base import DetectionResult, RobotDatasetAdapter
from robot_dh.adapters.bridgedata import BridgeDataAdapter
from robot_dh.adapters.droid_lerobot import DroidLeRobotAdapter
from robot_dh.adapters.robomimic import RobomimicAdapter
from robot_dh.adapters.universal import UniversalAdapter


_BUILTIN_ORDER = ("droid", "robomimic", "bridge", "universal")


def _build_default_adapters() -> dict[str, RobotDatasetAdapter]:
    return {
        "droid": DroidLeRobotAdapter(),
        "robomimic": RobomimicAdapter(),
        "bridge": BridgeDataAdapter(),
        "universal": UniversalAdapter(),
    }


@dataclass(slots=True)
class AdapterRegistry:
    adapters: dict[str, RobotDatasetAdapter] = field(default_factory=_build_default_adapters)
    yaml_overrides: dict[str, dict[str, Any]] = field(default_factory=dict)

    def list_families(self) -> list[str]:
        return list(self.adapters.keys())

    def get(self, family: str) -> RobotDatasetAdapter:
        key = family.lower()
        if key not in self.adapters:
            return self.adapters["universal"]
        return self.adapters[key]

    def detect(
        self,
        dataset_uri: str,
        *,
        dataset_id: str | None = None,
        listings: list[str] | None = None,
    ) -> DetectionResult:
        candidates: list[DetectionResult] = []
        for fam in _BUILTIN_ORDER:
            adapter = self.adapters.get(fam)
            if adapter is None:
                continue
            res = adapter.detect(dataset_uri, dataset_id=dataset_id, listings=listings)
            candidates.append(res)
        # universal confidence=0.1 永远兜底；按 confidence 降序，相同保持声明顺序。
        candidates.sort(key=lambda r: r.confidence, reverse=True)
        return candidates[0]

    def detect_all(
        self,
        dataset_uri: str,
        *,
        dataset_id: str | None = None,
        listings: list[str] | None = None,
    ) -> list[DetectionResult]:
        """返回所有 adapter 的 detect 结果（含 confidence 与命中标记），调试用。"""
        return [
            self.adapters[fam].detect(dataset_uri, dataset_id=dataset_id, listings=listings)
            for fam in _BUILTIN_ORDER
            if fam in self.adapters
        ]

    def normalize_options_for(self, family: str, dataset_uri: str) -> dict[str, Any]:
        base = self.get(family).normalize_options(dataset_uri)
        override = (self.yaml_overrides.get(family) or {}).get("normalize_options") or {}
        return {**base, **override}

    def qc_options_for(self, family: str) -> dict[str, Any]:
        return dict((self.yaml_overrides.get(family) or {}).get("qc_options") or {})


_DEFAULT_REGISTRY: AdapterRegistry | None = None


def load_adapter_registry(
    *,
    config_path: str | Path | None = "configs/dataset_adapters.yaml",
) -> AdapterRegistry:
    """读取 dataset_adapters.yaml 中的 overrides，构建一个新的 registry。"""
    overrides: dict[str, dict[str, Any]] = {}
    if config_path is not None:
        path = Path(config_path)
        if path.exists():
            data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
            for item in data.get("adapters", []):
                fam = str(item.get("family") or "").lower()
                if not fam:
                    continue
                overrides[fam] = {
                    "normalize_options": item.get("normalize_options") or {},
                    "qc_options": item.get("qc_options") or {},
                    "dataset_id_prefixes": item.get("dataset_id_prefixes") or [],
                }
    reg = AdapterRegistry(yaml_overrides=overrides)
    return reg


def _default() -> AdapterRegistry:
    global _DEFAULT_REGISTRY
    if _DEFAULT_REGISTRY is None:
        try:
            _DEFAULT_REGISTRY = load_adapter_registry()
        except Exception:  # noqa: BLE001 - 任何 yaml 失败都不阻断默认 adapter
            _DEFAULT_REGISTRY = AdapterRegistry()
    return _DEFAULT_REGISTRY


def get_adapter(family: str) -> RobotDatasetAdapter:
    return _default().get(family)


def list_adapters() -> list[str]:
    return _default().list_families()


def detect_adapter(
    dataset_uri: str,
    *,
    dataset_id: str | None = None,
    listings: list[str] | None = None,
) -> DetectionResult:
    return _default().detect(dataset_uri, dataset_id=dataset_id, listings=listings)
