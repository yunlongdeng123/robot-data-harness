"""v1.7 RobotDatasetAdapter 注册表。

`RobotDatasetAdapter` 是一个轻量统一接口，把 v1.6 已有的 hf_adapter / qc /
normalize 串成"识别 -> probe -> normalize -> contract"四件套。它**不重写**
现有的 hf_adapters / lerobot_v2 / hdf5_probe / parquet_probe，只在上面盖一层
路由 + 默认参数。

公开 API：

    from robot_dh.adapters import (
        RobotDatasetAdapter,
        DetectionResult,
        ProbeResult,
        get_adapter,
        detect_adapter,
        list_adapters,
        load_adapter_registry,
    )
"""

from robot_dh.adapters.base import (
    DetectionResult,
    EpisodeRef,
    NormalizeResult,
    ProbeResult,
    RobotDatasetAdapter,
)
from robot_dh.adapters.registry import (
    AdapterRegistry,
    detect_adapter,
    get_adapter,
    list_adapters,
    load_adapter_registry,
)

__all__ = [
    "RobotDatasetAdapter",
    "DetectionResult",
    "EpisodeRef",
    "NormalizeResult",
    "ProbeResult",
    "AdapterRegistry",
    "detect_adapter",
    "get_adapter",
    "list_adapters",
    "load_adapter_registry",
]
