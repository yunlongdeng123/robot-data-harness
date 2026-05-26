"""v1.7 Local-First Robot Data Platform Runtime - 本地运行时。

集中处理：
  - LocalRuntimeConfig 路径口径（host / k8s / raw / lake / cache / workdir / manifests / logs）；
  - devscale dataset 注册（消费 configs/devscale_datasets.yaml）；
  - preflight doctor（本地 root 存在、可写、devscale manifest 完整、总量 <= 3GB）；
  - 本地 datasets verify（按 manifest 校验文件存在 + size）。

公开接口：

    from robot_dh.local_runtime import (
        LocalRuntimeConfig,
        DevscaleDataset,
        load_devscale_registry,
        load_runtime_config,
        runtime_doctor,
        verify_local_datasets,
    )
"""

from robot_dh.local_runtime.paths import (
    LocalRuntimeConfig,
    load_runtime_config,
)
from robot_dh.local_runtime.devscale import (
    DevscaleDataset,
    DevscaleRegistry,
    load_devscale_registry,
)
from robot_dh.local_runtime.preflight import (
    RuntimeDoctorReport,
    runtime_doctor,
)
from robot_dh.local_runtime.verification import (
    DatasetVerifyReport,
    verify_local_datasets,
)

__all__ = [
    "LocalRuntimeConfig",
    "load_runtime_config",
    "DevscaleDataset",
    "DevscaleRegistry",
    "load_devscale_registry",
    "RuntimeDoctorReport",
    "runtime_doctor",
    "DatasetVerifyReport",
    "verify_local_datasets",
]
