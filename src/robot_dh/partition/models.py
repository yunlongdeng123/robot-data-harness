"""partition 数据结构。"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal

PartitionType = Literal["file_prefix", "episode", "hdf5_file", "parquet_file", "single"]


@dataclass(slots=True)
class Partition:
    partition_id: str
    partition_index: int
    dataset_uri: str
    partition_uri: str
    input_files: list[str] = field(default_factory=list)
    input_bytes: int = 0
    estimated_rows: int = 0
    metrics: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class PartitionPlan:
    partition_plan_id: str
    dataset_id: str
    version: str
    dataset_uri: str
    dataset_family: str
    partition_type: PartitionType
    target_partition_size_bytes: int
    total_input_bytes: int
    partitions: list[Partition] = field(default_factory=list)
    created_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "partition_plan_id": self.partition_plan_id,
            "dataset_id": self.dataset_id,
            "version": self.version,
            "dataset_uri": self.dataset_uri,
            "dataset_family": self.dataset_family,
            "partition_type": self.partition_type,
            "target_partition_size_bytes": self.target_partition_size_bytes,
            "total_input_bytes": self.total_input_bytes,
            "partitions": [p.to_dict() for p in self.partitions],
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "PartitionPlan":
        partitions = [
            Partition(
                partition_id=p["partition_id"],
                partition_index=int(p["partition_index"]),
                dataset_uri=p.get("dataset_uri", ""),
                partition_uri=p.get("partition_uri", ""),
                input_files=list(p.get("input_files") or []),
                input_bytes=int(p.get("input_bytes") or 0),
                estimated_rows=int(p.get("estimated_rows") or 0),
                metrics=dict(p.get("metrics") or {}),
            )
            for p in payload.get("partitions") or []
        ]
        return cls(
            partition_plan_id=str(payload.get("partition_plan_id")),
            dataset_id=str(payload.get("dataset_id")),
            version=str(payload.get("version")),
            dataset_uri=str(payload.get("dataset_uri")),
            dataset_family=str(payload.get("dataset_family", "unknown")),
            partition_type=str(payload.get("partition_type", "single")),  # type: ignore[arg-type]
            target_partition_size_bytes=int(payload.get("target_partition_size_bytes") or 0),
            total_input_bytes=int(payload.get("total_input_bytes") or 0),
            partitions=partitions,
            created_at=str(payload.get("created_at") or ""),
        )
