"""sharded ETL 的数据模型。"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class PlanDataset:
    dataset_id: str
    version: str
    dataset_uri: str
    input_bytes: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class PlanShard:
    shard_id: str
    shard_index: int
    datasets: list[PlanDataset] = field(default_factory=list)
    total_bytes: int = 0
    status: str = "PENDING"

    def to_dict(self) -> dict[str, Any]:
        return {
            "shard_id": self.shard_id,
            "shard_index": self.shard_index,
            "total_bytes": int(self.total_bytes),
            "status": self.status,
            "datasets": [d.to_dict() for d in self.datasets],
        }


@dataclass
class EtlPlan:
    plan_id: str
    created_at: str
    root_uri: str
    lake_root: str
    target_shard_size_bytes: int
    total_datasets: int
    total_bytes: int
    shards: list[PlanShard] = field(default_factory=list)
    include_patterns: list[str] = field(default_factory=list)
    exclude_patterns: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "created_at": self.created_at,
            "root_uri": self.root_uri,
            "lake_root": self.lake_root,
            "target_shard_size_bytes": int(self.target_shard_size_bytes),
            "total_datasets": int(self.total_datasets),
            "total_bytes": int(self.total_bytes),
            "include_patterns": list(self.include_patterns),
            "exclude_patterns": list(self.exclude_patterns),
            "shards": [s.to_dict() for s in self.shards],
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "EtlPlan":
        shards: list[PlanShard] = []
        for s in raw.get("shards", []) or []:
            datasets = [PlanDataset(**d) for d in s.get("datasets", []) or []]
            shards.append(
                PlanShard(
                    shard_id=s["shard_id"],
                    shard_index=int(s.get("shard_index", 0)),
                    datasets=datasets,
                    total_bytes=int(s.get("total_bytes", 0)),
                    status=str(s.get("status", "PENDING")),
                )
            )
        return cls(
            plan_id=str(raw.get("plan_id")),
            created_at=str(raw.get("created_at")),
            root_uri=str(raw.get("root_uri", "")),
            lake_root=str(raw.get("lake_root", "")),
            target_shard_size_bytes=int(raw.get("target_shard_size_bytes", 0)),
            total_datasets=int(raw.get("total_datasets", 0)),
            total_bytes=int(raw.get("total_bytes", 0)),
            shards=shards,
            include_patterns=list(raw.get("include_patterns", []) or []),
            exclude_patterns=list(raw.get("exclude_patterns", []) or []),
        )

    def get_shard(self, shard_id: int | str) -> PlanShard | None:
        if isinstance(shard_id, int):
            for s in self.shards:
                if s.shard_index == shard_id:
                    return s
            return None
        for s in self.shards:
            if s.shard_id == shard_id:
                return s
        return None


@dataclass
class ShardSummary:
    plan_id: str
    shard_id: str
    shard_index: int
    status: str
    total: int
    succeeded: int
    failed: int
    skipped: int
    duration_sec: float
    started_at: str
    finished_at: str
    runs: list[dict[str, Any]] = field(default_factory=list)
    summary_uri: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "ShardSummary":
        return cls(
            plan_id=str(raw.get("plan_id", "")),
            shard_id=str(raw.get("shard_id", "")),
            shard_index=int(raw.get("shard_index", 0)),
            status=str(raw.get("status", "")),
            total=int(raw.get("total", 0)),
            succeeded=int(raw.get("succeeded", 0)),
            failed=int(raw.get("failed", 0)),
            skipped=int(raw.get("skipped", 0)),
            duration_sec=float(raw.get("duration_sec", 0.0)),
            started_at=str(raw.get("started_at", "")),
            finished_at=str(raw.get("finished_at", "")),
            runs=list(raw.get("runs", []) or []),
            summary_uri=raw.get("summary_uri"),
        )
