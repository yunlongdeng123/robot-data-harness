"""distill_report.json 数据类。"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class DistillReport:
    """蒸馏数据集报告。"""

    distill_id: str
    distill_format: str
    teacher_model_id: str | None
    source_inference_job_id: str | None
    dataset_id: str | None
    version: str | None
    output_uri: str
    train_uri: str
    val_uri: str
    test_uri: str
    num_total: int
    num_train: int
    num_val: int
    num_test: int
    num_skipped: int
    status: str
    split_ratio: list[float] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "distill_id": self.distill_id,
            "distill_format": self.distill_format,
            "teacher_model_id": self.teacher_model_id,
            "source_inference_job_id": self.source_inference_job_id,
            "dataset_id": self.dataset_id,
            "version": self.version,
            "output_uri": self.output_uri,
            "train_uri": self.train_uri,
            "val_uri": self.val_uri,
            "test_uri": self.test_uri,
            "num_total": self.num_total,
            "num_train": self.num_train,
            "num_val": self.num_val,
            "num_test": self.num_test,
            "num_skipped": self.num_skipped,
            "status": self.status,
            "split_ratio": list(self.split_ratio),
        }
