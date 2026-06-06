"""v1.9 批量推理：input builder / runner / outputs / metrics / benchmark。"""

from robot_dh.inference.batch import (
    InferenceInputBuilder,
    InferenceInputError,
)
from robot_dh.inference.benchmark import BenchmarkResult, run_benchmark
from robot_dh.inference.job import (
    InferenceJob,
    get_job,
    list_jobs,
    new_job_id,
)
from robot_dh.inference.metrics import InferenceMetrics, compute_metrics, percentile
from robot_dh.inference.runner import (
    InferenceJobError,
    InferenceRunResult,
    run_inference,
)

__all__ = [
    "InferenceInputBuilder",
    "InferenceInputError",
    "InferenceJob",
    "InferenceJobError",
    "InferenceRunResult",
    "run_inference",
    "new_job_id",
    "list_jobs",
    "get_job",
    "InferenceMetrics",
    "compute_metrics",
    "percentile",
    "BenchmarkResult",
    "run_benchmark",
]
