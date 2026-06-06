"""推理任务状态机与结果判定。

状态取值与 postgres/migrations/007 的 inference_jobs.status 对齐。
"""

from __future__ import annotations

from dataclasses import dataclass

JOB_CREATED = "CREATED"
JOB_QUEUED = "QUEUED"
JOB_RUNNING = "RUNNING"
JOB_SUCCEEDED = "SUCCEEDED"
JOB_FAILED = "FAILED"
JOB_RETRYING = "RETRYING"
JOB_CANCELLED = "CANCELLED"
JOB_DEAD_LETTER = "DEAD_LETTER"

JOB_STATUSES: tuple[str, ...] = (
    JOB_CREATED,
    JOB_QUEUED,
    JOB_RUNNING,
    JOB_SUCCEEDED,
    JOB_FAILED,
    JOB_RETRYING,
    JOB_CANCELLED,
    JOB_DEAD_LETTER,
)

TERMINAL_STATUSES: frozenset[str] = frozenset(
    {JOB_SUCCEEDED, JOB_FAILED, JOB_CANCELLED, JOB_DEAD_LETTER}
)

# 允许的状态迁移（用于校验，不强制；v1.9 轻量）。
VALID_TRANSITIONS: dict[str, set[str]] = {
    JOB_CREATED: {JOB_QUEUED, JOB_RUNNING, JOB_CANCELLED},
    JOB_QUEUED: {JOB_RUNNING, JOB_CANCELLED},
    JOB_RUNNING: {JOB_SUCCEEDED, JOB_FAILED, JOB_RETRYING, JOB_CANCELLED},
    JOB_RETRYING: {JOB_RUNNING, JOB_FAILED, JOB_DEAD_LETTER, JOB_CANCELLED},
    JOB_FAILED: {JOB_RETRYING, JOB_DEAD_LETTER},
    JOB_SUCCEEDED: set(),
    JOB_CANCELLED: set(),
    JOB_DEAD_LETTER: set(),
}


def can_transition(src: str, dst: str) -> bool:
    return dst in VALID_TRANSITIONS.get(src, set())


def is_terminal(status: str) -> bool:
    return status in TERMINAL_STATUSES


@dataclass
class JobOutcome:
    """job 收尾判定结果。

    exit_code 语义（见 v1_9_promptB 第六节）：
    - 全部成功 -> SUCCEEDED, exit 0
    - 部分失败但低于阈值 -> SUCCEEDED + warn, exit 0
    - 失败率过高或 fail-fast -> FAILED, exit 1
    """

    status: str
    warn: bool
    exit_code: int
    error_rate: float


def evaluate_job_outcome(
    total: int,
    failed: int,
    *,
    max_error_rate: float = 0.5,
    fail_fast_triggered: bool = False,
) -> JobOutcome:
    """根据样本成功/失败数与阈值给出 job 终态。"""
    error_rate = (failed / total) if total > 0 else (1.0 if failed > 0 else 0.0)
    if fail_fast_triggered or error_rate > max_error_rate:
        return JobOutcome(status=JOB_FAILED, warn=False, exit_code=1, error_rate=error_rate)
    warn = failed > 0
    return JobOutcome(status=JOB_SUCCEEDED, warn=warn, exit_code=0, error_rate=error_rate)
