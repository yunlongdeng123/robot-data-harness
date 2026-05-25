"""v1.6 progress / heartbeat / checkpoint 模块。

公开 API：
    HeartbeatReporter      ：周期性心跳上报（JSONL + structured log + 可选 PG）
    ProgressLogger         ：长循环周期性进度日志（结构化）
    Checkpoint / CheckpointStore ：normalize 等长任务的步骤级 checkpoint
"""

from robot_dh.progress.heartbeat import HeartbeatReporter, HeartbeatPayload, default_heartbeats_dir
from robot_dh.progress.progress_logger import ProgressLogger
from robot_dh.progress.checkpoint import (
    Checkpoint,
    CheckpointFile,
    CheckpointStore,
    CHECKPOINT_FILENAME,
    load_checkpoint,
    save_checkpoint,
)

__all__ = [
    "HeartbeatReporter",
    "HeartbeatPayload",
    "default_heartbeats_dir",
    "ProgressLogger",
    "Checkpoint",
    "CheckpointFile",
    "CheckpointStore",
    "CHECKPOINT_FILENAME",
    "load_checkpoint",
    "save_checkpoint",
]
