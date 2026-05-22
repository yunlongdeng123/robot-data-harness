from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any


LOGGER_NAME = "robot_dh"
_CURRENT_LOG_FORMAT = "human"


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        if isinstance(record.msg, dict):
            payload = dict(record.msg)
        else:
            payload = {"message": record.getMessage()}
        payload.setdefault("timestamp", datetime.now(timezone.utc).isoformat())
        payload.setdefault("level", record.levelname)
        return json.dumps(payload, ensure_ascii=False, default=str)


def configure_logging(level: int = logging.INFO, log_format: str = "human") -> logging.Logger:
    global _CURRENT_LOG_FORMAT
    _CURRENT_LOG_FORMAT = log_format
    root = logging.getLogger()
    root.handlers.clear()
    handler = logging.StreamHandler()
    if log_format == "json":
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(logging.Formatter("%(message)s"))
    root.setLevel(level)
    root.addHandler(handler)
    return logging.getLogger(LOGGER_NAME)


def get_logger() -> logging.Logger:
    return logging.getLogger(LOGGER_NAME)


def get_log_format() -> str:
    return _CURRENT_LOG_FORMAT


def log_event(event: str, *, level: int = logging.INFO, **fields: Any) -> None:
    logger = get_logger()
    if _CURRENT_LOG_FORMAT != "json":
        message = fields.pop("message", None)
        if message is not None:
            logger.log(level, message)
        return
    logger.log(level, {"event": event, **fields})
