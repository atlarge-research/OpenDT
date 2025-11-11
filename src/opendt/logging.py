"""Application-wide logging configuration with JSON output."""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone


class JSONLogFormatter(logging.Formatter):
    """Serialize log records as structured JSON for easy filtering."""

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        for key, value in record.__dict__.items():
            if key.startswith("_"):
                continue
            if key in payload or key in {"args", "created", "exc_info", "exc_text", "filename", "funcName",
                                         "levelname", "levelno", "lineno", "message", "module", "msecs",
                                         "msg", "name", "pathname", "process", "processName", "relativeCreated",
                                         "stack_info", "thread", "threadName"}:
                continue
            payload[key] = value
        return json.dumps(payload, ensure_ascii=False)


def configure_logging() -> None:
    level = os.environ.get("OPENDT_LOG_LEVEL", "INFO").upper()
    handler = logging.StreamHandler()
    handler.setFormatter(JSONLogFormatter())

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)


configure_logging()
