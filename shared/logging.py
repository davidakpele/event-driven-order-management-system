# blast_assessment/shared/logging.py

import logging
import json
import sys
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from contextvars import ContextVar

# Context variables for request tracing
correlation_id_var: ContextVar[Optional[str]] = ContextVar("correlation_id", default=None)
service_name_var: ContextVar[str] = ContextVar("service_name", default="blast")


class StructuredFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log_entry: Dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "service": service_name_var.get(),
            "logger": record.name,
            "message": record.getMessage(),
        }

        correlation_id = correlation_id_var.get()
        if correlation_id:
            log_entry["correlation_id"] = correlation_id

        # Extra fields attached via logger.info("msg", extra={...})
        for key, value in record.__dict__.items():
            if key not in (
                "name", "msg", "args", "levelname", "levelno", "pathname",
                "filename", "module", "exc_info", "exc_text", "stack_info",
                "lineno", "funcName", "created", "msecs", "relativeCreated",
                "thread", "threadName", "processName", "process", "message",
                "taskName",
            ):
                log_entry[key] = value

        if record.exc_info:
            log_entry["exception"] = {
                "type": record.exc_info[0].__name__ if record.exc_info[0] else None,
                "message": str(record.exc_info[1]),
                "traceback": traceback.format_exception(*record.exc_info),
            }

        return json.dumps(log_entry)


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(StructuredFormatter())
        logger.addHandler(handler)
        logger.propagate = False

    return logger


def set_correlation_id(correlation_id: str) -> None:
    correlation_id_var.set(correlation_id)


def set_service_name(name: str) -> None:
    service_name_var.set(name)


class LogContext:
    """Context manager to temporarily set logging context variables."""

    def __init__(self, correlation_id: Optional[str] = None, service: Optional[str] = None):
        self.correlation_id = correlation_id
        self.service = service
        self._tokens = []

    def __enter__(self):
        if self.correlation_id:
            self._tokens.append(correlation_id_var.set(self.correlation_id))
        if self.service:
            self._tokens.append(service_name_var.set(self.service))
        return self

    def __exit__(self, *args):
        for token in reversed(self._tokens):
            token.var.reset(token)