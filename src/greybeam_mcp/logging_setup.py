"""Structured JSON logging on stderr (stdout reserved for MCP JSON-RPC)."""
from __future__ import annotations

import json
import logging
import sys
from typing import IO, Any, Literal

_RESERVED = {
    "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
    "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
    "created", "msecs", "relativeCreated", "thread", "threadName",
    "processName", "process", "message",
}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _RESERVED:
                payload[key] = value
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def setup_logging(stream: IO[str] | None = None, level: str = "INFO") -> None:
    handler = logging.StreamHandler(stream or sys.stderr)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)


def tool_call_log(
    *,
    request_id: str,
    tool_name: str,
    route: Literal["greybeam", "child"],
    latency_ms: int,
    outcome: Literal["ok", "tool_error", "cancelled", "crash"],
    cancelled: bool,
    rows_returned: int | None = None,
    child_pid: int | None = None,
    error_kind: str | None = None,
    error_code: str | None = None,
) -> dict[str, Any]:
    """Per-spec log schema (§7.5)."""
    return {
        "request_id": request_id,
        "tool_name": tool_name,
        "route": route,
        "child_pid": child_pid,
        "latency_ms": latency_ms,
        "outcome": outcome,
        "cancelled": cancelled,
        "rows_returned": rows_returned,
        "error_kind": error_kind,
        "error_code": error_code,
    }
