"""Greybeam-owned `run_snowflake_query` tool.

Per spec §5.3 / §7.2:
  - blocking DB work runs on a worker thread via asyncio.to_thread (Task 9)
  - `cursor.execute(timeout=gb.query_timeout)` bounds wall-clock per call;
    `cursor.cancel()` is called explicitly on row_cap / byte_cap exceedance
  - a `CancelToken` is registered with `cursor.cancel` so a future
    `notifications/cancelled`-driven `token.set()` would interrupt the worker.
    v1 does NOT wire that trigger (see Task 20's "Cancellation scope — v1");
    the registration is scaffolding so v1.1 is a pure wiring change.
  - row_cap / byte_cap are enforced incrementally during streaming fetch
  - no local SQL parsing; Greybeam backend is the policy authority
"""
from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from typing import Any

from pydantic import BaseModel, field_validator

from greybeam_mcp.cancel import CancelToken
from greybeam_mcp.config import GreybeamConfig, SnowflakeConfig
from greybeam_mcp.greybeam.connection import open_connection


INPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "statement": {
            "type": "string",
            "description": "SQL statement to execute via the Greybeam proxy.",
        }
    },
    "required": ["statement"],
    "additionalProperties": False,
}


class CapExceededError(Exception):
    """Raised when row or byte cap is exceeded mid-fetch."""

    def __init__(self, kind: str, limit: int, observed: int):
        self.kind = kind
        self.limit = limit
        self.observed = observed
        super().__init__(
            f"{kind} exceeded: observed {observed} > limit {limit}"
        )


class Cancelled(Exception):
    """Raised when the cancel token was set mid-fetch."""


class RunSnowflakeQueryInput(BaseModel):
    statement: str

    @field_validator("statement")
    @classmethod
    def non_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("statement must be non-empty")
        return v


@dataclass
class ToolResult:
    is_error: bool
    rows: list[dict[str, Any]] = field(default_factory=list)
    error_kind: str | None = None
    error_message: str | None = None


def _est_bytes(rows: list[dict[str, Any]]) -> int:
    """Cheap upper-bound estimate of serialized JSON size."""
    return sum(len(json.dumps(r, default=str)) for r in rows)


def _execute_sync(
    *,
    conn: Any,
    statement: str,
    row_cap: int,
    byte_cap: int,
    timeout: int,
    cancel_token: CancelToken,
    batch_size: int = 1000,
) -> list[dict[str, Any]]:
    """Streaming fetch with cap and cancellation enforcement.

    Runs on a worker thread (caller wraps in asyncio.to_thread).
    Memory is bounded to roughly row_cap plus one batch.

    Cancellation: `cursor.cancel` is registered with the token so the
    dispatcher (event loop) thread can interrupt a blocking `cursor.execute`
    or `cursor.fetchmany` by calling `token.set()`. The next batch boundary
    then sees the flag and exits cleanly with `Cancelled`.
    """
    with conn.cursor(_dict_cursor=True) as cursor:
        cancel_token.register_cancel(cursor.cancel)
        cursor.execute(statement, timeout=timeout)
        results: list[dict[str, Any]] = []
        total_bytes = 0
        while True:
            if cancel_token.is_set():
                # cursor.cancel was already fired from set() in the dispatcher thread.
                raise Cancelled()
            batch = cursor.fetchmany(batch_size)
            if not batch:
                return results
            results.extend(batch)
            total_bytes += _est_bytes(batch)
            if len(results) > row_cap:
                cursor.cancel()
                raise CapExceededError("row_cap", row_cap, len(results))
            if total_bytes > byte_cap:
                cursor.cancel()
                raise CapExceededError("byte_cap", byte_cap, total_bytes)


async def run_snowflake_query(
    *,
    statement: str,
    sf: SnowflakeConfig,
    gb: GreybeamConfig,
    cancel_token: CancelToken | None = None,
) -> ToolResult:
    """Async entrypoint. Validates input, opens connection, offloads sync work.

    Cancellation is delivered via the cancel_token from the dispatcher thread.
    """
    RunSnowflakeQueryInput(statement=statement)  # validation
    token = cancel_token or CancelToken()

    def _run() -> list[dict[str, Any]]:
        with open_connection(sf, gb) as conn:
            return _execute_sync(
                conn=conn,
                statement=statement,
                row_cap=gb.row_cap,
                byte_cap=gb.byte_cap,
                timeout=gb.query_timeout,
                cancel_token=token,
                batch_size=1000,
            )

    try:
        rows = await asyncio.to_thread(_run)
    except Cancelled:
        # Per spec §5.3 step 7: cancellation produces NO tool result.
        # Re-raise so the server can suppress the response per MCP spec.
        raise
    except CapExceededError as e:
        return ToolResult(
            is_error=True,
            error_kind="cap_exceeded",
            error_message=(
                f"Result {e.kind} exceeded ({e.observed} > {e.limit}). "
                "Refine the query (LIMIT, WHERE, narrower SELECT)."
            ),
        )
    except Exception as e:
        return ToolResult(
            is_error=True,
            error_kind=type(e).__name__,
            error_message=str(e),
        )
    return ToolResult(is_error=False, rows=rows)
