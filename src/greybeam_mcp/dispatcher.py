"""Route tools/call to owned implementations or the child manager.

**Cancellation - v1 scope.** ``dispatcher.cancel(request_id)`` is defined
and tested here, but v1 does NOT wire it to any production trigger
(see Task 20's "Cancellation scope - v1" note). There is no
``notifications/cancelled`` handler registered on the MCP server, no
asyncio timeout watchdog, and no other code path that calls ``.cancel()``
outside of the unit tests in this module. The API below is scaffolding
for v1.1, where a ``notifications/cancelled`` handler will invoke it.
Keeping the shape tested now locks the contract so v1.1 is a wiring
change, not a redesign.

In-flight tracking is cleaned up via try/finally on every dispatch so
tokens/IDs do NOT accumulate over the lifetime of the server - this is
a real invariant v1 enforces even though .cancel() is not externally
driven.
"""
from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Any, Literal

from greybeam_mcp.cancel import CancelToken
from greybeam_mcp.child.manager import ChildManager
from greybeam_mcp.config import GreybeamConfig, SnowflakeConfig
from greybeam_mcp.tools.cortex_analyst import cortex_analyst
from greybeam_mcp.tools.registry import resolve
from greybeam_mcp.tools.run_snowflake_query import run_snowflake_query

log = logging.getLogger(__name__)


@dataclass
class _InFlight:
    route: Literal["owned", "delegated"]
    token: CancelToken | None  # set for owned; None for delegated


class Dispatcher:
    def __init__(
        self, *, sf: SnowflakeConfig, gb: GreybeamConfig, child: ChildManager
    ) -> None:
        self._sf = sf
        self._gb = gb
        self._child = child
        self._in_flight: dict[str, _InFlight] = {}
        # Strong refs to fire-and-forget delegated-cancel notification tasks
        # so they don't get GC'd before completion. Done callback removes the
        # ref and logs exceptions to avoid asyncio's "Task exception was never
        # retrieved" warning.
        self._fire_and_forget: set[asyncio.Task[Any]] = set()

    def in_flight_count(self) -> int:
        return len(self._in_flight)

    def cancel(self, request_id: str) -> None:
        entry = self._in_flight.get(request_id)
        if entry is None:
            return
        if entry.route == "owned" and entry.token is not None:
            entry.token.set()
            return
        # Delegated: forward the notification to the child (best-effort).
        task = asyncio.create_task(
            self._child.send_notification(
                "notifications/cancelled", {"requestId": request_id}
            )
        )
        self._fire_and_forget.add(task)
        task.add_done_callback(self._on_cancel_task_done)

    def _on_cancel_task_done(self, task: asyncio.Task[Any]) -> None:
        self._fire_and_forget.discard(task)
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            log.warning(
                "delegated_cancel_notification_failed",
                exc_info=(type(exc), exc, exc.__traceback__),
            )

    async def dispatch(
        self,
        name: str,
        arguments: dict[str, Any],
        *,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        route = resolve(name)  # raises UnknownToolError
        token = CancelToken() if (route == "owned" and request_id) else None
        if request_id:
            self._in_flight[request_id] = _InFlight(route=route, token=token)
        try:
            if route == "owned":
                return await self._dispatch_owned(name, arguments, token)
            return await self._child.call_tool(name, arguments)
        finally:
            if request_id:
                self._in_flight.pop(request_id, None)

    async def _dispatch_owned(
        self, name: str, arguments: dict[str, Any], token: CancelToken | None
    ) -> dict[str, Any]:
        if name == "run_snowflake_query":
            result = await run_snowflake_query(
                statement=arguments["statement"],
                sf=self._sf,
                gb=self._gb,
                cancel_token=token,
            )
            if result.is_error:
                return _err(result.error_message or "")
            return _ok_text(json.dumps(result.rows, default=str))

        if name == "cortex_analyst":
            ar = await cortex_analyst(
                arguments=arguments,
                sf=self._sf,
                gb=self._gb,
                cancel_token=token,
            )
            if ar.is_error:
                return _err(ar.error_message or "")
            return _ok_text(json.dumps(ar.json_payload, default=str))

        raise AssertionError(f"owned tool {name!r} has no dispatch path")


def _ok_text(text: str) -> dict[str, Any]:
    return {"isError": False, "content": [{"type": "text", "text": text}]}


def _err(message: str) -> dict[str, Any]:
    return {"isError": True, "content": [{"type": "text", "text": message}]}
