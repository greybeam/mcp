"""Bounded-restart manager for the upstream Snowflake MCP child process.

Owns lifecycle of a single ``ChildMcpClient``: bounded-retry startup,
runtime crash detection on ``call_tool``, fire-and-forget recovery, and
state-change notifications used downstream to drive
``notifications/tools/list_changed``.

State machine
-------------
- ``STOPPED``: no running client (initial state, after teardown, or
  immediately following a runtime crash before recovery completes).
- ``RUNNING``: client is healthy; ``tools`` reflects the latest
  ``list_tools`` result and ``call_tool`` is permitted.
- ``DEGRADED``: startup or recovery exhausted retries with
  ``cortex_search_required=False``; ``call_tool`` is rejected and the
  delegated tool list is empty.

The state-change callback is the source-of-truth event for the eventual
server wiring to emit ``notifications/tools/list_changed`` — recovery
deliberately does NOT inline that notification here.
"""
from __future__ import annotations

import asyncio
import enum
import logging
import random
from typing import Any, Awaitable, Callable

from greybeam_mcp.child.client import ChildMcpClient
from greybeam_mcp.config import RestartPolicy

log = logging.getLogger(__name__)


class ChildState(enum.Enum):
    STOPPED = "stopped"
    RUNNING = "running"
    DEGRADED = "degraded"


class ChildManager:
    def __init__(
        self,
        *,
        client_factory: Callable[[], ChildMcpClient],
        restart_policy: RestartPolicy,
        cortex_search_required: bool,
        on_state_change: Callable[[ChildState], Awaitable[None]] | None = None,
    ) -> None:
        self._factory = client_factory
        self._policy = restart_policy
        self._cortex_search_required = cortex_search_required
        self._on_state_change = on_state_change

        self._client: ChildMcpClient | None = None
        self._recovery_task: asyncio.Task[None] | None = None
        self._recovery_lock = asyncio.Lock()

        self.state: ChildState = ChildState.STOPPED
        self.tools: list[dict[str, Any]] = []

    async def start(self) -> None:
        """Bounded-retry startup. Honors ``cortex_search_required`` semantics."""
        last_exc: BaseException | None = None
        for attempt in range(self._policy.max_attempts):
            try:
                client, tools = await self._try_start_once()
            except Exception as exc:
                last_exc = exc
                log.warning(
                    "child startup attempt %d/%d failed: %s",
                    attempt + 1,
                    self._policy.max_attempts,
                    exc,
                )
                await self._sleep_backoff(attempt)
                continue
            self._client = client
            self.tools = tools
            await self._set_state(ChildState.RUNNING)
            return

        # All attempts exhausted.
        if self._cortex_search_required:
            assert last_exc is not None
            raise last_exc
        await self._set_state(ChildState.DEGRADED)

    async def _try_start_once(self) -> tuple[ChildMcpClient, list[dict[str, Any]]]:
        client = self._factory()
        await client.start()
        tools = await client.list_tools()
        if self._cortex_search_required and not any(
            t.get("name") == "cortex_search" for t in tools
        ):
            await client.stop()
            raise RuntimeError("cortex_search not advertised by child")
        return client, tools

    async def _sleep_backoff(self, attempt: int) -> None:
        idx = min(attempt, len(self._policy.backoff_seconds) - 1)
        seconds = self._policy.backoff_seconds[idx]
        if self._policy.jitter and seconds > 0:
            seconds = seconds * (0.5 + random.random())
        await asyncio.sleep(seconds)

    async def call_tool(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        if self.state != ChildState.RUNNING or self._client is None:
            raise RuntimeError(f"child not available (state={self.state.value})")
        client = self._client
        try:
            return await client.call_tool(name, arguments)
        except Exception:
            log.warning(
                "child call_tool(%s) failed; tearing down and scheduling recovery",
                name,
                exc_info=True,
            )
            # Order is non-negotiable: state -> teardown -> schedule recovery.
            self._client = None
            self.tools = []
            await self._set_state(ChildState.STOPPED)
            # client.stop() is idempotent and only re-raises BaseException
            # (CancelledError/KeyboardInterrupt/SystemExit) — no try/except
            # around it for plain Exception.
            await client.stop()
            if self._recovery_task is None or self._recovery_task.done():
                self._recovery_task = asyncio.create_task(self._recover())
            raise

    async def _recover(self) -> None:
        async with self._recovery_lock:
            if self.state == ChildState.RUNNING:
                # Someone else recovered while we waited on the lock.
                return
            for attempt in range(self._policy.max_attempts):
                try:
                    client, tools = await self._try_start_once()
                except Exception as exc:
                    log.warning(
                        "child recovery attempt %d/%d failed: %s",
                        attempt + 1,
                        self._policy.max_attempts,
                        exc,
                    )
                    await self._sleep_backoff(attempt)
                    continue
                self._client = client
                self.tools = tools
                await self._set_state(ChildState.RUNNING)
                return
            await self._set_state(ChildState.DEGRADED)

    async def send_notification(self, method: str, params: dict[str, Any]) -> None:
        if self.state != ChildState.RUNNING or self._client is None:
            return
        try:
            await self._client.send_notification(method, params)
        except Exception:
            log.warning(
                "child send_notification(%s) failed (best-effort)",
                method,
                exc_info=True,
            )

    async def stop(self) -> None:
        task = self._recovery_task
        self._recovery_task = None
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        if self._client is not None:
            client = self._client
            self._client = None
            # client.stop() is idempotent; no try/except needed here.
            await client.stop()
        self.tools = []
        await self._set_state(ChildState.STOPPED)

    async def _set_state(self, state: ChildState) -> None:
        self.state = state
        if self._on_state_change is not None:
            await self._on_state_change(state)
