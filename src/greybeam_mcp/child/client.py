"""Async stdio client for the upstream Snowflake MCP child process.

Wraps the mcp SDK's ``stdio_client`` and ``ClientSession`` behind a stable
public surface (``start``/``list_tools``/``call_tool``/``send_notification``/
``is_alive``/``stop``) so the dispatcher and child manager (later commits)
compile against this module unchanged when the SDK shifts under us.

``send_notification`` accepts a JSON-RPC ``method``/``params`` pair and
coerces it into the SDK's typed ``ClientNotification`` discriminated-union
before forwarding to the session — that's what ``BaseSession`` expects.
"""
from __future__ import annotations

from contextlib import AsyncExitStack
from typing import Any

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.types import ClientNotification


class ChildMcpClient:
    def __init__(
        self,
        command: str,
        args: list[str],
        env: dict[str, str] | None = None,
    ) -> None:
        self._command = command
        self._args = args
        self._env = env
        self._exit_stack: AsyncExitStack | None = None
        self._session: ClientSession | None = None

    async def start(self) -> None:
        params = StdioServerParameters(
            command=self._command, args=self._args, env=self._env
        )
        stack = AsyncExitStack()
        read, write = await stack.enter_async_context(stdio_client(params))
        session = await stack.enter_async_context(ClientSession(read, write))
        await session.initialize()
        self._exit_stack = stack
        self._session = session

    async def list_tools(self) -> list[dict[str, Any]]:
        assert self._session is not None, "ChildMcpClient.start() must be awaited first"
        result = await self._session.list_tools()
        return [t.model_dump() for t in result.tools]

    async def call_tool(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        assert self._session is not None, "ChildMcpClient.start() must be awaited first"
        result = await self._session.call_tool(name, arguments=arguments)
        return result.model_dump()

    async def send_notification(self, method: str, params: dict[str, Any]) -> None:
        """Forward a JSON-RPC notification to the child.

        The mcp SDK's ``BaseSession.send_notification`` requires a typed
        ``ClientNotification`` (a Pydantic discriminated union over the known
        client-to-server notification methods, e.g. ``notifications/cancelled``).
        We coerce the raw ``method``/``params`` dict via ``model_validate``
        rather than constructing a specific typed notification — that keeps
        this wrapper agnostic to which notification the dispatcher sends.
        """
        assert self._session is not None, "ChildMcpClient.start() must be awaited first"
        notification = ClientNotification.model_validate(
            {"method": method, "params": params}
        )
        await self._session.send_notification(notification)

    def is_alive(self) -> bool:
        return self._session is not None

    async def stop(self) -> None:
        if self._exit_stack is not None:
            await self._exit_stack.aclose()
        self._exit_stack = None
        self._session = None
