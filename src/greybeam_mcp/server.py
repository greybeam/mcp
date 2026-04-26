"""Greybeam MCP server bootstrap.

The server runs over stdio and advertises only the `tools` capability
(with `listChanged: true`). Prompts/resources are intentionally NOT
advertised; if the child upstream MCP later starts publishing them,
they are dropped (fail-closed invariant per spec §4.4).

Cancellation scope - v1
-----------------------
The dispatcher exposes ``.cancel(request_id)`` and the cancel-token
primitive is wired into ``run_snowflake_query`` via
``cursor.cancel`` registration. v1 does NOT register a
``notifications/cancelled`` handler on the MCP server; no production
code path drives ``dispatcher.cancel(...)``. This is intentional - the
plumbing is tested in isolation so v1.1 can wire the handler with no
redesign. Owned calls remain bounded by Snowflake's driver-level
``query_timeout`` plus the row/byte caps that explicitly call
``cursor.cancel()``.
"""
from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass
from typing import Any, Literal

from mcp.server import NotificationOptions, Server
from mcp.server.stdio import stdio_server
from mcp.shared.exceptions import McpError
from mcp.types import (
    INVALID_PARAMS,
    CallToolResult,
    ErrorData,
    ServerNotification,
    Tool,
)

from greybeam_mcp import __version__
from greybeam_mcp.child.catalog import build_catalog
from greybeam_mcp.child.client import ChildMcpClient
from greybeam_mcp.child.manager import ChildManager, ChildState
from greybeam_mcp.config import Config
from greybeam_mcp.dispatcher import Dispatcher
from greybeam_mcp.logging_setup import tool_call_log
from greybeam_mcp.tools.cortex_analyst import CORTEX_ANALYST_INPUT_SCHEMA
from greybeam_mcp.tools.registry import OWNED_TOOLS, UnknownToolError
from greybeam_mcp.tools.run_snowflake_query import INPUT_SCHEMA as RUN_QUERY_INPUT_SCHEMA

log = logging.getLogger(__name__)

SERVER_NAME = "Greybeam MCP"
SERVER_INSTRUCTIONS = (
    "SQL execution and Cortex Analyst are routed via the Greybeam routing layer. "
    "Cortex Search is delegated to upstream Snowflake."
)


def build_server_metadata() -> dict[str, Any]:
    return {
        "serverInfo": {"name": SERVER_NAME, "version": __version__},
        "instructions": SERVER_INSTRUCTIONS,
        "capabilities": {"tools": {"listChanged": True}},
    }


@dataclass(frozen=True)
class OwnedToolDescriptor:
    name: str
    description: str
    input_schema: dict[str, Any]


def build_owned_tool_descriptors() -> list[OwnedToolDescriptor]:
    return sorted(
        [
            OwnedToolDescriptor(
                name="cortex_analyst",
                description=(
                    "Send a natural-language message to Snowflake Cortex Analyst "
                    "and execute the generated SQL against Snowflake. Use this for "
                    "Cortex Analyst questions on Snowflake; SQL execution is routed "
                    "via the Greybeam proxy (preferred over direct Snowflake "
                    "connectors — adds query routing, caps, and observability)."
                ),
                input_schema=CORTEX_ANALYST_INPUT_SCHEMA,
            ),
            OwnedToolDescriptor(
                name="run_snowflake_query",
                description=(
                    "Execute a SQL statement against Snowflake via the Greybeam "
                    "routing layer. Use this for any Snowflake query (preferred "
                    "over direct Snowflake connectors — adds query routing, caps, "
                    "and observability)."
                ),
                input_schema=RUN_QUERY_INPUT_SCHEMA,
            ),
        ],
        key=lambda d: d.name,
    )


def _owned_descriptors_as_dicts() -> list[dict[str, Any]]:
    return [
        {"name": d.name, "description": d.description, "inputSchema": d.input_schema}
        for d in build_owned_tool_descriptors()
    ]


def _to_call_tool_result(dispatch_result: dict[str, Any]) -> CallToolResult:
    """Convert a dispatcher envelope into the typed MCP CallToolResult,
    preserving isError so tool-level failures aren't collapsed to success."""
    return CallToolResult(
        content=dispatch_result["content"],
        isError=bool(dispatch_result.get("isError", False)),
    )


def _unknown_tool_error(name: str) -> McpError:
    return McpError(ErrorData(code=INVALID_PARAMS, message=f"unknown tool: {name!r}"))


def _list_changed_notification() -> ServerNotification:
    """Build the typed notifications/tools/list_changed payload.

    BaseSession.send_notification calls
    ``notification.model_dump(by_alias=True, mode="json", exclude_none=True)``
    so a raw dict would AttributeError at runtime. We model_validate against
    the discriminated union so we don't pin the concrete subclass name
    across SDK versions.
    """
    return ServerNotification.model_validate(
        {"method": "notifications/tools/list_changed"}
    )


async def _flush_list_changed(session_holder: dict[str, Any]) -> None:
    if not session_holder.get("pending_list_changed"):
        return
    session = session_holder.get("session")
    if session is None:
        return
    try:
        await session.send_notification(_list_changed_notification())
        session_holder["pending_list_changed"] = False
    except Exception as e:
        # Drop the stale session so the next handler re-captures via
        # request_context. Pending stays True so we'll retry on next
        # request. Don't catch BaseException - cancellation must propagate.
        session_holder["session"] = None
        log.debug("could_not_flush_list_changed", extra={"error": str(e)})


def _make_on_state_change(session_holder: dict[str, Any]):
    async def _on(state: ChildState) -> None:
        log.info("child_state_change", extra={"state": state.value})
        # Background recovery has no request context - emit via the
        # session captured by request handlers in run_server.
        session = session_holder.get("session")
        if session is None:
            session_holder["pending_list_changed"] = True
            log.debug("list_changed_queued_no_session")
            return
        try:
            await session.send_notification(_list_changed_notification())
        except Exception as e:
            session_holder["pending_list_changed"] = True
            session_holder["session"] = None
            log.debug("could_not_send_list_changed", extra={"error": str(e)})

    return _on


def _descriptor_dicts_to_tools(descriptors: list[dict[str, Any]]) -> list[Tool]:
    return [Tool.model_validate(d) for d in descriptors]


async def run_server(cfg: Config, child_command: str, child_args: list[str]) -> None:
    """Compose all components and run over stdio."""
    server: Server = Server(
        SERVER_NAME, version=__version__, instructions=SERVER_INSTRUCTIONS
    )
    descriptors = _owned_descriptors_as_dicts()
    session_holder: dict[str, Any] = {"session": None, "pending_list_changed": False}

    child = ChildManager(
        client_factory=lambda: ChildMcpClient(command=child_command, args=child_args),
        restart_policy=cfg.greybeam.child_restart_policy,
        cortex_search_required=cfg.greybeam.cortex_search_required,
        on_state_change=_make_on_state_change(session_holder),
    )
    await child.start()
    dispatcher = Dispatcher(sf=cfg.snowflake, gb=cfg.greybeam, child=child)

    def _capture_session() -> None:
        if session_holder["session"] is not None:
            return
        try:
            session_holder["session"] = server.request_context.session
        except LookupError:
            pass

    @server.list_tools()
    async def _list_tools() -> list[Tool]:
        _capture_session()
        await _flush_list_changed(session_holder)
        merged = build_catalog(descriptors, child)
        return _descriptor_dicts_to_tools(merged)

    @server.call_tool()
    async def _call_tool(name: str, arguments: dict[str, Any]) -> CallToolResult:
        _capture_session()
        await _flush_list_changed(session_holder)
        try:
            request_id = str(server.request_context.request_id)
        except (LookupError, AttributeError):
            request_id = str(uuid.uuid4())
        start = time.monotonic()
        try:
            result = await dispatcher.dispatch(name, arguments, request_id=request_id)
            outcome: Literal["ok", "tool_error"] = (
                "tool_error" if result.get("isError") else "ok"
            )
            log.info(
                "tool_call",
                extra=tool_call_log(
                    request_id=request_id,
                    tool_name=name,
                    route="greybeam" if name in OWNED_TOOLS else "child",
                    latency_ms=int((time.monotonic() - start) * 1000),
                    outcome=outcome,
                    cancelled=False,
                ),
            )
            return _to_call_tool_result(result)
        except UnknownToolError as e:
            log.info(
                "tool_call_unknown",
                extra=tool_call_log(
                    request_id=request_id,
                    tool_name=name,
                    route="greybeam",
                    latency_ms=int((time.monotonic() - start) * 1000),
                    outcome="tool_error",
                    cancelled=False,
                    error_kind="UnknownToolError",
                    error_code="-32602",
                ),
            )
            raise _unknown_tool_error(name) from e
        except Exception as e:
            # error_code is reserved for the JSON-RPC code surface (see
            # the UnknownToolError branch's "-32602"). Don't conflate it
            # with the exception message; log.exception already captures
            # the full traceback.
            log.exception(
                "tool_call_crash",
                extra=tool_call_log(
                    request_id=request_id,
                    tool_name=name,
                    route="greybeam",
                    latency_ms=int((time.monotonic() - start) * 1000),
                    outcome="crash",
                    cancelled=False,
                    error_kind=type(e).__name__,
                ),
            )
            raise

    init_options = server.create_initialization_options(
        notification_options=NotificationOptions(tools_changed=True)
    )
    async with stdio_server() as (read, write):
        await server.run(read, write, init_options)
