"""Greybeam MCP server bootstrap.

The server runs over stdio and advertises only the `tools` capability
(with `listChanged: true`). Prompts/resources are intentionally NOT
advertised; if the child upstream MCP later starts publishing them,
they are dropped (fail-closed invariant per spec §4.4).
"""
from __future__ import annotations

from typing import Any

from greybeam_mcp import __version__


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
