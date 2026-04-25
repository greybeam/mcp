"""Tool registry: owned vs delegated routing per spec §5.2."""
from __future__ import annotations

from typing import Any, Literal

OWNED_TOOLS: frozenset[str] = frozenset({"run_snowflake_query", "cortex_analyst"})
DELEGATED_TOOLS: frozenset[str] = frozenset({"cortex_search"})


class UnknownToolError(Exception):
    """Raised when a tool name is neither owned nor in the expected delegated set."""


def resolve(name: str) -> Literal["owned", "delegated"]:
    if name in OWNED_TOOLS:
        return "owned"
    if name in DELEGATED_TOOLS:
        return "delegated"
    raise UnknownToolError(f"unknown tool: {name!r}")


def merge_tool_lists(
    owned: list[dict[str, Any]], delegated: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Per spec §5.2: owned tools alphabetical first, then delegated alphabetical.

    Delegated tools not in DELEGATED_TOOLS are filtered out (fail-closed).
    """
    owned_sorted = sorted(owned, key=lambda t: t["name"])
    delegated_filtered = [t for t in delegated if t["name"] in DELEGATED_TOOLS]
    delegated_sorted = sorted(delegated_filtered, key=lambda t: t["name"])
    return [*owned_sorted, *delegated_sorted]
