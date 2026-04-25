"""Build the merged tools/list payload from owned + delegated sources."""
from __future__ import annotations

from typing import Any

from greybeam_mcp.child.manager import ChildManager, ChildState
from greybeam_mcp.tools.registry import merge_tool_lists


def build_catalog(
    owned_tools: list[dict[str, Any]], child: ChildManager
) -> list[dict[str, Any]]:
    """Return owned tools alphabetically, followed by delegated tools when the child is RUNNING.

    Delegated tools are dropped entirely while the manager is STOPPED or
    DEGRADED — the merge filter in ``merge_tool_lists`` then enforces the
    fail-closed allowlist on whatever the child reported.
    """
    delegated = child.tools if child.state == ChildState.RUNNING else []
    return merge_tool_lists(owned_tools, delegated)
