from __future__ import annotations

from unittest.mock import MagicMock

from greybeam_mcp.child.catalog import build_catalog
from greybeam_mcp.child.manager import ChildState


def _owned_tools_stub() -> list[dict]:
    return [
        {"name": "run_snowflake_query", "description": "x", "inputSchema": {}},
        {"name": "cortex_analyst", "description": "y", "inputSchema": {}},
    ]


def test_running_includes_filtered_delegated() -> None:
    mgr = MagicMock(
        state=ChildState.RUNNING,
        tools=[
            {"name": "cortex_search", "description": "z", "inputSchema": {}},
            {"name": "rogue", "description": "no", "inputSchema": {}},
        ],
    )
    catalog = build_catalog(_owned_tools_stub(), mgr)
    names = [t["name"] for t in catalog]
    assert names == ["cortex_analyst", "run_snowflake_query", "cortex_search"]


def test_degraded_drops_delegated() -> None:
    mgr = MagicMock(state=ChildState.DEGRADED, tools=[])
    catalog = build_catalog(_owned_tools_stub(), mgr)
    names = [t["name"] for t in catalog]
    assert names == ["cortex_analyst", "run_snowflake_query"]


def test_stopped_drops_delegated() -> None:
    mgr = MagicMock(state=ChildState.STOPPED, tools=[])
    catalog = build_catalog(_owned_tools_stub(), mgr)
    names = [t["name"] for t in catalog]
    assert names == ["cortex_analyst", "run_snowflake_query"]
