"""Snapshot test for the child Snowflake MCP's tools/list output.

Spawns the actual upstream child (snowflake-labs-mcp 1.4.1, import name
mcp_server_snowflake) and compares the filtered tools/list against the
snapshot. Gated behind GREYBEAM_RUN_CHILD_CONTRACT=1 because it requires
the upstream package to be installed AND the child to be runnable
end-to-end. Re-run and re-approve the snapshot whenever the upstream
pin is bumped.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

import pytest

from greybeam_mcp.child.client import ChildMcpClient
from greybeam_mcp.child.config_writer import write_child_config
from greybeam_mcp.config import (
    CortexSearchService,
    OtherServices,
    SnowflakeConfig,
)

FIX = Path(__file__).parent / "fixtures"

pytestmark = pytest.mark.skipif(
    os.environ.get("GREYBEAM_RUN_CHILD_CONTRACT") != "1",
    reason="set GREYBEAM_RUN_CHILD_CONTRACT=1 to run child contract tests",
)


def _filter(tool: dict[str, Any]) -> dict[str, Any]:
    schema = tool.get("inputSchema") or tool.get("input_schema") or {}
    return {
        "name": tool["name"],
        "input_schema_required_fields": sorted(schema.get("required") or []),
    }


@pytest.mark.asyncio
async def test_child_tools_list_matches_snapshot(tmp_path: Path) -> None:
    sf = SnowflakeConfig(
        account=os.environ.get("SNOWFLAKE_ACCOUNT", "test"),
        user=os.environ.get("SNOWFLAKE_USER", "test"),
        password=os.environ.get("SNOWFLAKE_PASSWORD", "test"),
        search_services=[
            CortexSearchService(
                service_name="docs",
                description="Docs",
                database_name="DOCS",
                schema_name="PUBLIC",
            )
        ],
        analyst_services=[],
        agent_services=[],
        other_services=OtherServices(
            query_manager=False, object_manager=False, semantic_manager=False
        ),
    )
    child_yaml = tmp_path / "child.yaml"
    write_child_config(sf, child_yaml)

    client = ChildMcpClient(
        command=sys.executable,
        args=[
            "-m",
            "mcp_server_snowflake",
            "--service-config-file",
            str(child_yaml),
        ],
    )
    await client.start()
    try:
        tools = await client.list_tools()
    finally:
        await client.stop()

    actual = {
        "tools": sorted([_filter(t) for t in tools], key=lambda t: t["name"])
    }
    expected = json.loads((FIX / "child_tools_list.json").read_text())
    assert actual == expected, (
        "Child tools/list shape drift detected. Re-approve fixture if intentional."
    )
