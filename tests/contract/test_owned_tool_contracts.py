"""Contract tests pinning the owned-tool input schemas and dispatcher
envelope shapes against stored fixtures. No network or DB access.
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from greybeam_mcp.config import (
    GreybeamConfig,
    OtherServices,
    RestartPolicy,
    SnowflakeConfig,
)
from greybeam_mcp.dispatcher import Dispatcher
from greybeam_mcp.tools.cortex_analyst import (
    CORTEX_ANALYST_INPUT_SCHEMA,
    CortexAnalystResult,
)
from greybeam_mcp.tools.run_snowflake_query import (
    INPUT_SCHEMA as RUN_QUERY_INPUT_SCHEMA,
)
from greybeam_mcp.tools.run_snowflake_query import ToolResult

FIX = Path(__file__).parent / "fixtures"


def _configs() -> tuple[SnowflakeConfig, GreybeamConfig]:
    sf = SnowflakeConfig(
        account="abc",
        user="agent",
        password="pw",
        analyst_services=[],
        agent_services=[],
        other_services=OtherServices(
            query_manager=False, object_manager=False, semantic_manager=False
        ),
    )
    gb = GreybeamConfig(
        proxy_host="g.example.com",
        row_cap=10,
        byte_cap=10_000,
        query_timeout=30,
        child_restart_policy=RestartPolicy(
            max_attempts=3, backoff_seconds=[1, 4, 16], jitter=True
        ),
        cortex_search_required=True,
    )
    return sf, gb


def test_run_snowflake_query_input_schema_matches_fixture() -> None:
    fixture = json.loads((FIX / "run_snowflake_query.json").read_text())
    assert RUN_QUERY_INPUT_SCHEMA == fixture["input_schema"]


def test_cortex_analyst_input_schema_required_matches_fixture() -> None:
    fixture = json.loads((FIX / "cortex_analyst.json").read_text())
    assert CORTEX_ANALYST_INPUT_SCHEMA["required"] == fixture["input_schema_required"]


@pytest.mark.asyncio
async def test_run_snowflake_query_envelope_shape() -> None:
    sf, gb = _configs()
    fixture = json.loads((FIX / "run_snowflake_query.json").read_text())
    d = Dispatcher(sf=sf, gb=gb, child=MagicMock())
    with patch(
        "greybeam_mcp.dispatcher.run_snowflake_query",
        AsyncMock(return_value=ToolResult(is_error=False, rows=fixture["sample_rows"])),
    ):
        result = await d.dispatch("run_snowflake_query", fixture["sample_input"])
    assert result["isError"] is False
    block = result["content"][0]
    assert block["type"] == "text"
    assert json.loads(block["text"]) == fixture["sample_rows"]


@pytest.mark.asyncio
async def test_cortex_analyst_envelope_shape_with_sql() -> None:
    sf, gb = _configs()
    fixture = json.loads((FIX / "cortex_analyst.json").read_text())
    d = Dispatcher(sf=sf, gb=gb, child=MagicMock())
    payload = {
        "text": "Generated SQL:",
        "sql": "SELECT 1",
        "results": fixture["sample_rows"],
    }
    with patch(
        "greybeam_mcp.dispatcher.cortex_analyst",
        AsyncMock(return_value=CortexAnalystResult(is_error=False, json_payload=payload)),
    ):
        result = await d.dispatch("cortex_analyst", {"messages": []})
    # Pin the success envelope before decoding — without this guard a
    # regression that flips success→error surfaces as a confusing
    # JSONDecodeError/KeyError instead of an isError mismatch.
    assert result["isError"] is False
    decoded = json.loads(result["content"][0]["text"])
    assert sorted(decoded.keys()) == sorted(fixture["expected_payload_keys_when_sql_succeeds"])


@pytest.mark.asyncio
async def test_cortex_analyst_envelope_shape_text_only() -> None:
    sf, gb = _configs()
    fixture = json.loads((FIX / "cortex_analyst.json").read_text())
    d = Dispatcher(sf=sf, gb=gb, child=MagicMock())
    payload = {"text": "Just a thought."}
    with patch(
        "greybeam_mcp.dispatcher.cortex_analyst",
        AsyncMock(return_value=CortexAnalystResult(is_error=False, json_payload=payload)),
    ):
        result = await d.dispatch("cortex_analyst", {"messages": []})
    # Pin the success envelope before decoding — without this guard a
    # regression that flips success→error surfaces as a confusing
    # JSONDecodeError/KeyError instead of an isError mismatch.
    assert result["isError"] is False
    decoded = json.loads(result["content"][0]["text"])
    assert sorted(decoded.keys()) == sorted(fixture["expected_payload_keys_when_text_only"])
