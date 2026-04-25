from unittest.mock import MagicMock, patch

import pytest

from greybeam_mcp.config import GreybeamConfig, OtherServices, RestartPolicy, SnowflakeConfig
from greybeam_mcp.tools.run_snowflake_query import (
    INPUT_SCHEMA,
    RunSnowflakeQueryInput,
    run_snowflake_query,
)


@pytest.fixture
def configs():
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
        proxy_host="greybeam.example.com",
        row_cap=10,
        byte_cap=10_000,
        query_timeout=30,
        child_restart_policy=RestartPolicy(
            max_attempts=3, backoff_seconds=[1, 4, 16], jitter=True
        ),
        cortex_search_required=True,
    )
    return sf, gb


def test_input_schema_matches_upstream():
    """Spec §5.3 step 1: schema is `statement: str` only, no Greybeam-only params."""
    assert INPUT_SCHEMA["type"] == "object"
    assert list(INPUT_SCHEMA["properties"].keys()) == ["statement"]
    assert INPUT_SCHEMA["required"] == ["statement"]


def test_input_model_rejects_empty_statement():
    with pytest.raises(ValueError):
        RunSnowflakeQueryInput(statement="")


@pytest.mark.asyncio
async def test_run_snowflake_query_returns_rows_and_passes_caps(configs):
    """Verify the wrapper offloads to _execute_sync with the right kwargs."""
    sf, gb = configs
    rows = [{"a": 1}, {"a": 2}]
    with patch(
        "greybeam_mcp.tools.run_snowflake_query.open_connection"
    ) as open_conn, patch(
        "greybeam_mcp.tools.run_snowflake_query._execute_sync", return_value=rows
    ) as exec_spy:
        open_conn.return_value.__enter__.return_value = MagicMock()
        result = await run_snowflake_query(
            statement="SELECT 1", sf=sf, gb=gb, cancel_token=None
        )

    assert result.is_error is False
    assert result.rows == rows
    kwargs = exec_spy.call_args.kwargs
    assert kwargs["statement"] == "SELECT 1"
    assert kwargs["row_cap"] == gb.row_cap
    assert kwargs["byte_cap"] == gb.byte_cap
    assert kwargs["timeout"] == gb.query_timeout


@pytest.mark.asyncio
async def test_cancelled_propagates_no_tool_result(configs):
    """Per spec §5.3 step 7: Cancelled re-raises; no ToolResult is produced."""
    from greybeam_mcp.tools.run_snowflake_query import Cancelled

    sf, gb = configs
    with patch(
        "greybeam_mcp.tools.run_snowflake_query.open_connection"
    ) as open_conn, patch(
        "greybeam_mcp.tools.run_snowflake_query._execute_sync",
        side_effect=Cancelled(),
    ):
        open_conn.return_value.__enter__.return_value = MagicMock()
        with pytest.raises(Cancelled):
            await run_snowflake_query(
                statement="SELECT 1", sf=sf, gb=gb, cancel_token=None
            )


@pytest.mark.asyncio
async def test_cap_exceeded_maps_to_tool_error(configs):
    from greybeam_mcp.tools.run_snowflake_query import CapExceededError

    sf, gb = configs
    with patch(
        "greybeam_mcp.tools.run_snowflake_query.open_connection"
    ) as open_conn, patch(
        "greybeam_mcp.tools.run_snowflake_query._execute_sync",
        side_effect=CapExceededError("row_cap", limit=10, observed=11),
    ):
        open_conn.return_value.__enter__.return_value = MagicMock()
        result = await run_snowflake_query(
            statement="SELECT *", sf=sf, gb=gb, cancel_token=None
        )

    assert result.is_error is True
    assert result.error_kind == "cap_exceeded"
    assert "row_cap" in result.error_message
    assert "Refine" in result.error_message


@pytest.mark.asyncio
async def test_arbitrary_exception_maps_to_tool_error(configs):
    sf, gb = configs
    with patch(
        "greybeam_mcp.tools.run_snowflake_query.open_connection"
    ) as open_conn, patch(
        "greybeam_mcp.tools.run_snowflake_query._execute_sync",
        side_effect=RuntimeError("oh no"),
    ):
        open_conn.return_value.__enter__.return_value = MagicMock()
        result = await run_snowflake_query(
            statement="SELECT 1", sf=sf, gb=gb, cancel_token=None
        )

    assert result.is_error is True
    assert result.error_kind == "RuntimeError"
    assert result.error_message == "oh no"
