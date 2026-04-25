from unittest.mock import AsyncMock, patch

import pytest

from greybeam_mcp.config import (
    GreybeamConfig,
    OtherServices,
    RestartPolicy,
    SnowflakeConfig,
)
from greybeam_mcp.tools.cortex_analyst import (
    CORTEX_ANALYST_INPUT_SCHEMA,
    cortex_analyst,
)
from greybeam_mcp.tools.run_snowflake_query import ToolResult


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


def test_input_schema_matches_upstream_shape():
    assert CORTEX_ANALYST_INPUT_SCHEMA["type"] == "object"
    assert "messages" in CORTEX_ANALYST_INPUT_SCHEMA["properties"]


@pytest.mark.asyncio
async def test_text_only_returns_text(configs):
    sf, gb = configs
    fake_client = AsyncMock()
    fake_client.send_message = AsyncMock(return_value={
        "message": {"content": [{"type": "text", "text": "Hi"}]}
    })

    with patch(
        "greybeam_mcp.tools.cortex_analyst.CortexAnalystClient",
        return_value=fake_client,
    ):
        payload = await cortex_analyst(
            arguments={"messages": []},
            sf=sf,
            gb=gb,
            cancel_token=None,
        )
    assert payload.is_error is False
    assert payload.json_payload["text"] == "Hi"
    assert payload.json_payload.get("sql") is None
    assert payload.json_payload.get("results") is None


@pytest.mark.asyncio
async def test_sql_block_executed_and_results_returned(configs):
    sf, gb = configs
    fake_client = AsyncMock()
    fake_client.send_message = AsyncMock(return_value={
        "message": {
            "content": [
                {"type": "text", "text": "Generated SQL:"},
                {"type": "sql", "statement": "SELECT 1"},
            ]
        }
    })

    with patch(
        "greybeam_mcp.tools.cortex_analyst.CortexAnalystClient",
        return_value=fake_client,
    ), patch(
        "greybeam_mcp.tools.cortex_analyst.run_snowflake_query",
        AsyncMock(return_value=ToolResult(is_error=False, rows=[{"a": 1}])),
    ):
        payload = await cortex_analyst(
            arguments={"messages": []}, sf=sf, gb=gb, cancel_token=None
        )

    assert payload.is_error is False
    assert payload.json_payload["sql"] == "SELECT 1"
    assert payload.json_payload["results"] == [{"a": 1}]


@pytest.mark.asyncio
async def test_internal_sql_failure_fails_whole_tool(configs):
    sf, gb = configs
    fake_client = AsyncMock()
    fake_client.send_message = AsyncMock(return_value={
        "message": {
            "content": [
                {"type": "text", "text": "Generated SQL:"},
                {"type": "sql", "statement": "SELECT 1"},
            ]
        }
    })

    with patch(
        "greybeam_mcp.tools.cortex_analyst.CortexAnalystClient",
        return_value=fake_client,
    ), patch(
        "greybeam_mcp.tools.cortex_analyst.run_snowflake_query",
        AsyncMock(
            return_value=ToolResult(
                is_error=True,
                error_kind="cap_exceeded",
                error_message="row_cap exceeded",
            )
        ),
    ):
        payload = await cortex_analyst(
            arguments={"messages": []}, sf=sf, gb=gb, cancel_token=None
        )

    # Whole-tool failure per spec §5.4 step 4 — no partial {text, sql} payload.
    assert payload.is_error is True
    assert payload.error_kind == "cortex_analyst_sql_failed"
    assert "row_cap" in payload.error_message
    assert payload.json_payload is None


@pytest.mark.asyncio
async def test_rest_failure_returns_cortex_analyst_api_error(configs):
    """Cortex Analyst REST exception -> wrapped as cortex_analyst_api with no payload."""
    sf, gb = configs
    fake_client = AsyncMock()
    fake_client.send_message = AsyncMock(
        side_effect=RuntimeError("Cortex Analyst returned 401: bad creds")
    )

    with patch(
        "greybeam_mcp.tools.cortex_analyst.CortexAnalystClient",
        return_value=fake_client,
    ):
        result = await cortex_analyst(
            arguments={"messages": []}, sf=sf, gb=gb, cancel_token=None
        )

    assert result.is_error is True
    assert result.error_kind == "cortex_analyst_api"
    assert "401" in result.error_message
    assert result.json_payload is None


@pytest.mark.asyncio
async def test_password_secretstr_is_unwrapped_for_client(configs):
    """SnowflakeConfig.password is SecretStr; the client must receive a plain str.

    Pins the SecretStr unwrap so a regression that passes the SecretStr
    object directly (which renders as '**********' in the auth header)
    would fail this test instead of producing a confusing 401 in prod.
    """
    sf, gb = configs
    fake_client = AsyncMock()
    fake_client.send_message = AsyncMock(return_value={
        "message": {"content": [{"type": "text", "text": "ok"}]}
    })

    with patch(
        "greybeam_mcp.tools.cortex_analyst.CortexAnalystClient",
        return_value=fake_client,
    ) as ctor:
        await cortex_analyst(
            arguments={"messages": []}, sf=sf, gb=gb, cancel_token=None
        )

    kwargs = ctor.call_args.kwargs
    assert kwargs["password"] == "pw"  # plain string, NOT SecretStr('**********')
    assert kwargs["account"] == "abc"
    assert kwargs["user"] == "agent"
