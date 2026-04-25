import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from greybeam_mcp.config import (
    GreybeamConfig,
    OtherServices,
    RestartPolicy,
    SnowflakeConfig,
)
from greybeam_mcp.dispatcher import Dispatcher
from greybeam_mcp.tools.cortex_analyst import CortexAnalystResult
from greybeam_mcp.tools.run_snowflake_query import ToolResult


def _configs():
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


@pytest.mark.asyncio
async def test_dispatch_run_snowflake_query_success():
    sf, gb = _configs()
    d = Dispatcher(sf=sf, gb=gb, child=MagicMock())
    with patch(
        "greybeam_mcp.dispatcher.run_snowflake_query",
        AsyncMock(return_value=ToolResult(is_error=False, rows=[{"a": 1}])),
    ):
        result = await d.dispatch("run_snowflake_query", {"statement": "SELECT 1"})
    assert result["isError"] is False
    assert json.loads(result["content"][0]["text"]) == [{"a": 1}]


@pytest.mark.asyncio
async def test_dispatch_run_snowflake_query_error():
    sf, gb = _configs()
    d = Dispatcher(sf=sf, gb=gb, child=MagicMock())
    with patch(
        "greybeam_mcp.dispatcher.run_snowflake_query",
        AsyncMock(
            return_value=ToolResult(
                is_error=True,
                error_kind="cap_exceeded",
                error_message="row_cap exceeded",
            )
        ),
    ):
        result = await d.dispatch("run_snowflake_query", {"statement": "SELECT 1"})
    assert result["isError"] is True
    assert "row_cap" in result["content"][0]["text"]


@pytest.mark.asyncio
async def test_dispatch_cortex_analyst_success():
    sf, gb = _configs()
    d = Dispatcher(sf=sf, gb=gb, child=MagicMock())
    payload = {"text": "ok", "sql": "SELECT 1", "results": [{"a": 1}]}
    with patch(
        "greybeam_mcp.dispatcher.cortex_analyst",
        AsyncMock(return_value=CortexAnalystResult(is_error=False, json_payload=payload)),
    ):
        result = await d.dispatch("cortex_analyst", {"messages": []})
    assert result["isError"] is False
    assert json.loads(result["content"][0]["text"]) == payload


@pytest.mark.asyncio
async def test_dispatch_cortex_search_forwards_to_child():
    sf, gb = _configs()
    child = MagicMock()
    child.call_tool = AsyncMock(
        return_value={"isError": False, "content": [{"type": "text", "text": "ok"}]}
    )
    d = Dispatcher(sf=sf, gb=gb, child=child)
    result = await d.dispatch("cortex_search", {"query": "x"})
    child.call_tool.assert_awaited_once_with("cortex_search", {"query": "x"})
    assert result == {"isError": False, "content": [{"type": "text", "text": "ok"}]}


@pytest.mark.asyncio
async def test_unknown_tool_raises():
    sf, gb = _configs()
    d = Dispatcher(sf=sf, gb=gb, child=MagicMock())
    from greybeam_mcp.tools.registry import UnknownToolError

    with pytest.raises(UnknownToolError):
        await d.dispatch("nope", {})


@pytest.mark.asyncio
async def test_token_dict_drains_after_successful_dispatch():
    sf, gb = _configs()
    d = Dispatcher(sf=sf, gb=gb, child=MagicMock())
    with patch(
        "greybeam_mcp.dispatcher.run_snowflake_query",
        AsyncMock(return_value=ToolResult(is_error=False, rows=[])),
    ):
        await d.dispatch("run_snowflake_query", {"statement": "SELECT 1"}, request_id="r1")
        await d.dispatch("run_snowflake_query", {"statement": "SELECT 1"}, request_id="r2")
    assert d.in_flight_count() == 0


@pytest.mark.asyncio
async def test_token_dict_drains_after_failed_dispatch():
    sf, gb = _configs()
    d = Dispatcher(sf=sf, gb=gb, child=MagicMock())
    with patch(
        "greybeam_mcp.dispatcher.run_snowflake_query",
        AsyncMock(side_effect=RuntimeError("boom")),
    ):
        with pytest.raises(RuntimeError):
            await d.dispatch(
                "run_snowflake_query", {"statement": "SELECT 1"}, request_id="r1"
            )
    assert d.in_flight_count() == 0


@pytest.mark.asyncio
async def test_cancel_owned_call_sets_token():
    sf, gb = _configs()
    d = Dispatcher(sf=sf, gb=gb, child=MagicMock())
    captured = {}

    async def slow(**kwargs):
        captured["token"] = kwargs["cancel_token"]
        # Yield repeatedly so the test can observe in-flight state and call cancel
        # before the dispatch completes; the token must propagate by the time we
        # return.
        for _ in range(20):
            await asyncio.sleep(0)
            if kwargs["cancel_token"].is_set():
                break
        return ToolResult(is_error=False, rows=[])

    with patch("greybeam_mcp.dispatcher.run_snowflake_query", slow):
        task = asyncio.create_task(
            d.dispatch("run_snowflake_query", {"statement": "SELECT 1"}, request_id="r1")
        )
        for _ in range(10):
            await asyncio.sleep(0)
            if "token" in captured:
                break
        # Pin identity: the token forwarded to run_snowflake_query MUST be the
        # one registered in _in_flight, otherwise dispatcher.cancel(request_id)
        # would set a different token than the one the worker observes.
        registered = d._in_flight["r1"].token
        assert registered is captured["token"]
        d.cancel("r1")
        await task

    assert captured["token"].is_set()


@pytest.mark.asyncio
async def test_cancel_delegated_call_forwards_notification_to_child():
    sf, gb = _configs()
    child = MagicMock()
    child.send_notification = AsyncMock()
    d = Dispatcher(sf=sf, gb=gb, child=child)

    async def slow_call(name, args):
        await asyncio.sleep(0.05)
        return {"isError": False, "content": []}

    child.call_tool = AsyncMock(side_effect=slow_call)

    task = asyncio.create_task(d.dispatch("cortex_search", {"query": "x"}, request_id="r1"))
    for _ in range(10):
        await asyncio.sleep(0)
    d.cancel("r1")
    await task
    # cancel() schedules the child notification as a fire-and-forget task;
    # drain it explicitly so the assertion isn't racing the scheduler.
    if d._fire_and_forget:
        await asyncio.gather(*list(d._fire_and_forget))
    child.send_notification.assert_awaited_with(
        "notifications/cancelled", {"requestId": "r1"}
    )


def test_cancel_unknown_request_is_noop():
    sf, gb = _configs()
    d = Dispatcher(sf=sf, gb=gb, child=MagicMock())
    d.cancel("never-existed")  # must not raise
