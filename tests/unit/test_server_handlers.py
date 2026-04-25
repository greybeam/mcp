from unittest.mock import AsyncMock

import pytest

from greybeam_mcp.server import (
    OwnedToolDescriptor,
    _flush_list_changed,
    _to_call_tool_result,
    _unknown_tool_error,
    build_owned_tool_descriptors,
)


def test_owned_tool_descriptors_have_run_snowflake_query_and_cortex_analyst():
    descriptors = build_owned_tool_descriptors()
    names = [d.name for d in descriptors]
    assert names == sorted(names)
    assert "run_snowflake_query" in names
    assert "cortex_analyst" in names
    for d in descriptors:
        assert isinstance(d, OwnedToolDescriptor)
        assert d.input_schema["type"] == "object"


def test_to_call_tool_result_preserves_is_error_true():
    envelope = {"isError": True, "content": [{"type": "text", "text": "boom"}]}
    out = _to_call_tool_result(envelope)
    assert out.isError is True
    assert [
        c.model_dump(by_alias=True, exclude_none=True) for c in out.content
    ] == envelope["content"]


def test_to_call_tool_result_preserves_is_error_false():
    envelope = {"isError": False, "content": [{"type": "text", "text": "ok"}]}
    out = _to_call_tool_result(envelope)
    assert out.isError is False
    assert [
        c.model_dump(by_alias=True, exclude_none=True) for c in out.content
    ] == envelope["content"]


def test_to_call_tool_result_defaults_is_error_to_false_when_missing():
    envelope = {"content": []}
    out = _to_call_tool_result(envelope)
    assert out.isError is False


def test_unknown_tool_error_maps_to_invalid_params():
    from mcp.shared.exceptions import McpError
    from mcp.types import INVALID_PARAMS

    err = _unknown_tool_error("nope")
    assert isinstance(err, McpError)
    assert err.error.code == INVALID_PARAMS
    assert "nope" in err.error.message


@pytest.mark.asyncio
async def test_flush_list_changed_delivers_when_session_captured():
    session = AsyncMock()
    holder = {"session": session, "pending_list_changed": True}
    await _flush_list_changed(holder)
    session.send_notification.assert_awaited_once()
    assert holder["pending_list_changed"] is False
    assert holder["session"] is session  # kept on success
    # Extract the method from whatever was sent (typed obj or dict).
    sent = session.send_notification.await_args.args[0]
    method = (
        sent.root.method
        if hasattr(sent, "root")
        else sent.method
        if hasattr(sent, "method")
        else sent["method"]
    )
    assert method == "notifications/tools/list_changed"


@pytest.mark.asyncio
async def test_flush_list_changed_is_noop_when_nothing_pending():
    session = AsyncMock()
    holder = {"session": session, "pending_list_changed": False}
    await _flush_list_changed(holder)
    session.send_notification.assert_not_awaited()


@pytest.mark.asyncio
async def test_flush_list_changed_is_noop_when_no_session_yet():
    holder = {"session": None, "pending_list_changed": True}
    await _flush_list_changed(holder)
    assert holder["pending_list_changed"] is True


@pytest.mark.asyncio
async def test_flush_list_changed_resets_session_on_send_failure():
    session = AsyncMock()
    session.send_notification.side_effect = RuntimeError("closed")
    holder = {"session": session, "pending_list_changed": True}
    await _flush_list_changed(holder)
    assert holder["pending_list_changed"] is True  # retry next time
    assert holder["session"] is None
