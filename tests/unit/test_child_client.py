from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from greybeam_mcp.child.client import ChildMcpClient


def _make_stdio_cm() -> MagicMock:
    """A context manager whose __aenter__ returns (read, write) streams."""
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=(MagicMock(), MagicMock()))
    cm.__aexit__ = AsyncMock(return_value=None)
    return cm


def _make_session_cm(session: MagicMock) -> MagicMock:
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=session)
    cm.__aexit__ = AsyncMock(return_value=None)
    return cm


@pytest.mark.asyncio
async def test_client_initializes_and_lists_tools() -> None:
    session = MagicMock()
    session.initialize = AsyncMock()
    tool = SimpleNamespace(model_dump=lambda: {"name": "cortex_search"})
    session.list_tools = AsyncMock(return_value=SimpleNamespace(tools=[tool]))

    with patch(
        "greybeam_mcp.child.client.stdio_client", return_value=_make_stdio_cm()
    ), patch(
        "greybeam_mcp.child.client.ClientSession",
        return_value=_make_session_cm(session),
    ):
        client = ChildMcpClient(command="echo", args=["hi"])
        await client.start()

        session.initialize.assert_awaited_once()

        tools = await client.list_tools()
        assert tools == [{"name": "cortex_search"}]

        await client.stop()


@pytest.mark.asyncio
async def test_call_tool_forwards_to_session() -> None:
    session = MagicMock()
    session.initialize = AsyncMock()
    result_obj = SimpleNamespace(
        model_dump=lambda: {"isError": False, "content": [{"type": "text", "text": "ok"}]}
    )
    session.call_tool = AsyncMock(return_value=result_obj)

    with patch(
        "greybeam_mcp.child.client.stdio_client", return_value=_make_stdio_cm()
    ), patch(
        "greybeam_mcp.child.client.ClientSession",
        return_value=_make_session_cm(session),
    ):
        client = ChildMcpClient(command="echo", args=[])
        await client.start()

        result = await client.call_tool("cortex_search", {"query": "x"})

        session.call_tool.assert_awaited_once_with(
            "cortex_search", arguments={"query": "x"}
        )
        assert result == {"isError": False, "content": [{"type": "text", "text": "ok"}]}

        await client.stop()


@pytest.mark.asyncio
async def test_send_notification_forwards_to_session() -> None:
    session = MagicMock()
    session.initialize = AsyncMock()
    session.send_notification = AsyncMock()

    with patch(
        "greybeam_mcp.child.client.stdio_client", return_value=_make_stdio_cm()
    ), patch(
        "greybeam_mcp.child.client.ClientSession",
        return_value=_make_session_cm(session),
    ):
        client = ChildMcpClient(command="echo", args=[])
        await client.start()

        await client.send_notification(
            "notifications/cancelled", {"requestId": "abc", "reason": "user"}
        )

        session.send_notification.assert_awaited_once()

        await client.stop()


def test_is_alive_reflects_session_state() -> None:
    client = ChildMcpClient(command="echo", args=[])
    assert client.is_alive() is False
