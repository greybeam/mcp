from __future__ import annotations

import asyncio

import pytest
from unittest.mock import AsyncMock

from greybeam_mcp.child.manager import ChildManager, ChildState
from greybeam_mcp.config import RestartPolicy


def _policy(attempts: int = 3) -> RestartPolicy:
    return RestartPolicy(
        max_attempts=attempts, backoff_seconds=[0.001, 0.001, 0.001], jitter=False
    )


@pytest.mark.asyncio
async def test_start_succeeds_first_try() -> None:
    client = AsyncMock()
    client.start = AsyncMock()
    client.list_tools = AsyncMock(return_value=[{"name": "cortex_search"}])

    mgr = ChildManager(
        client_factory=lambda: client,
        restart_policy=_policy(),
        cortex_search_required=True,
    )
    await mgr.start()
    assert mgr.state == ChildState.RUNNING
    assert mgr.tools == [{"name": "cortex_search"}]


@pytest.mark.asyncio
async def test_startup_required_search_missing_raises() -> None:
    client = AsyncMock()
    client.start = AsyncMock()
    client.list_tools = AsyncMock(return_value=[])

    mgr = ChildManager(
        client_factory=lambda: client,
        restart_policy=_policy(),
        cortex_search_required=True,
    )
    with pytest.raises(RuntimeError, match="cortex_search not advertised"):
        await mgr.start()


@pytest.mark.asyncio
async def test_startup_failure_retries_then_degraded_when_optional() -> None:
    attempts: list[int] = []

    def factory() -> AsyncMock:
        attempts.append(1)
        c = AsyncMock()
        c.start = AsyncMock(side_effect=RuntimeError("nope"))
        return c

    mgr = ChildManager(
        client_factory=factory,
        restart_policy=_policy(attempts=2),
        cortex_search_required=False,
    )
    await mgr.start()
    assert mgr.state == ChildState.DEGRADED
    assert len(attempts) == 2


@pytest.mark.asyncio
async def test_startup_failure_required_raises() -> None:
    def factory() -> AsyncMock:
        c = AsyncMock()
        c.start = AsyncMock(side_effect=RuntimeError("nope"))
        return c

    mgr = ChildManager(
        client_factory=factory,
        restart_policy=_policy(attempts=2),
        cortex_search_required=True,
    )
    with pytest.raises(RuntimeError):
        await mgr.start()


@pytest.mark.asyncio
async def test_call_tool_rejects_when_not_running() -> None:
    client = AsyncMock()
    client.start = AsyncMock()
    client.list_tools = AsyncMock(return_value=[{"name": "cortex_search"}])
    client.call_tool = AsyncMock(return_value={"isError": False, "content": []})

    mgr = ChildManager(
        client_factory=lambda: client,
        restart_policy=_policy(),
        cortex_search_required=True,
    )
    await mgr.start()
    mgr.state = ChildState.DEGRADED
    with pytest.raises(RuntimeError, match="child not available"):
        await mgr.call_tool("cortex_search", {"query": "x"})


@pytest.mark.asyncio
async def test_runtime_crash_triggers_recovery_and_succeeds() -> None:
    calls = {"count": 0}

    def factory() -> AsyncMock:
        calls["count"] += 1
        c = AsyncMock()
        c.start = AsyncMock()
        c.list_tools = AsyncMock(return_value=[{"name": "cortex_search"}])
        if calls["count"] == 1:
            c.call_tool = AsyncMock(side_effect=ConnectionError("pipe closed"))
        else:
            c.call_tool = AsyncMock(return_value={"isError": False, "content": []})
        c.stop = AsyncMock()
        return c

    states: list[ChildState] = []

    async def on_state(s: ChildState) -> None:
        states.append(s)

    mgr = ChildManager(
        client_factory=factory,
        restart_policy=_policy(),
        cortex_search_required=True,
        on_state_change=on_state,
    )
    await mgr.start()
    assert mgr.state == ChildState.RUNNING

    with pytest.raises(ConnectionError):
        await mgr.call_tool("cortex_search", {"query": "x"})

    for _ in range(50):
        if mgr.state == ChildState.RUNNING and calls["count"] >= 2:
            break
        await asyncio.sleep(0.01)

    assert calls["count"] == 2
    assert mgr.state == ChildState.RUNNING
    assert ChildState.RUNNING in states


@pytest.mark.asyncio
async def test_runtime_crash_exhausts_retries_then_degraded() -> None:
    calls = {"count": 0}

    def factory() -> AsyncMock:
        calls["count"] += 1
        c = AsyncMock()
        c.list_tools = AsyncMock(return_value=[{"name": "cortex_search"}])
        c.call_tool = AsyncMock(side_effect=ConnectionError("pipe closed"))
        c.stop = AsyncMock()
        if calls["count"] == 1:
            c.start = AsyncMock()
        else:
            c.start = AsyncMock(side_effect=RuntimeError("nope"))
        return c

    states: list[ChildState] = []

    async def on_state(s: ChildState) -> None:
        states.append(s)

    mgr = ChildManager(
        client_factory=factory,
        restart_policy=_policy(attempts=2),
        cortex_search_required=False,
        on_state_change=on_state,
    )
    await mgr.start()
    with pytest.raises(ConnectionError):
        await mgr.call_tool("cortex_search", {"query": "x"})

    for _ in range(50):
        if mgr.state == ChildState.DEGRADED:
            break
        await asyncio.sleep(0.01)

    assert mgr.state == ChildState.DEGRADED
    # State callback must fire exactly RUNNING -> STOPPED -> DEGRADED with
    # no duplicates (each transition becomes a tools/list_changed in the
    # eventual server wiring; duplicates would cause notification storms).
    assert states == [ChildState.RUNNING, ChildState.STOPPED, ChildState.DEGRADED]


@pytest.mark.asyncio
async def test_list_tools_failure_stops_started_client() -> None:
    """If client.start() succeeds but list_tools() raises, the started
    client must be stopped before the retry — otherwise the subprocess leaks.
    """
    calls = {"count": 0}
    started_clients: list[AsyncMock] = []

    def factory() -> AsyncMock:
        calls["count"] += 1
        c = AsyncMock()
        c.start = AsyncMock()
        c.stop = AsyncMock()
        if calls["count"] == 1:
            c.list_tools = AsyncMock(side_effect=ConnectionError("died on first request"))
        else:
            c.list_tools = AsyncMock(return_value=[{"name": "cortex_search"}])
        started_clients.append(c)
        return c

    mgr = ChildManager(
        client_factory=factory,
        restart_policy=_policy(attempts=2),
        cortex_search_required=True,
    )
    await mgr.start()
    assert mgr.state == ChildState.RUNNING
    assert calls["count"] == 2
    # First (broken) client must have been stopped before the second was spawned.
    started_clients[0].stop.assert_awaited()


@pytest.mark.asyncio
async def test_state_callback_exception_does_not_break_recovery() -> None:
    """on_state_change raising must not prevent teardown + recovery scheduling.

    Without this guarantee, a misbehaving observer could leak a dead
    client and silently disable recovery.
    """
    calls = {"count": 0}

    def factory() -> AsyncMock:
        calls["count"] += 1
        c = AsyncMock()
        c.list_tools = AsyncMock(return_value=[{"name": "cortex_search"}])
        c.start = AsyncMock()
        c.stop = AsyncMock()
        if calls["count"] == 1:
            c.call_tool = AsyncMock(side_effect=ConnectionError("pipe closed"))
        else:
            c.call_tool = AsyncMock(return_value={"isError": False, "content": []})
        return c

    async def on_state(_s: ChildState) -> None:
        raise RuntimeError("observer is broken")

    mgr = ChildManager(
        client_factory=factory,
        restart_policy=_policy(),
        cortex_search_required=True,
        on_state_change=on_state,
    )
    await mgr.start()
    assert mgr.state == ChildState.RUNNING

    # The original ConnectionError must surface — not the callback's RuntimeError.
    with pytest.raises(ConnectionError):
        await mgr.call_tool("cortex_search", {"query": "x"})

    for _ in range(50):
        if mgr.state == ChildState.RUNNING and calls["count"] >= 2:
            break
        await asyncio.sleep(0.01)

    assert mgr.state == ChildState.RUNNING
    assert calls["count"] == 2


@pytest.mark.asyncio
async def test_concurrent_failing_call_tools_fire_state_callback_once() -> None:
    """Two concurrent call_tool failures must NOT double-fire state transitions
    or schedule parallel recovery tasks. Pins the guard contract that
    relies on `self._client = None` running synchronously before any await
    in the except branch — a regression in that ordering would let both
    callers slip through and double-fire the tools/list_changed signal.
    """
    calls = {"count": 0}

    def factory() -> AsyncMock:
        calls["count"] += 1
        c = AsyncMock()
        c.list_tools = AsyncMock(return_value=[{"name": "cortex_search"}])
        c.start = AsyncMock()
        c.stop = AsyncMock()
        if calls["count"] == 1:
            # Slow failure so both concurrent callers are awaiting the same
            # call_tool when the exception lands.
            async def slow_fail(*_args: object, **_kwargs: object) -> None:
                await asyncio.sleep(0.02)
                raise ConnectionError("pipe closed")

            c.call_tool = AsyncMock(side_effect=slow_fail)
        else:
            c.call_tool = AsyncMock(return_value={"isError": False, "content": []})
        return c

    states: list[ChildState] = []

    async def on_state(s: ChildState) -> None:
        states.append(s)

    mgr = ChildManager(
        client_factory=factory,
        restart_policy=_policy(),
        cortex_search_required=True,
        on_state_change=on_state,
    )
    await mgr.start()
    assert mgr.state == ChildState.RUNNING

    # Fire two concurrent call_tools; both should raise, only one should
    # transition the state machine.
    results = await asyncio.gather(
        mgr.call_tool("cortex_search", {"query": "a"}),
        mgr.call_tool("cortex_search", {"query": "b"}),
        return_exceptions=True,
    )
    assert all(isinstance(r, ConnectionError) for r in results)

    # Wait for recovery to complete.
    for _ in range(50):
        if mgr.state == ChildState.RUNNING and calls["count"] >= 2:
            break
        await asyncio.sleep(0.01)

    # STOPPED must appear exactly once across the lifecycle (one teardown,
    # not two), even though two callers failed at the same time.
    assert states.count(ChildState.STOPPED) == 1
    # Final sequence: RUNNING (start) -> STOPPED (teardown) -> RUNNING (recover).
    assert states == [ChildState.RUNNING, ChildState.STOPPED, ChildState.RUNNING]
    assert calls["count"] == 2  # one initial + one recovery


@pytest.mark.asyncio
async def test_start_cancellation_stops_started_client() -> None:
    """If start() is cancelled while inside list_tools, the already-started
    client must still be torn down — pins the BaseException leg of
    _try_start_once's cleanup that the inline comment calls out.
    """
    started_clients: list[AsyncMock] = []
    list_tools_entered = asyncio.Event()

    def factory() -> AsyncMock:
        c = AsyncMock()
        c.start = AsyncMock()
        c.stop = AsyncMock()

        async def slow_list_tools() -> list[dict[str, str]]:
            list_tools_entered.set()
            await asyncio.sleep(5)  # long enough that the test cancels first
            return [{"name": "cortex_search"}]

        c.list_tools = AsyncMock(side_effect=slow_list_tools)
        started_clients.append(c)
        return c

    mgr = ChildManager(
        client_factory=factory,
        restart_policy=_policy(),
        cortex_search_required=True,
    )

    task = asyncio.create_task(mgr.start())
    await list_tools_entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # The started client must have had stop() awaited despite the cancel.
    assert len(started_clients) == 1
    started_clients[0].stop.assert_awaited()


@pytest.mark.asyncio
async def test_stop_cancels_in_flight_recovery() -> None:
    """stop() while recovery is mid-backoff must cancel the recovery task and finish cleanly."""
    calls = {"count": 0}

    def factory() -> AsyncMock:
        calls["count"] += 1
        c = AsyncMock()
        c.list_tools = AsyncMock(return_value=[{"name": "cortex_search"}])
        c.stop = AsyncMock()
        if calls["count"] == 1:
            c.start = AsyncMock()
            c.call_tool = AsyncMock(side_effect=ConnectionError("pipe closed"))
        else:
            # Slow restart so stop() catches recovery mid-flight
            async def slow_start() -> None:
                await asyncio.sleep(5)

            c.start = AsyncMock(side_effect=slow_start)
        return c

    mgr = ChildManager(
        client_factory=factory,
        restart_policy=RestartPolicy(
            max_attempts=3, backoff_seconds=[0.001], jitter=False
        ),
        cortex_search_required=False,
    )
    await mgr.start()
    with pytest.raises(ConnectionError):
        await mgr.call_tool("cortex_search", {"query": "x"})

    # Give the recovery task a moment to enter its slow start()
    await asyncio.sleep(0.05)
    # Now stop — must cancel the in-flight recovery task cleanly
    await asyncio.wait_for(mgr.stop(), timeout=2.0)
    assert mgr.state == ChildState.STOPPED
