import sys
from unittest.mock import MagicMock

import pytest

from greybeam_mcp.cancel import CancelToken
from greybeam_mcp.tools.run_snowflake_query import (
    CapExceededError,
    Cancelled,
    _execute_sync,
)


def _mock_conn(batches: list[list[dict]]):
    conn = MagicMock()
    cursor = MagicMock()
    cursor.fetchmany.side_effect = [*batches, []]
    conn.cursor.return_value.__enter__.return_value = cursor
    conn.cursor.return_value.__exit__.return_value = False
    return conn, cursor


def test_returns_full_results_under_cap():
    conn, cursor = _mock_conn([[{"a": 1}, {"a": 2}]])
    rows = _execute_sync(
        conn=conn,
        statement="SELECT 1",
        row_cap=10,
        byte_cap=10_000,
        timeout=30,
        cancel_token=CancelToken(),
        batch_size=1000,
    )
    assert rows == [{"a": 1}, {"a": 2}]
    cursor.execute.assert_called_once_with("SELECT 1", timeout=30)


def test_row_cap_exceeded_raises():
    conn, cursor = _mock_conn([[{"a": i} for i in range(100)]])
    with pytest.raises(CapExceededError) as exc:
        _execute_sync(
            conn=conn,
            statement="SELECT *",
            row_cap=10,
            byte_cap=10_000_000,
            timeout=30,
            cancel_token=CancelToken(),
            batch_size=1000,
        )
    cursor.cancel.assert_called_once()
    assert "row_cap" in str(exc.value)


def test_byte_cap_exceeded_raises():
    big_value = "x" * 10_000
    conn, cursor = _mock_conn([[{"a": big_value} for _ in range(5)]])
    with pytest.raises(CapExceededError) as exc:
        _execute_sync(
            conn=conn,
            statement="SELECT *",
            row_cap=1_000_000,
            byte_cap=1000,
            timeout=30,
            cancel_token=CancelToken(),
            batch_size=1000,
        )
    cursor.cancel.assert_called_once()
    assert "byte_cap" in str(exc.value)


def test_cancel_token_observed_between_batches():
    token = CancelToken()
    seen = []

    def fetch_many_side_effect(_n):
        if not seen:
            token.set()
        seen.append(1)
        return [{"a": 1}]

    conn = MagicMock()
    cursor = MagicMock()
    cursor.fetchmany.side_effect = fetch_many_side_effect
    conn.cursor.return_value.__enter__.return_value = cursor
    conn.cursor.return_value.__exit__.return_value = False

    with pytest.raises(Cancelled):
        _execute_sync(
            conn=conn,
            statement="SELECT *",
            row_cap=1_000_000,
            byte_cap=1_000_000,
            timeout=30,
            cancel_token=token,
            batch_size=1,
        )
    cursor.cancel.assert_called_once()
