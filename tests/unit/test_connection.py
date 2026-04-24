from unittest.mock import MagicMock, patch

import pytest

from greybeam_mcp.config import GreybeamConfig, RestartPolicy, SnowflakeConfig, OtherServices
from greybeam_mcp.greybeam.connection import open_connection


@pytest.fixture
def configs():
    sf = SnowflakeConfig(
        account="abc-xyz",
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
        byte_cap=1000,
        query_timeout=30,
        child_restart_policy=RestartPolicy(
            max_attempts=3, backoff_seconds=[1, 4, 16], jitter=True
        ),
        cortex_search_required=True,
    )
    return sf, gb


def test_open_connection_targets_greybeam_host(configs):
    sf, gb = configs
    with patch("greybeam_mcp.greybeam.connection.snowflake.connector.connect") as mock_connect:
        mock_connect.return_value = MagicMock()
        with open_connection(sf, gb) as conn:
            assert conn is mock_connect.return_value
        kwargs = mock_connect.call_args.kwargs
        assert kwargs["host"] == "greybeam.example.com"
        assert kwargs["account"] == "abc-xyz"
        assert kwargs["user"] == "agent"
        assert kwargs["password"] == "pw"


def test_open_connection_closes_on_exit(configs):
    sf, gb = configs
    with patch("greybeam_mcp.greybeam.connection.snowflake.connector.connect") as mock_connect:
        conn = MagicMock()
        mock_connect.return_value = conn
        with open_connection(sf, gb):
            pass
        conn.close.assert_called_once()
