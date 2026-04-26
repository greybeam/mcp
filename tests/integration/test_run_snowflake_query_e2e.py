"""End-to-end smoke tests for run_snowflake_query against a real Greybeam
dev endpoint. Gated behind:
  GREYBEAM_RUN_INTEGRATION=1
  SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD
  GREYBEAM_PROXY_HOST  (e.g. greybeam-dev.example.com)
"""
from __future__ import annotations

import os

import pytest

from greybeam_mcp.config import (
    GreybeamConfig,
    OtherServices,
    RestartPolicy,
    SnowflakeConfig,
)
from greybeam_mcp.tools.run_snowflake_query import run_snowflake_query

pytestmark = pytest.mark.skipif(
    os.environ.get("GREYBEAM_RUN_INTEGRATION") != "1",
    reason="set GREYBEAM_RUN_INTEGRATION=1 to run integration tests",
)


@pytest.fixture
def configs() -> tuple[SnowflakeConfig, GreybeamConfig]:
    sf = SnowflakeConfig(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        analyst_services=[],
        agent_services=[],
        other_services=OtherServices(
            query_manager=False, object_manager=False, semantic_manager=False
        ),
    )
    gb = GreybeamConfig(
        proxy_host=os.environ["GREYBEAM_PROXY_HOST"],
        row_cap=10_000,
        byte_cap=10_000_000,
        query_timeout=60,
        child_restart_policy=RestartPolicy(
            max_attempts=3, backoff_seconds=[1, 4, 16], jitter=True
        ),
        cortex_search_required=False,
    )
    return sf, gb


@pytest.mark.asyncio
async def test_select_one(configs: tuple[SnowflakeConfig, GreybeamConfig]) -> None:
    sf, gb = configs
    result = await run_snowflake_query(
        statement="SELECT 1 AS x", sf=sf, gb=gb, cancel_token=None
    )
    assert result.is_error is False, result.error_message
    # Snowflake may return uppercase or lowercase column names depending on
    # quoting / role configuration; accept either.
    assert result.rows == [{"X": 1}] or result.rows == [{"x": 1}]


@pytest.mark.asyncio
async def test_row_cap_trips(configs: tuple[SnowflakeConfig, GreybeamConfig]) -> None:
    sf, gb = configs
    gb_small = gb.model_copy(update={"row_cap": 5})
    result = await run_snowflake_query(
        statement="SELECT seq8() FROM table(generator(rowcount=>1000))",
        sf=sf,
        gb=gb_small,
        cancel_token=None,
    )
    assert result.is_error is True
    # Assert on the stable error_kind rather than substring-matching the
    # human-readable message — copy edits to error_message would break
    # the test without any behavior change. error_kind="cap_exceeded"
    # covers both row and byte cap; here we know it's the row cap because
    # the byte cap is unchanged at 10MB and 1000 generator rows fit in it.
    assert result.error_kind == "cap_exceeded"
