"""Greybeam-owned `cortex_analyst` shim.

Per spec §5.4:
  1. POST to Cortex Analyst REST API at the real Snowflake account URL.
  2. Parse response: keep `text` and optional `sql` only.
  3. If SQL present, execute via run_snowflake_query pathway (Greybeam-routed).
  4. On internal SQL failure, the whole tool fails (no partial payload).
  5. Otherwise return Pydantic JSON containing text + optional sql + optional results.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel

from greybeam_mcp.cancel import CancelToken
from greybeam_mcp.config import GreybeamConfig, SnowflakeConfig
from greybeam_mcp.tools.cortex_analyst_client import CortexAnalystClient
from greybeam_mcp.tools.cortex_analyst_parser import parse_analyst_response
from greybeam_mcp.tools.run_snowflake_query import ToolResult, run_snowflake_query


CORTEX_ANALYST_INPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "messages": {
            "type": "array",
            "description": "Conversation messages forwarded to Cortex Analyst.",
            "items": {"type": "object"},
        }
    },
    "required": ["messages"],
    "additionalProperties": True,
}


class AnalystPayload(BaseModel):
    """Strict upstream-compat wire shape: text + optional sql + optional results."""

    text: str = ""
    sql: str | None = None
    results: list[dict[str, Any]] | None = None


@dataclass
class CortexAnalystResult:
    is_error: bool
    json_payload: dict[str, Any] | None = None
    error_kind: str | None = None
    error_message: str | None = None


async def cortex_analyst(
    *,
    arguments: dict[str, Any],
    sf: SnowflakeConfig,
    gb: GreybeamConfig,
    cancel_token: CancelToken | None = None,
) -> CortexAnalystResult:
    # SnowflakeConfig.password is a SecretStr; the REST client takes a
    # plain string. Extract here so the auth header isn't built from
    # the SecretStr's '**********' repr.
    pw = sf.password.get_secret_value() if sf.password is not None else None
    client = CortexAnalystClient(
        account=sf.account,
        user=sf.user,
        password=pw,
    )
    try:
        raw = await client.send_message(arguments)
    except Exception as e:
        # Wrap REST/transport failures (4xx/5xx, network errors, missing
        # credentials raised inside send_message). Cancellation
        # (CancelledError, BaseException) deliberately propagates.
        return CortexAnalystResult(
            is_error=True,
            error_kind="cortex_analyst_api",
            error_message=str(e),
        )

    parsed = parse_analyst_response(raw)
    results: list[dict[str, Any]] | None = None

    if parsed.sql:
        sql_result: ToolResult = await run_snowflake_query(
            statement=parsed.sql, sf=sf, gb=gb, cancel_token=cancel_token
        )
        if sql_result.is_error:
            # Whole-tool failure per spec §5.4 step 4: no partial payload.
            return CortexAnalystResult(
                is_error=True,
                error_kind="cortex_analyst_sql_failed",
                error_message=(
                    f"Internal SQL execution failed: {sql_result.error_kind}: "
                    f"{sql_result.error_message}"
                ),
            )
        results = sql_result.rows

    # Build the payload once with all fields rather than constructing then
    # mutating — preserves the immutability convention from CLAUDE.md.
    payload = AnalystPayload(text=parsed.text, sql=parsed.sql, results=results)
    return CortexAnalystResult(
        is_error=False,
        json_payload=payload.model_dump(exclude_none=True),
    )
