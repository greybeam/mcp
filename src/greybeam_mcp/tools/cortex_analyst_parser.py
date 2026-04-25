"""Parse Cortex Analyst responses into the upstream-compatible shape.

Per spec §5.4 step 3: materialize `text` and `sql` only. All other content
types (including `suggestions` and any future types) are dropped to preserve
strict parity with upstream Snowflake MCP's parser.
"""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class ParsedAnalyst(BaseModel):
    text: str = ""
    sql: str | None = None


def parse_analyst_response(raw: dict[str, Any]) -> ParsedAnalyst:
    blocks = raw.get("message", {}).get("content", []) or []
    text_parts: list[str] = []
    sql: str | None = None
    for block in blocks:
        btype = block.get("type")
        if btype == "text":
            text_parts.append(block.get("text", ""))
        elif btype == "sql":
            sql = block.get("statement")
    return ParsedAnalyst(text="\n".join(text_parts), sql=sql)
