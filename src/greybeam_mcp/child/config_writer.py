"""Serialize a SnowflakeConfig into the YAML the upstream MCP child consumes.

The child YAML is constrained by the locked invariants in
``greybeam_mcp.config``: analyst_services and agent_services must be empty,
and other_services flags must all be False. We surface those values
explicitly so an operator inspecting the file can see the policy.
"""
from __future__ import annotations

from pathlib import Path

import yaml

from greybeam_mcp.config import SnowflakeConfig


def write_child_config(sf: SnowflakeConfig, out_path: Path) -> None:
    payload = {
        "search_services": [s.model_dump() for s in sf.search_services],
        "analyst_services": [],
        "agent_services": [],
        "other_services": {
            "query_manager": False,
            "object_manager": False,
            "semantic_manager": False,
        },
    }
    out_path.write_text(yaml.safe_dump(payload, sort_keys=False))
