from __future__ import annotations

from pathlib import Path

import yaml

from greybeam_mcp.child.config_writer import write_child_config
from greybeam_mcp.config import CortexSearchService, OtherServices, SnowflakeConfig


def test_write_child_config_emits_locked_invariants(tmp_path: Path) -> None:
    sf = SnowflakeConfig(
        account="abc",
        user="agent",
        password="pw",
        search_services=[
            CortexSearchService(
                service_name="docs",
                description="Internal docs search",
                database_name="DOCS_DB",
                schema_name="PUBLIC",
            )
        ],
        analyst_services=[],
        agent_services=[],
        other_services=OtherServices(
            query_manager=False, object_manager=False, semantic_manager=False
        ),
    )
    out_path = tmp_path / "child.yaml"

    write_child_config(sf, out_path)

    dumped = yaml.safe_load(out_path.read_text())
    assert dumped["analyst_services"] == []
    assert dumped["agent_services"] == []
    assert dumped["other_services"] == {
        "query_manager": False,
        "object_manager": False,
        "semantic_manager": False,
    }
    assert len(dumped["search_services"]) == 1
    assert dumped["search_services"][0]["service_name"] == "docs"
    assert dumped["search_services"][0]["database_name"] == "DOCS_DB"
    assert dumped["search_services"][0]["schema_name"] == "PUBLIC"
    assert dumped["search_services"][0]["description"] == "Internal docs search"
