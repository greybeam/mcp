import pytest

from greybeam_mcp.tools.registry import (
    DELEGATED_TOOLS,
    OWNED_TOOLS,
    UnknownToolError,
    merge_tool_lists,
    resolve,
)


def test_owned_and_delegated_sets():
    assert OWNED_TOOLS == {"cortex_analyst", "run_snowflake_query"}
    assert DELEGATED_TOOLS == {"cortex_search"}


def test_resolve_owned():
    assert resolve("run_snowflake_query") == "owned"
    assert resolve("cortex_analyst") == "owned"


def test_resolve_delegated():
    assert resolve("cortex_search") == "delegated"


def test_resolve_unknown_raises():
    with pytest.raises(UnknownToolError):
        resolve("anything_else")


def test_merge_tool_lists_is_deterministic():
    owned = [{"name": "run_snowflake_query"}, {"name": "cortex_analyst"}]
    delegated = [{"name": "cortex_search"}]
    merged = merge_tool_lists(owned, delegated)
    assert [t["name"] for t in merged] == [
        "cortex_analyst",
        "run_snowflake_query",
        "cortex_search",
    ]


def test_merge_filters_unexpected_delegated_tools():
    owned = [{"name": "run_snowflake_query"}, {"name": "cortex_analyst"}]
    delegated = [{"name": "cortex_search"}, {"name": "rogue_tool"}]
    merged = merge_tool_lists(owned, delegated)
    names = [t["name"] for t in merged]
    assert "rogue_tool" not in names
    assert "cortex_search" in names
