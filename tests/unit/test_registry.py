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


def test_merge_keeps_owned_before_delegated_even_when_alphabetically_after():
    """Owned tools always precede delegated, regardless of alphabetical order."""
    owned = [{"name": "zz_owned"}]
    delegated = [{"name": "cortex_search"}]
    merged = merge_tool_lists(owned, delegated)
    assert [t["name"] for t in merged] == ["zz_owned", "cortex_search"]


def test_merge_raises_on_duplicate_names():
    """A malformed input list with intra-side dups must fail fast."""
    owned = [{"name": "cortex_analyst"}, {"name": "cortex_analyst"}]
    delegated = [{"name": "cortex_search"}]
    with pytest.raises(ValueError, match="duplicate tool name"):
        merge_tool_lists(owned, delegated)
