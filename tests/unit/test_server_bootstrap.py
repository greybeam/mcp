from greybeam_mcp.server import build_server_metadata


def test_metadata_advertises_tools_listChanged():
    meta = build_server_metadata()
    assert meta["serverInfo"]["name"] == "Greybeam MCP"
    assert "Greybeam" in meta["instructions"]
    assert meta["capabilities"]["tools"]["listChanged"] is True
    # fail-closed: prompts/resources NOT advertised
    assert "prompts" not in meta["capabilities"]
    assert "resources" not in meta["capabilities"]
