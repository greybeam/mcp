# Greybeam MCP

MCP server that routes SQL queries through the Greybeam proxy while delegating Cortex Search to upstream Snowflake MCP. See `docs/superpowers/specs/2026-04-24-greybeam-mcp-design.md` for design.

Install (once published):

    uvx greybeam-mcp --config /path/to/greybeam.yaml
