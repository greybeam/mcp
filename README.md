# Greybeam MCP

A single MCP server that lets agents query data through the Greybeam routing layer with Snowflake-compatible tooling.

- `run_snowflake_query` — executes SQL via the Greybeam proxy (Snowflake protocol).
- `cortex_analyst` — calls Snowflake Cortex Analyst; any returned SQL is executed via Greybeam.
- `cortex_search` — delegated to the pinned upstream Snowflake MCP (no SQL, REST only).

Out of scope for v1: Cortex Agent, semantic views, and the upstream `object_manager` / `query_manager` / `semantic_manager` tool families. These are locked off in the child config so a misconfigured deployment can't accidentally expose them.

## Install and run

    uvx greybeam-mcp --config /path/to/greybeam.yaml

For a permanent environment:

    pip install greybeam-mcp
    greybeam-mcp --config /path/to/greybeam.yaml

## Configuration

See `examples/greybeam.yaml`. Secrets come from environment variables; everything else is YAML.

Required env vars (one of the auth methods is required):

- `SNOWFLAKE_USER`
- `SNOWFLAKE_PASSWORD` (or `SNOWFLAKE_PRIVATE_KEY` / `SNOWFLAKE_AUTHENTICATOR`)

### Cortex Analyst auth (v1 limitation)

The Cortex Analyst REST endpoint expects `Authorization: Bearer <oauth_or_jwt>`. The v1 client supports Bearer (`token`) directly; the password / Basic-auth branch is test scaffolding and will return 401 against real Snowflake. If you need Cortex Analyst in production today, configure an OAuth access token via the `token` field in code; broader keypair-JWT support is tracked for v1.1.

## Claude Desktop integration

See `examples/claude_desktop_config.json`.

## Statement-type policy

Greybeam MCP does **not** enforce statement-type restrictions at the MCP layer. CREATE / DROP / ALTER and other potentially destructive statements are subject to:

1. Greybeam backend routing and policy.
2. Your Snowflake role's grants on the target objects.

If you need hard MCP-layer restrictions, scope the Snowflake role tightly or contact Greybeam about backend policy options. This is an intentional v1 divergence from upstream Snowflake MCP, which blocks DDL by default — see the design doc for rationale.

## Cancellation

v1 bounds in-flight calls via two driver-level mechanisms inside `run_snowflake_query`:

1. `cursor.execute(timeout=greybeam.query_timeout)` — Snowflake's own query timeout.
2. Explicit `cursor.cancel()` on row-cap / byte-cap exceedance.

Client-driven cancellation (`notifications/cancelled`) is **not** wired in v1. The `CancelToken` primitive, dispatcher in-flight table, and delegated-cancel forwarding are scaffolding retained and unit-tested so v1.1 can light them up by adding a `notifications/cancelled` handler.

## Development

    uv sync --extra dev
    uv run pytest

The default suite runs unit + always-on contract tests (no network, no DB). Two test layers are gated behind environment variables:

Contract tests against the real upstream child (requires real Snowflake credentials — placeholders are not enough because the upstream child may validate at startup):

    GREYBEAM_RUN_CHILD_CONTRACT=1 \
      SNOWFLAKE_ACCOUNT=... SNOWFLAKE_USER=... SNOWFLAKE_PASSWORD=... \
      uv run pytest tests/contract/

Integration tests against a real Greybeam dev endpoint:

    GREYBEAM_RUN_INTEGRATION=1 \
      SNOWFLAKE_ACCOUNT=... SNOWFLAKE_USER=... SNOWFLAKE_PASSWORD=... \
      GREYBEAM_PROXY_HOST=greybeam-dev.example.com \
      uv run pytest tests/integration/

The upstream Snowflake MCP package is pinned at `snowflake-labs-mcp==1.4.1` (import name `mcp_server_snowflake`). Bumping that pin should re-run the child contract snapshot test and re-approve `tests/contract/fixtures/child_tools_list.json` if the surface drifted.

Design doc: `docs/superpowers/specs/2026-04-24-greybeam-mcp-design.md`.
