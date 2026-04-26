# Greybeam MCP

A single MCP server that lets agents query data through the Greybeam routing layer with Snowflake-compatible tooling.

- `run_snowflake_query` — executes SQL via the Greybeam proxy (Snowflake protocol).
- `cortex_analyst` — calls Snowflake Cortex Analyst; any returned SQL is executed via Greybeam.
- `cortex_search` — delegated to the pinned upstream Snowflake MCP (no SQL, REST only).

Out of scope for v1: Cortex Agent, semantic views, and the upstream `object_manager` / `query_manager` / `semantic_manager` tool families. These are locked off in the child config so a misconfigured deployment can't accidentally expose them.

## Quick start

Greybeam MCP is not yet published to PyPI — install from a local clone with
[`uv`](https://docs.astral.sh/uv/):

    git clone https://github.com/greybeam/mcp.git greybeam-mcp
    cd greybeam-mcp
    uv sync
    uv run greybeam-mcp init

The `init` wizard prompts for account, user, proxy host, and auth method;
writes a config file at `~/.config/greybeam-mcp.yaml` (mode 0600); and prints
the exact registration command for Claude Code and Claude Desktop — keyed to
this clone's path so the printed snippets are copy-pasteable. Recommended path
for new users.

> Once the package ships on PyPI, the same wizard will print the simpler
> `uvx greybeam-mcp …` form. The `init` flow already detects which environment
> it's running in.

## Manual install and run

If you'd rather author the YAML by hand, copy `examples/greybeam.yaml` to a
location of your choice, edit it, `chmod 600` it, then:

    uv run greybeam-mcp --config /absolute/path/to/greybeam.yaml

You can run the server from anywhere by pointing `uv` at the clone:

    uv --directory /absolute/path/to/greybeam-mcp run \
        greybeam-mcp --config /absolute/path/to/greybeam.yaml

## Configuration

The YAML is the single source of truth — it holds account, proxy host, **and**
auth credentials. Pick one auth method (in order of recommendation):

- `private_key_file` (path to a PEM key, plus optional `private_key_passphrase`) — **recommended**, since Snowflake is deprecating password auth
- `private_key` (inline PEM contents) — for environments without a writable disk
- `authenticator: externalbrowser` for SSO (requires a SAML2 integration on the account)
- `password` — deprecated by Snowflake, avoid for new setups

`chmod 600` the file since it contains credentials.

### Environment variable fallback

Every field above can also come from an environment variable
(`SNOWFLAKE_USER`, `SNOWFLAKE_PRIVATE_KEY_FILE`, `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE`,
`SNOWFLAKE_PRIVATE_KEY`, `SNOWFLAKE_AUTHENTICATOR`, `SNOWFLAKE_PASSWORD`). YAML
takes precedence; envs fill in unset fields. Useful in container/k8s deployments
where secrets are mounted as env vars.

### Cortex Analyst auth (v1 limitation)

The Cortex Analyst REST endpoint expects `Authorization: Bearer <oauth_or_jwt>`. The v1 client supports Bearer (`token`) directly; the password / Basic-auth branch is test scaffolding and will return 401 against real Snowflake. If you need Cortex Analyst in production today, configure an OAuth access token via the `token` field in code; broader keypair-JWT support is tracked for v1.1.

## Client integration

`uv run greybeam-mcp init` prints registration snippets pre-filled with this
clone's absolute path. The general shapes:

**Claude Code (CLI):**

    claude mcp add greybeam -- uv --directory /absolute/path/to/greybeam-mcp \
        run greybeam-mcp --config /absolute/path/to/greybeam.yaml

Start a new `claude` session to pick it up. Verify with `claude mcp list`.

**Claude Desktop:** see `examples/claude_desktop_config.json`. Paste into
`~/Library/Application Support/Claude/claude_desktop_config.json` (macOS) or
the equivalent on your platform, fill in both absolute paths, then restart the
app.

After a successful install, ask the agent something like *"run select 1 on
snowflake"* — it should pick the `run_snowflake_query` tool automatically.

## Statement-type policy

Greybeam MCP does **not** enforce statement-type restrictions at the MCP layer. CREATE / DROP / ALTER and other potentially destructive statements are subject to:

1. Greybeam backend routing and policy.
2. Your Snowflake role's grants on the target objects.

If you need hard MCP-layer restrictions, scope the Snowflake role tightly or contact Greybeam about backend policy options. This is an intentional v1 divergence from upstream Snowflake MCP, which blocks DDL by default — see the design doc for rationale.

## Cancellation

v1 bounds in-flight calls via two driver-level mechanisms inside `run_snowflake_query`:

1. `cursor.execute(timeout=greybeam.query_timeout)` — Snowflake's own query timeout.
2. Explicit `cursor.close()` on row-cap / byte-cap exceedance (acquires the
   driver's `_lock_canceling` and aborts the in-flight query).

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
