# AGENTS.md

Reference guide for contributors and AI agents working on Greybeam MCP. Keep it terse — link out to the spec or the source for the why.

## Quick Reference

```bash
uv sync --extra dev                 # install deps
uv run pytest                       # run unit + always-on contract suite
uv run pytest -v                    # see SKIPPED env-gated tests
uv run ruff check src/ tests/       # lint
uv run mypy src/                    # type-check (optional)
uv run greybeam-mcp --help          # smoke the entrypoint
uv run greybeam-mcp --config ./examples/greybeam.yaml --log-level DEBUG
```

Default suite reports `104 passed, 3 skipped`. The 3 skips are env-gated:

- `GREYBEAM_RUN_CHILD_CONTRACT=1` + real `SNOWFLAKE_*` — runs `tests/contract/test_child_tools_list_snapshot.py` against the real upstream child.
- `GREYBEAM_RUN_INTEGRATION=1` + `SNOWFLAKE_*` + `GREYBEAM_PROXY_HOST` — runs `tests/integration/` against a real Greybeam dev endpoint.

## Structure

```
src/greybeam_mcp/
  __main__.py          # CLI entrypoint; tempfile lifecycle for child YAML
  server.py            # MCP Server runtime, handlers, session_holder closure
  dispatcher.py        # owned-vs-delegated routing + in-flight cancel table
  cancel.py            # CancelToken primitive (scaffolding for v1.1 cancel)
  config.py            # Pydantic schema; env-injected secrets; locked invariants
  logging_setup.py     # JSON logger + tool_call_log schema
  greybeam/
    connection.py      # per-call Snowflake connection through Greybeam proxy
  child/
    client.py          # stdio MCP client wrapping the upstream child
    manager.py         # bounded restart, runtime crash recovery, ChildState
    catalog.py         # merge owned + delegated tool lists for tools/list
    config_writer.py   # writes upstream YAML from locked invariants
  tools/
    registry.py        # OWNED_TOOLS / DELEGATED_TOOLS + resolve()
    run_snowflake_query.py    # streaming SQL with row/byte caps
    cortex_analyst.py         # orchestrator: REST → parser → run_snowflake_query
    cortex_analyst_client.py  # async httpx REST client for Cortex Analyst
    cortex_analyst_parser.py  # text+sql extraction (drops unknown content types)
tests/
  unit/                # 99 tests — production behavior in isolation
  contract/            # 5 always-on tests + 1 env-gated upstream snapshot
  integration/         # 2 env-gated e2e tests
  contract/fixtures/   # JSON fixtures pinning input schemas + envelope shapes
docs/superpowers/
  specs/2026-04-24-greybeam-mcp-design.md   # design doc — source of truth
  plans/2026-04-24-greybeam-mcp-v1.md       # v1 implementation plan
```

## Where to Look

| Want to change…                         | File                                                  |
|-----------------------------------------|-------------------------------------------------------|
| Add an owned tool                       | `tools/registry.py` (add to `OWNED_TOOLS`) + new tool module + dispatch branch in `dispatcher.py:_dispatch_owned` + descriptor in `server.py:build_owned_tool_descriptors` |
| Add a delegated tool                    | `tools/registry.py` (add to `DELEGATED_TOOLS`) + child must advertise it; routing already works |
| Change row/byte caps behavior           | `tools/run_snowflake_query.py:_execute_sync`          |
| Tweak Cortex Analyst content types      | `tools/cortex_analyst_parser.py`                      |
| Change child restart policy             | `child/manager.py` + the `RestartPolicy` config field |
| Add a new MCP capability                | `server.py:build_server_metadata` + handler in `run_server` |
| Modify the JSON log schema              | `logging_setup.py:tool_call_log`                      |
| Wire `notifications/cancelled` (v1.1)   | `server.py:run_server` + `dispatcher.cancel(...)` already there |

## Locked Invariants — Do Not Relitigate

These were vetted across the v1 implementation and review cycle. Treat as non-negotiable unless the spec changes.

1. **`call_tool` returns the full `CallToolResult` envelope.** Never just `content`. `isError` must round-trip.
2. **`session.send_notification` requires a TYPED `ServerNotification`** (or `ClientNotification` on the child side). Raw dicts will `AttributeError` on `model_dump`. Use `ServerNotification.model_validate({...})`.
3. **Background `tools/list_changed` goes through the `session_holder` closure.** Never `server.request_context.session` from a background task — it's a `ContextVar` and will `LookupError`.
4. **`ChildMcpClient.stop()` is idempotent and only re-raises `BaseException`.** Plain `Exception` from `aclose()` is logged and swallowed; `CancelledError` propagates.
5. **`ChildManager._set_state` catches `Exception` in the on-state-change callback.** The `state` attribute is the authoritative gate; the callback is fire-and-forget notification.
6. **`run_snowflake_query` failures surface via `ToolResult(is_error=True, ...)` — never raise.** Caps, timeouts, policy errors, connection failures all return the envelope.
7. **`cortex_analyst` whole-tool-failure on internal SQL failure** (spec §5.4 step 4). No partial `text`/`sql`/`results` payload when the routed `run_snowflake_query` returns `is_error=True`.
8. **Cancellation is v1 scaffolding.** `CancelToken`, `dispatcher.cancel()`, and the in-flight table exist and are tested but **NEVER called from a production path**. v1 bounding is `cursor.execute(timeout=…)` plus explicit `cursor.cancel()` on cap exceedance. Do not add `notifications/cancelled` handlers, asyncio watchdogs, or signal handlers that invoke `dispatcher.cancel()`.
9. **Upstream package is pinned at `snowflake-labs-mcp==1.4.1`.** Import name is `mcp_server_snowflake`. Bumping the pin requires re-running the child snapshot test and re-approving `tests/contract/fixtures/child_tools_list.json`.
10. **`OtherServices` invariants are enforced by Pydantic validators.** `query_manager`, `object_manager`, `semantic_manager` must all be `False`. Don't loosen this.
11. **`UnknownToolError` maps to JSON-RPC `-32602` (INVALID_PARAMS)** via `_unknown_tool_error`. Do not let it fall through to the generic `Exception` branch (which would surface as `-32603` internal error).
12. **Tempfile cleanup in `__main__`.** Child YAML contains Snowflake credentials. Always `chmod 0o600` and unlink in the same `try/finally` that wraps `chmod` + `write_child_config` + `run_server`.

## Core Principles

1. **Surface roborev findings, don't hide them.** The review cycle catches real bugs (resource leaks, cancellation gotchas, immutability violations). Read every finding, fix the legitimate ones, document the skipped ones.
2. **No `except BaseException` outside of teardown.** Even teardown should narrow to `Exception` if cancellation needs to propagate.
3. **Build payloads once.** Construct fully and return; don't mutate Pydantic models after construction. Use `model_copy(update={...})` if you need a variant.
4. **Tests pin contracts, not implementations.** Assert `error_kind == "cap_exceeded"` (stable) over substring-matching `error_message` (brittle). Pin object identity (`registered is captured`) when you mean identity, not just equality.
5. **TDD per task.** Write the failing test first, confirm it fails, then implement. The plan tasks are structured this way for a reason.
6. **One logical commit per plan task group.** Don't bundle unrelated changes; don't split a single task across multiple commits.

## Contributing

1. Read the design doc (`docs/superpowers/specs/2026-04-24-greybeam-mcp-design.md`) and the relevant plan task before touching code.
2. Use TDD. Write the failing test, run it (confirm RED), implement, run again (GREEN), then refactor.
3. Run `uv run pytest -q` and `uv run ruff check src/ tests/` before committing.
4. Commit messages: `<type>(<scope>): <one-line description>` followed by a body explaining the **why**. Types: `feat`, `fix`, `test`, `docs`, `chore`, `refactor`, `perf`, `ci`.
5. After committing, run `roborev wait <sha>` and `roborev show <sha>`. Triage every finding — typically 1–3 of ~5 are legitimate. Fix in a follow-up commit; never silently bypass.
6. Do **not** push to remote unless explicitly asked. Do **not** run `roborev fix` / `roborev refine` / `--fix` flags — they bypass user review.

## Cancellation Roadmap (v1.1)

The scaffolding to enable client-driven cancellation is already here:

- `CancelToken` (`cancel.py`) — threadsafe set-once primitive.
- `Dispatcher._in_flight` table keyed by request_id, populated synchronously before the first `await`.
- `Dispatcher.cancel(request_id)` — sets the token for owned calls; forwards `notifications/cancelled` to the child for delegated calls (with strong-ref fire-and-forget task tracking).
- `ChildMcpClient.send_notification` — typed `ClientNotification` wrapper.

To wire it: register a `notifications/cancelled` handler on the MCP server in `run_server` that pulls `params.requestId` and calls `dispatcher.cancel(str(...))`. Add an end-to-end contract test. The plumbing is unit-tested already.
