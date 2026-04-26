"""Regression guard for the upstream Snowflake MCP child invocation.

The wrapper spawns ``snowflake-labs-mcp`` via stdio. The package does not
expose a ``__main__`` module, so ``python -m mcp_server_snowflake`` fails
silently in production while CI happily ships. This test pins the default
invocation in ``greybeam_mcp.__main__`` to one that actually imports the
upstream entry point, so a future upstream rename or repackage breaks
this test instead of the user's MCP startup.
"""
from __future__ import annotations

import subprocess
import sys


def _default_upstream_args() -> list[str]:
    """Extract the default upstream args from the CLI entrypoint.

    Re-parses ``main`` rather than hard-coding a copy here so the test
    couples to the production default. Anyone editing the default must
    keep it importable, or this test will fail.
    """
    import inspect

    from greybeam_mcp import __main__ as entry

    src = inspect.getsource(entry.main)
    # Sentinel guards us against accidental drift to ``-m`` or any other
    # form that bypasses entry-point resolution.
    assert "from mcp_server_snowflake import main; main()" in src, (
        "Default upstream invocation must import the entry point via -c. "
        "snowflake-labs-mcp has no __main__ module; -m would fail."
    )
    return ["-c", "from mcp_server_snowflake import main; main()"]


def test_upstream_entry_point_is_importable() -> None:
    """The upstream package must expose ``main`` at the package root.

    This catches regressions where snowflake-labs-mcp is bumped to a
    version that moves or renames the entry point.
    """
    from mcp_server_snowflake import main  # noqa: F401


def test_default_upstream_args_run_under_current_interpreter() -> None:
    """The default ``-c`` payload must be syntactically valid and resolve.

    Runs the same ``python -c`` payload greybeam-mcp uses to spawn the
    child, but with ``--help`` so the upstream exits cleanly without
    needing real Snowflake credentials.
    """
    args = _default_upstream_args()
    result = subprocess.run(
        [sys.executable, *args, "--help"],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, (
        f"Upstream invocation failed.\nstdout={result.stdout}\n"
        f"stderr={result.stderr}"
    )
