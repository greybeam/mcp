"""CLI entrypoint for the Greybeam MCP server.

Spawns the upstream Snowflake MCP child via stdio. The default
``--upstream-command`` is the current Python interpreter and the
default ``--upstream-arg`` invocation imports and calls
``mcp_server_snowflake.main`` (the console-script entry point of the
``snowflake-labs-mcp`` package, which does not expose a ``__main__``
module). Both are overridable for integration testing or alternative
deployments.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
import tempfile
from pathlib import Path

from greybeam_mcp.child.config_writer import write_child_config
from greybeam_mcp.config import load_config
from greybeam_mcp.logging_setup import setup_logging
from greybeam_mcp.server import run_server


def main() -> None:
    # `greybeam-mcp init` is a sibling subcommand to the default serve flow.
    # Detect it before argparse runs so the existing --config-required parser
    # stays backward compatible for the serve path.
    if len(sys.argv) >= 2 and sys.argv[1] == "init":
        from greybeam_mcp.init import run_wizard

        run_wizard()
        return

    parser = argparse.ArgumentParser(prog="greybeam-mcp")
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument(
        "--upstream-command",
        default=sys.executable,
        help="Command to spawn the upstream Snowflake MCP child.",
    )
    parser.add_argument(
        "--upstream-arg",
        action="append",
        default=None,
        help=(
            "Repeatable args for the upstream command. Defaults to "
            "['-c', 'from mcp_server_snowflake import main; main()'] "
            "when not specified."
        ),
    )
    args = parser.parse_args()

    setup_logging(level=args.log_level)
    cfg = load_config(args.config)

    # The child config contains Snowflake credentials. Write with 0o600
    # perms and unlink unconditionally on shutdown — covering chmod,
    # write_child_config, and run_server in the same try so a partial
    # credential file is never left behind.
    with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as f:
        child_config_path = Path(f.name)
    try:
        os.chmod(child_config_path, 0o600)
        write_child_config(cfg.snowflake, child_config_path)

        # snowflake-labs-mcp ships a console-script entry point but no
        # ``__main__``, so ``-m mcp_server_snowflake`` fails. Invoke the
        # entry-point function directly via ``-c`` for portability.
        upstream_args_base = args.upstream_arg or [
            "-c",
            "from mcp_server_snowflake import main; main()",
        ]
        upstream_args = [
            *upstream_args_base,
            "--service-config-file",
            str(child_config_path),
        ]
        asyncio.run(run_server(cfg, args.upstream_command, upstream_args))
    finally:
        child_config_path.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
