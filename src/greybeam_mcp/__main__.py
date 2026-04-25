"""CLI entrypoint for the Greybeam MCP server.

Spawns the upstream Snowflake MCP child via stdio. The default
``--upstream-command`` is the current Python interpreter and the
default upstream module is ``mcp_server_snowflake`` (the import name
of the ``snowflake-labs-mcp`` package). Both are overridable for
integration testing or alternative deployments.
"""
from __future__ import annotations

import argparse
import asyncio
import sys
import tempfile
from pathlib import Path

from greybeam_mcp.child.config_writer import write_child_config
from greybeam_mcp.config import load_config
from greybeam_mcp.logging_setup import setup_logging
from greybeam_mcp.server import run_server


def main() -> None:
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
            "['-m', 'mcp_server_snowflake'] when not specified."
        ),
    )
    args = parser.parse_args()

    setup_logging(level=args.log_level)
    cfg = load_config(args.config)

    with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as f:
        child_config_path = Path(f.name)
    write_child_config(cfg.snowflake, child_config_path)

    upstream_args_base = args.upstream_arg or ["-m", "mcp_server_snowflake"]
    upstream_args = [
        *upstream_args_base,
        "--service-config-file",
        str(child_config_path),
    ]
    asyncio.run(run_server(cfg, args.upstream_command, upstream_args))


if __name__ == "__main__":
    main()
