import argparse
import logging
from pathlib import Path

from greybeam_mcp.config import load_config
from greybeam_mcp.logging_setup import setup_logging

log = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(prog="greybeam-mcp")
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    setup_logging(level=args.log_level)
    cfg = load_config(args.config)

    # Server runtime is wired in a later commit (registry + child manager,
    # then stdio forwarding). For now, log a structured partial-install
    # message and raise so the failure is obvious.
    log.warning(
        "server_bootstrap_partial",
        extra={"account": cfg.snowflake.account},
    )
    raise NotImplementedError("server runtime not yet wired")


if __name__ == "__main__":
    main()
