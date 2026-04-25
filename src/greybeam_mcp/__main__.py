import argparse
from pathlib import Path

from greybeam_mcp.config import load_config
from greybeam_mcp.logging_setup import setup_logging


def main() -> None:
    parser = argparse.ArgumentParser(prog="greybeam-mcp")
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    setup_logging(level=args.log_level)
    cfg = load_config(args.config)

    # Server runtime is wired in a later commit (registry + child manager,
    # then stdio forwarding). For now, fail loudly so partial installs are
    # obvious.
    raise NotImplementedError(
        f"Server runtime not yet wired. Loaded config for account={cfg.snowflake.account}."
    )


if __name__ == "__main__":
    main()
