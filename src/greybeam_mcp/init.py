"""Interactive setup wizard for Greybeam MCP.

Prompts for account, user, proxy host, and an authentication method,
then writes a self-contained YAML config (mode 0o600) and prints the
exact registration commands for Claude Code and Claude Desktop.

Pure I/O is injected (input_fn / getpass_fn / out_stream) so the wizard
is unit-testable.
"""
from __future__ import annotations

import getpass as _getpass
import json
import os
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any, TextIO

import yaml

DEFAULT_CONFIG_PATH = Path.home() / ".config" / "greybeam-mcp.yaml"


def _prompt(
    label: str,
    default: str | None,
    input_fn: Callable[[str], str],
) -> str:
    suffix = f" [{default}]: " if default is not None else ": "
    val = input_fn(f"{label}{suffix}").strip()
    if not val and default is not None:
        return default
    return val


def _prompt_required(label: str, input_fn: Callable[[str], str]) -> str:
    while True:
        val = input_fn(f"{label}: ").strip()
        if val:
            return val
        print("  required.")


def _prompt_existing_file(
    label: str, input_fn: Callable[[str], str], out: TextIO
) -> str:
    """Prompt for a file path, re-prompting until it resolves to a real file."""
    while True:
        raw = _prompt_required(label, input_fn)
        resolved = Path(raw).expanduser().resolve()
        if resolved.is_file():
            return str(resolved)
        print(f"  not a file: {resolved}", file=out)


def _prompt_passphrase_with_confirmation(
    getpass_fn: Callable[[str], str], out: TextIO
) -> str:
    """Prompt for a passphrase twice and re-prompt on mismatch.

    Empty input (unencrypted key) is accepted on the first prompt without
    asking for confirmation — re-typing nothing adds no value.
    """
    while True:
        first = getpass_fn("  Passphrase (blank if unencrypted): ")
        if not first:
            return ""
        second = getpass_fn("  Confirm passphrase: ")
        if first == second:
            return first
        print("  passphrases did not match — try again.", file=out)


def _prompt_auth(
    input_fn: Callable[[str], str],
    getpass_fn: Callable[[str], str],
    out: TextIO,
) -> dict[str, Any]:
    print(file=out)
    print("Authentication method:", file=out)
    print("  1) Key-pair file (recommended)", file=out)
    print("  2) SSO via browser (requires SAML2 integration on the account)", file=out)
    print("  3) Password (deprecated by Snowflake)", file=out)
    while True:
        choice = input_fn("Choose [1]: ").strip() or "1"
        if choice in {"1", "2", "3"}:
            break
        print("  enter 1, 2, or 3.", file=out)

    if choice == "1":
        path = _prompt_existing_file("  Path to private key (.p8)", input_fn, out)
        passphrase = _prompt_passphrase_with_confirmation(getpass_fn, out)
        result: dict[str, Any] = {"private_key_file": path}
        if passphrase:
            result["private_key_passphrase"] = passphrase
        return result

    if choice == "2":
        return {"authenticator": "externalbrowser"}

    return {"password": getpass_fn("  Password: ")}


def _build_payload(
    *, account: str, user: str, proxy_host: str, auth: dict[str, Any]
) -> dict[str, Any]:
    return {
        "snowflake": {
            "account": account,
            "user": user,
            **auth,
            "search_services": [],
            "analyst_services": [],
            "agent_services": [],
            "other_services": {
                "query_manager": False,
                "object_manager": False,
                "semantic_manager": False,
            },
        },
        "greybeam": {
            "proxy_host": proxy_host,
            "row_cap": 10000,
            "byte_cap": 10000000,
            "query_timeout": 300,
            "child_restart_policy": {
                "max_attempts": 3,
                "backoff_seconds": [1, 4, 16],
                "jitter": True,
            },
            "cortex_search_required": False,
            "log_sql": False,
        },
    }


def _detect_source_repo() -> Path | None:
    """Return the repo root if init.py is running from a source checkout.

    When invoked via `uvx greybeam-mcp init` (installed package), this returns
    None and we recommend `uvx`. When invoked via `uv run greybeam-mcp init`
    from a clone, we recommend `uv --directory <repo> run` so the wizard's
    output works against an unpublished build.
    """
    candidate = Path(__file__).resolve().parent.parent.parent
    if (candidate / "pyproject.toml").exists():
        return candidate
    return None


def _print_followups(
    config_path: Path,
    out: TextIO,
    repo_root: Path | None,
) -> None:
    print(file=out)
    print(f"Wrote config: {config_path}", file=out)
    print(file=out)
    print("Register with Claude Code:", file=out)

    if repo_root is not None:
        print(
            f"  claude mcp add greybeam -- uv --directory {repo_root} "
            f"run greybeam-mcp --config {config_path}",
            file=out,
        )
        print(file=out)
        print(
            "(source checkout detected. After publishing to PyPI, the simpler form is:",
            file=out,
        )
        print(
            f"   claude mcp add greybeam -- uvx greybeam-mcp --config {config_path})",
            file=out,
        )
    else:
        print(
            f"  claude mcp add greybeam -- uvx greybeam-mcp --config {config_path}",
            file=out,
        )

    print(file=out)
    print("Or for Claude Desktop, add this to claude_desktop_config.json:", file=out)
    if repo_root is not None:
        desktop_entry = {
            "command": "uv",
            "args": [
                "--directory",
                str(repo_root),
                "run",
                "greybeam-mcp",
                "--config",
                str(config_path),
            ],
        }
    else:
        desktop_entry = {
            "command": "uvx",
            "args": ["greybeam-mcp", "--config", str(config_path)],
        }
    print(json.dumps({"mcpServers": {"greybeam": desktop_entry}}, indent=2), file=out)


def run_wizard(
    *,
    input_fn: Callable[[str], str] = input,
    getpass_fn: Callable[[str], str] = _getpass.getpass,
    out_path: Path | None = None,
    out_stream: TextIO | None = None,
) -> Path:
    out = out_stream or sys.stdout
    print("Greybeam MCP setup", file=out)
    print("------------------", file=out)

    account = _prompt_required(
        "Snowflake account locator (e.g. ABC12345-XY67890)", input_fn
    )
    proxy_host = _prompt_required(
        "Greybeam proxy host (e.g. ABC12345-XY67890.demo.compute.greybeam.ai)",
        input_fn,
    )
    user = _prompt_required("Snowflake user", input_fn)

    auth = _prompt_auth(input_fn, getpass_fn, out)

    if out_path is None:
        target_str = _prompt(
            "Save config to", str(DEFAULT_CONFIG_PATH), input_fn
        )
        target = Path(target_str).expanduser()
    else:
        target = out_path

    if target.exists():
        confirm = input_fn(f"  {target} exists. Overwrite? [y/N]: ").strip().lower()
        if confirm != "y":
            print("Aborted.", file=out)
            sys.exit(1)

    payload = _build_payload(
        account=account, user=user, proxy_host=proxy_host, auth=auth
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    # Create with 0o600 atomically — no chmod-after-write window where the
    # credentials file might exist with broader perms under permissive umasks.
    fd = os.open(
        target, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600
    )
    with os.fdopen(fd, "w") as f:
        yaml.safe_dump(payload, f, sort_keys=False)

    _print_followups(target, out, _detect_source_repo())
    return target
