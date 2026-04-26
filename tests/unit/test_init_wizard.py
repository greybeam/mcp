"""Tests for the interactive init wizard."""
from __future__ import annotations

from collections.abc import Iterator

import pytest
import yaml

from greybeam_mcp.config import load_config
from greybeam_mcp.init import run_wizard


def _scripted_input(answers: list[str]) -> tuple[Iterator[str], list[str]]:
    """Return (input_fn, prompts_seen).

    The fn pops answers in order; prompts_seen records what was asked so
    tests can assert on prompt text without coupling to spacing.
    """
    prompts: list[str] = []
    it = iter(answers)

    def _input(prompt: str) -> str:
        prompts.append(prompt)
        try:
            return next(it)
        except StopIteration as e:
            raise AssertionError(
                f"Wizard asked more questions than scripted; latest prompt: {prompt!r}"
            ) from e

    return _input, prompts  # type: ignore[return-value]


def test_keypair_no_passphrase_writes_loadable_yaml(tmp_path):
    out_path = tmp_path / "greybeam.yaml"
    key_path = tmp_path / "key.p8"
    key_path.write_text("dummy")

    answers = [
        "ABC12345-XY67890",                              # account
        "ABC12345-XY67890.demo.compute.greybeam.ai",     # proxy_host
        "agent_user",                                     # snowflake user
        "1",                                              # auth choice = keypair file
        str(key_path),                                    # key path
    ]
    input_fn, _ = _scripted_input(answers)

    def getpass_fn(prompt: str) -> str:
        return ""  # no passphrase

    written = run_wizard(
        input_fn=input_fn, getpass_fn=getpass_fn, out_path=out_path
    )

    assert written == out_path
    assert out_path.exists()
    assert oct(out_path.stat().st_mode & 0o777) == "0o600"

    cfg = load_config(out_path)
    assert cfg.snowflake.account == "ABC12345-XY67890"
    assert cfg.snowflake.user == "agent_user"
    assert cfg.snowflake.private_key_file == key_path
    assert cfg.snowflake.private_key_passphrase is None
    assert cfg.greybeam.proxy_host == "ABC12345-XY67890.demo.compute.greybeam.ai"


def test_keypair_with_passphrase_persisted(tmp_path):
    out_path = tmp_path / "greybeam.yaml"
    key_path = tmp_path / "key.p8"
    key_path.write_text("dummy")

    input_fn, _ = _scripted_input(
        ["acct-1", "acct-1.demo.compute.greybeam.ai", "u", "1", str(key_path)]
    )
    getpass_calls: list[str] = []

    def getpass_fn(prompt: str) -> str:
        getpass_calls.append(prompt)
        return "s3cret"

    run_wizard(input_fn=input_fn, getpass_fn=getpass_fn, out_path=out_path)

    cfg = load_config(out_path)
    assert cfg.snowflake.private_key_passphrase is not None
    assert cfg.snowflake.private_key_passphrase.get_secret_value() == "s3cret"
    assert getpass_calls, "passphrase should be read via getpass, not input"


def test_sso_writes_externalbrowser(tmp_path):
    out_path = tmp_path / "greybeam.yaml"
    input_fn, _ = _scripted_input(
        ["acct-1", "custom.proxy.example.com", "u", "2"]
    )

    run_wizard(
        input_fn=input_fn, getpass_fn=lambda p: "", out_path=out_path
    )

    cfg = load_config(out_path)
    assert cfg.snowflake.authenticator == "externalbrowser"
    assert cfg.snowflake.password is None
    assert cfg.greybeam.proxy_host == "custom.proxy.example.com"


def test_password_path_uses_getpass(tmp_path):
    out_path = tmp_path / "greybeam.yaml"
    input_fn, _ = _scripted_input(["acct-1", "acct-1.demo.compute.greybeam.ai", "u", "3"])
    getpass_calls: list[str] = []

    def getpass_fn(prompt: str) -> str:
        getpass_calls.append(prompt)
        return "hunter2"

    run_wizard(input_fn=input_fn, getpass_fn=getpass_fn, out_path=out_path)

    cfg = load_config(out_path)
    assert cfg.snowflake.password is not None
    assert cfg.snowflake.password.get_secret_value() == "hunter2"
    assert getpass_calls


def test_existing_file_requires_overwrite_confirmation(tmp_path):
    out_path = tmp_path / "greybeam.yaml"
    out_path.write_text("pre-existing")

    input_fn, _ = _scripted_input(
        # final 'n' declines overwrite
        ["acct-1", "acct-1.demo.compute.greybeam.ai", "u", "2", "n"]
    )

    with pytest.raises(SystemExit):
        run_wizard(
            input_fn=input_fn, getpass_fn=lambda p: "", out_path=out_path
        )

    # Original content untouched.
    assert out_path.read_text() == "pre-existing"


def test_existing_file_overwrite_when_confirmed(tmp_path):
    out_path = tmp_path / "greybeam.yaml"
    out_path.write_text("pre-existing")

    input_fn, _ = _scripted_input(
        ["acct-1", "acct-1.demo.compute.greybeam.ai", "u", "2", "y"]
    )

    run_wizard(
        input_fn=input_fn, getpass_fn=lambda p: "", out_path=out_path
    )

    cfg = load_config(out_path)
    assert cfg.snowflake.account == "acct-1"


def test_required_field_reprompts_on_empty(tmp_path):
    """Empty input on a required field re-prompts rather than crashing."""
    out_path = tmp_path / "greybeam.yaml"
    input_fn, prompts = _scripted_input(
        # blank then real account, then proxy, user, auth
        ["", "acct-1", "acct-1.demo.compute.greybeam.ai", "u", "2"]
    )

    run_wizard(
        input_fn=input_fn, getpass_fn=lambda p: "", out_path=out_path
    )

    cfg = load_config(out_path)
    assert cfg.snowflake.account == "acct-1"
    # Account should have been prompted at least twice (once empty, once real).
    account_prompts = [p for p in prompts if "account" in p.lower()]
    assert len(account_prompts) >= 2


def test_followup_uses_local_uv_form_in_source_checkout(tmp_path, capsys):
    """When invoked from a source checkout, the printed registration command
    must use `uv --directory <repo> run` so it works against an unpublished build."""
    out_path = tmp_path / "greybeam.yaml"
    input_fn, _ = _scripted_input(["acct-1", "acct-1.demo.compute.greybeam.ai", "u", "2"])
    run_wizard(
        input_fn=input_fn, getpass_fn=lambda p: "", out_path=out_path
    )
    captured = capsys.readouterr().out
    assert "uv --directory" in captured
    assert "run greybeam-mcp" in captured
    # The bare `uvx greybeam-mcp <config>` line (without `--directory`) should
    # not appear as the primary registration suggestion.
    primary_line = next(
        line for line in captured.splitlines() if "claude mcp add greybeam --" in line
    )
    assert "uv --directory" in primary_line


def test_followup_uses_uvx_form_when_installed(tmp_path, capsys, monkeypatch):
    """When invoked from an installed package (no pyproject.toml above init.py),
    the printed registration command must use `uvx greybeam-mcp`."""
    monkeypatch.setattr(
        "greybeam_mcp.init._detect_source_repo", lambda: None
    )
    out_path = tmp_path / "greybeam.yaml"
    input_fn, _ = _scripted_input(["acct-1", "acct-1.demo.compute.greybeam.ai", "u", "2"])
    run_wizard(
        input_fn=input_fn, getpass_fn=lambda p: "", out_path=out_path
    )
    captured = capsys.readouterr().out
    assert "uvx greybeam-mcp" in captured
    assert "uv --directory" not in captured


def test_yaml_locked_invariants_preserved(tmp_path):
    """Wizard output must satisfy the analyst/agent/other_services locks."""
    out_path = tmp_path / "greybeam.yaml"
    input_fn, _ = _scripted_input(["acct-1", "acct-1.demo.compute.greybeam.ai", "u", "2"])
    run_wizard(
        input_fn=input_fn, getpass_fn=lambda p: "", out_path=out_path
    )
    raw = yaml.safe_load(out_path.read_text())
    assert raw["snowflake"]["analyst_services"] == []
    assert raw["snowflake"]["agent_services"] == []
    assert raw["snowflake"]["other_services"] == {
        "query_manager": False,
        "object_manager": False,
        "semantic_manager": False,
    }
