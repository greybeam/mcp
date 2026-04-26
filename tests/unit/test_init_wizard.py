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


def test_config_file_created_via_os_open_with_restrictive_mode(tmp_path):
    """Verify the implementation uses os.open with 0o600, not write_text+chmod.

    Asserts the wizard never calls path.chmod for the output file (defense
    against re-introducing the chmod-after-write window).
    """
    from unittest.mock import patch

    out_path = tmp_path / "greybeam.yaml"
    input_fn, _ = _scripted_input(
        ["acct-1", "acct-1.demo.compute.greybeam.ai", "u", "2"]
    )

    real_open = __import__("os").open
    open_calls: list[tuple] = []

    def tracking_open(path, flags, mode=0o777, *args, **kwargs):
        open_calls.append((str(path), flags, mode))
        return real_open(path, flags, mode, *args, **kwargs)

    with patch("greybeam_mcp.init.os.open", side_effect=tracking_open):
        run_wizard(
            input_fn=input_fn, getpass_fn=lambda p: "", out_path=out_path
        )

    # The output file must have been opened with O_CREAT + 0o600.
    import os as _os
    matching = [c for c in open_calls if c[0] == str(out_path)]
    assert matching, "wizard did not open output via os.open"
    _, flags, mode = matching[-1]
    assert flags & _os.O_CREAT
    assert mode == 0o600
    assert oct(out_path.stat().st_mode & 0o777) == "0o600"


def test_keypair_reprompts_when_key_path_does_not_exist(tmp_path):
    """A typo in the key path should re-prompt, not silently produce broken YAML."""
    out_path = tmp_path / "greybeam.yaml"
    real_key = tmp_path / "key.p8"
    real_key.write_text("dummy")
    bogus = str(tmp_path / "does-not-exist.p8")

    answers = [
        "acct-1",
        "acct-1.demo.compute.greybeam.ai",
        "u",
        "1",       # keypair
        bogus,     # bogus path — wizard must re-prompt
        str(real_key),
    ]
    input_fn, prompts = _scripted_input(answers)

    run_wizard(
        input_fn=input_fn, getpass_fn=lambda p: "", out_path=out_path
    )

    cfg = load_config(out_path)
    assert cfg.snowflake.private_key_file == real_key
    # The key-path prompt should have appeared at least twice.
    key_prompts = [p for p in prompts if "key" in p.lower()]
    assert len(key_prompts) >= 2


def test_keypair_passphrase_reprompts_on_mismatch(tmp_path):
    """A mismatched passphrase confirmation must re-prompt rather than persist a typo."""
    out_path = tmp_path / "greybeam.yaml"
    key_path = tmp_path / "key.p8"
    key_path.write_text("dummy")

    input_fn, _ = _scripted_input(
        ["acct-1", "acct-1.demo.compute.greybeam.ai", "u", "1", str(key_path)]
    )

    # First two getpass calls mismatch, next two match.
    pw_iter = iter(["typo-1", "typo-2", "correct", "correct"])

    def getpass_fn(prompt: str) -> str:
        return next(pw_iter)

    run_wizard(
        input_fn=input_fn, getpass_fn=getpass_fn, out_path=out_path
    )

    cfg = load_config(out_path)
    assert cfg.snowflake.private_key_passphrase is not None
    assert cfg.snowflake.private_key_passphrase.get_secret_value() == "correct"


def test_keypair_empty_passphrase_is_not_confirmed(tmp_path):
    """Empty passphrase (unencrypted key) should NOT trigger a confirmation prompt."""
    out_path = tmp_path / "greybeam.yaml"
    key_path = tmp_path / "key.p8"
    key_path.write_text("dummy")

    input_fn, _ = _scripted_input(
        ["acct-1", "acct-1.demo.compute.greybeam.ai", "u", "1", str(key_path)]
    )

    getpass_calls: list[str] = []

    def getpass_fn(prompt: str) -> str:
        getpass_calls.append(prompt)
        return ""  # no passphrase

    run_wizard(
        input_fn=input_fn, getpass_fn=getpass_fn, out_path=out_path
    )

    # Exactly one passphrase prompt — no confirmation needed when empty.
    assert len(getpass_calls) == 1
    cfg = load_config(out_path)
    assert cfg.snowflake.private_key_passphrase is None


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
