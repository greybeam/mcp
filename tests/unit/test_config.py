import os
from pathlib import Path

import pytest
import yaml

from greybeam_mcp.config import Config, RestartPolicy, load_config


def write_yaml(tmp_path: Path, data: dict) -> Path:
    path = tmp_path / "greybeam.yaml"
    path.write_text(yaml.safe_dump(data))
    return path


def test_load_minimal_valid_config(tmp_path, monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_USER", "agent")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "pw")
    path = write_yaml(
        tmp_path,
        {
            "snowflake": {
                "account": "abc-xyz",
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
                "proxy_host": "greybeam.example.com",
                "row_cap": 10000,
                "byte_cap": 10_000_000,
                "query_timeout": 300,
                "child_restart_policy": {
                    "max_attempts": 3,
                    "backoff_seconds": [1, 4, 16],
                    "jitter": True,
                },
                "cortex_search_required": True,
                "log_sql": False,
            },
        },
    )

    cfg = load_config(path)

    assert cfg.snowflake.account == "abc-xyz"
    assert cfg.greybeam.proxy_host == "greybeam.example.com"
    assert cfg.greybeam.row_cap == 10000
    assert cfg.greybeam.child_restart_policy.max_attempts == 3
    assert cfg.snowflake.user == "agent"  # pulled from env


def test_invariant_locks_query_manager_false(tmp_path, monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_USER", "agent")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "pw")
    path = write_yaml(
        tmp_path,
        {
            "snowflake": {
                "account": "abc-xyz",
                "search_services": [],
                "analyst_services": [],
                "agent_services": [],
                "other_services": {
                    "query_manager": True,  # violates invariant
                    "object_manager": False,
                    "semantic_manager": False,
                },
            },
            "greybeam": {
                "proxy_host": "greybeam.example.com",
                "row_cap": 10000,
                "byte_cap": 10_000_000,
                "query_timeout": 300,
                "child_restart_policy": {
                    "max_attempts": 3,
                    "backoff_seconds": [1, 4, 16],
                    "jitter": True,
                },
                "cortex_search_required": True,
                "log_sql": False,
            },
        },
    )

    with pytest.raises(ValueError, match="query_manager must be False"):
        load_config(path)


def test_missing_snowflake_user_env_raises(tmp_path, monkeypatch):
    monkeypatch.delenv("SNOWFLAKE_USER", raising=False)
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "pw")
    path = write_yaml(
        tmp_path,
        {
            "snowflake": {
                "account": "abc-xyz",
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
                "proxy_host": "greybeam.example.com",
                "row_cap": 10000,
                "byte_cap": 10_000_000,
                "query_timeout": 300,
                "child_restart_policy": {
                    "max_attempts": 3,
                    "backoff_seconds": [1, 4, 16],
                    "jitter": True,
                },
                "cortex_search_required": True,
                "log_sql": False,
            },
        },
    )

    with pytest.raises(ValueError, match="SNOWFLAKE_USER"):
        load_config(path)


def test_yaml_user_satisfies_requirement_without_env(tmp_path, monkeypatch):
    """If snowflake.user is set in YAML, SNOWFLAKE_USER env is not required."""
    monkeypatch.delenv("SNOWFLAKE_USER", raising=False)
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "pw")
    path = write_yaml(
        tmp_path,
        {
            "snowflake": {
                "account": "abc-xyz",
                "user": "yaml-user",
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
                "proxy_host": "greybeam.example.com",
                "row_cap": 10000,
                "byte_cap": 10_000_000,
                "query_timeout": 300,
                "child_restart_policy": {
                    "max_attempts": 3,
                    "backoff_seconds": [1, 4, 16],
                    "jitter": True,
                },
                "cortex_search_required": True,
                "log_sql": False,
            },
        },
    )

    cfg = load_config(path)
    assert cfg.snowflake.user == "yaml-user"


def test_missing_all_auth_methods_raises(tmp_path, monkeypatch):
    """At least one of password, private_key, or authenticator must be set."""
    monkeypatch.setenv("SNOWFLAKE_USER", "agent")
    monkeypatch.delenv("SNOWFLAKE_PASSWORD", raising=False)
    monkeypatch.delenv("SNOWFLAKE_PRIVATE_KEY", raising=False)
    monkeypatch.delenv("SNOWFLAKE_AUTHENTICATOR", raising=False)
    path = write_yaml(
        tmp_path,
        {
            "snowflake": {
                "account": "abc-xyz",
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
                "proxy_host": "greybeam.example.com",
                "row_cap": 10000,
                "byte_cap": 10_000_000,
                "query_timeout": 300,
                "child_restart_policy": {
                    "max_attempts": 3,
                    "backoff_seconds": [1, 4, 16],
                    "jitter": True,
                },
                "cortex_search_required": True,
                "log_sql": False,
            },
        },
    )

    with pytest.raises(ValueError, match="snowflake auth required"):
        load_config(path)
