"""Pydantic-validated configuration for Greybeam MCP.

Secrets are read from environment variables; structured config from a YAML file.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, SecretStr, field_validator, model_validator


class CortexSearchService(BaseModel):
    service_name: str
    description: str
    database_name: str
    schema_name: str


class OtherServices(BaseModel):
    query_manager: bool
    object_manager: bool
    semantic_manager: bool

    @model_validator(mode="after")
    def enforce_invariants(self) -> "OtherServices":
        if self.query_manager:
            raise ValueError("query_manager must be False (Greybeam owns run_snowflake_query)")
        if self.object_manager:
            raise ValueError("object_manager must be False (out of scope in v1)")
        if self.semantic_manager:
            raise ValueError("semantic_manager must be False (out of scope in v1)")
        return self


class SnowflakeConfig(BaseModel):
    account: str
    user: str
    password: SecretStr | None = None
    private_key: SecretStr | None = None
    private_key_file: Path | None = None
    private_key_passphrase: SecretStr | None = None
    authenticator: str | None = None
    search_services: list[CortexSearchService] = Field(default_factory=list)
    analyst_services: list[Any] = Field(default_factory=list)
    agent_services: list[Any] = Field(default_factory=list)
    other_services: OtherServices

    @model_validator(mode="after")
    def enforce_locked_lists(self) -> "SnowflakeConfig":
        if self.analyst_services:
            raise ValueError("analyst_services must be empty (Greybeam owns cortex_analyst)")
        if self.agent_services:
            raise ValueError("agent_services must be empty (cortex_agent disabled in v1)")
        return self

    @model_validator(mode="after")
    def require_auth_method(self) -> "SnowflakeConfig":
        if not (
            self.password
            or self.private_key
            or self.private_key_file
            or self.authenticator
        ):
            raise ValueError(
                "snowflake auth required: set one of password, private_key, "
                "private_key_file, or authenticator (via YAML or SNOWFLAKE_PASSWORD / "
                "SNOWFLAKE_PRIVATE_KEY / SNOWFLAKE_PRIVATE_KEY_FILE / "
                "SNOWFLAKE_AUTHENTICATOR env)"
            )
        return self


class RestartPolicy(BaseModel):
    max_attempts: int = Field(ge=1)
    backoff_seconds: list[float] = Field(min_length=1)
    jitter: bool

    @field_validator("backoff_seconds")
    @classmethod
    def positive(cls, v: list[float]) -> list[float]:
        if any(x <= 0 for x in v):
            raise ValueError("backoff_seconds entries must be positive")
        return v


class GreybeamConfig(BaseModel):
    proxy_host: str
    row_cap: int = Field(gt=0)
    byte_cap: int = Field(gt=0)
    query_timeout: int = Field(gt=0)
    child_restart_policy: RestartPolicy
    cortex_search_required: bool
    log_sql: bool = False


class Config(BaseModel):
    snowflake: SnowflakeConfig
    greybeam: GreybeamConfig


def _inject_env(snowflake_data: dict[str, Any]) -> dict[str, Any]:
    """Snowflake credentials live in env vars; merge them into the dict.

    Env vars take lower precedence than YAML (setdefault), so an operator can
    override the env by setting the field explicitly in YAML.
    """
    if user := os.environ.get("SNOWFLAKE_USER"):
        snowflake_data.setdefault("user", user)
    if not snowflake_data.get("user"):
        raise ValueError(
            "SNOWFLAKE_USER environment variable is required "
            "(or set snowflake.user in YAML)"
        )

    if pw := os.environ.get("SNOWFLAKE_PASSWORD"):
        snowflake_data.setdefault("password", pw)
    if pk := os.environ.get("SNOWFLAKE_PRIVATE_KEY"):
        snowflake_data.setdefault("private_key", pk)
    if pkf := os.environ.get("SNOWFLAKE_PRIVATE_KEY_FILE"):
        snowflake_data.setdefault("private_key_file", pkf)
    if pkp := os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"):
        snowflake_data.setdefault("private_key_passphrase", pkp)
    if auth := os.environ.get("SNOWFLAKE_AUTHENTICATOR"):
        snowflake_data.setdefault("authenticator", auth)
    return snowflake_data


def load_config(path: Path) -> Config:
    raw = yaml.safe_load(path.read_text())
    if not isinstance(raw, dict):
        raise ValueError(f"Config at {path} must be a YAML mapping")
    raw["snowflake"] = _inject_env(raw.get("snowflake") or {})
    return Config.model_validate(raw)
