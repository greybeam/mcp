"""Open a fresh Snowflake-protocol connection to the Greybeam proxy.

Per spec §5.3, every owned-tool invocation gets a fresh connection to preserve
session isolation across tool calls (USE / BEGIN / COMMIT must not leak).
"""
from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

import snowflake.connector

from greybeam_mcp.config import GreybeamConfig, SnowflakeConfig


@contextmanager
def open_connection(
    sf: SnowflakeConfig, gb: GreybeamConfig
) -> Iterator[snowflake.connector.SnowflakeConnection]:
    auth_kwargs: dict[str, str] = {}
    if sf.password is not None:
        auth_kwargs["password"] = sf.password.get_secret_value()
    if sf.private_key is not None:
        auth_kwargs["private_key"] = sf.private_key.get_secret_value()
    if sf.authenticator:
        auth_kwargs["authenticator"] = sf.authenticator

    conn = snowflake.connector.connect(
        account=sf.account,
        user=sf.user,
        host=gb.proxy_host,
        client_session_keep_alive=False,
        **auth_kwargs,
    )
    try:
        yield conn
    finally:
        conn.close()
