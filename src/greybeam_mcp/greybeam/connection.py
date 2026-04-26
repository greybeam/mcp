"""Open a fresh Snowflake-protocol connection to the Greybeam proxy.

Per spec §5.3, every owned-tool invocation gets a fresh connection to preserve
session isolation across tool calls (USE / BEGIN / COMMIT must not leak).
"""
from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator

import snowflake.connector
from cryptography.hazmat.primitives import serialization

from greybeam_mcp.config import GreybeamConfig, SnowflakeConfig


def _pem_to_der(pem: bytes, passphrase: bytes | None) -> bytes:
    """Load a PEM-encoded private key and return DER PKCS8 bytes.

    The Snowflake connector expects ``private_key`` to be DER bytes, not raw
    PEM text. Encrypted keys are decrypted here using the supplied passphrase.
    """
    key = serialization.load_pem_private_key(pem, password=passphrase)
    return key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


@contextmanager
def open_connection(
    sf: SnowflakeConfig, gb: GreybeamConfig
) -> Iterator[snowflake.connector.SnowflakeConnection]:
    auth_kwargs: dict[str, Any] = {}
    if sf.password is not None:
        auth_kwargs["password"] = sf.password.get_secret_value()

    passphrase: bytes | None = (
        sf.private_key_passphrase.get_secret_value().encode()
        if sf.private_key_passphrase is not None
        else None
    )
    if sf.private_key_file is not None:
        pem = sf.private_key_file.read_bytes()
        auth_kwargs["private_key"] = _pem_to_der(pem, passphrase)
    elif sf.private_key is not None:
        auth_kwargs["private_key"] = _pem_to_der(
            sf.private_key.get_secret_value().encode(), passphrase
        )

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
