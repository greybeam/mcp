from unittest.mock import MagicMock, patch

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from greybeam_mcp.config import GreybeamConfig, RestartPolicy, SnowflakeConfig, OtherServices
from greybeam_mcp.greybeam.connection import open_connection


def _gen_rsa_pem(passphrase: bytes | None = None) -> bytes:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    enc = (
        serialization.BestAvailableEncryption(passphrase)
        if passphrase
        else serialization.NoEncryption()
    )
    return key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=enc,
    )


def _base_gb() -> GreybeamConfig:
    return GreybeamConfig(
        proxy_host="greybeam.example.com",
        row_cap=10,
        byte_cap=1000,
        query_timeout=30,
        child_restart_policy=RestartPolicy(
            max_attempts=3, backoff_seconds=[1, 4, 16], jitter=True
        ),
        cortex_search_required=True,
    )


def _base_other() -> OtherServices:
    return OtherServices(
        query_manager=False, object_manager=False, semantic_manager=False
    )


@pytest.fixture
def configs():
    sf = SnowflakeConfig(
        account="abc-xyz",
        user="agent",
        password="pw",
        analyst_services=[],
        agent_services=[],
        other_services=OtherServices(
            query_manager=False, object_manager=False, semantic_manager=False
        ),
    )
    gb = GreybeamConfig(
        proxy_host="greybeam.example.com",
        row_cap=10,
        byte_cap=1000,
        query_timeout=30,
        child_restart_policy=RestartPolicy(
            max_attempts=3, backoff_seconds=[1, 4, 16], jitter=True
        ),
        cortex_search_required=True,
    )
    return sf, gb


def test_open_connection_targets_greybeam_host(configs):
    sf, gb = configs
    with patch("greybeam_mcp.greybeam.connection.snowflake.connector.connect") as mock_connect:
        mock_connect.return_value = MagicMock()
        with open_connection(sf, gb) as conn:
            assert conn is mock_connect.return_value
        kwargs = mock_connect.call_args.kwargs
        assert kwargs["host"] == "greybeam.example.com"
        assert kwargs["account"] == "abc-xyz"
        assert kwargs["user"] == "agent"
        assert kwargs["password"] == "pw"


def test_open_connection_closes_on_exit(configs):
    sf, gb = configs
    with patch("greybeam_mcp.greybeam.connection.snowflake.connector.connect") as mock_connect:
        conn = MagicMock()
        mock_connect.return_value = conn
        with open_connection(sf, gb):
            pass
        conn.close.assert_called_once()


def test_open_connection_with_private_key_file_passes_der_bytes(tmp_path):
    """Connector receives DER bytes loaded from the file, not the file path."""
    pem = _gen_rsa_pem()
    key_path = tmp_path / "key.p8"
    key_path.write_bytes(pem)

    sf = SnowflakeConfig(
        account="abc-xyz",
        user="agent",
        private_key_file=key_path,
        analyst_services=[],
        agent_services=[],
        other_services=_base_other(),
    )
    gb = _base_gb()

    with patch("greybeam_mcp.greybeam.connection.snowflake.connector.connect") as mock_connect:
        mock_connect.return_value = MagicMock()
        with open_connection(sf, gb):
            pass
        kwargs = mock_connect.call_args.kwargs
        pk = kwargs["private_key"]
        assert isinstance(pk, bytes)
        # DER PKCS8 starts with SEQUENCE tag 0x30
        assert pk[0] == 0x30
        # Driver must NOT see a path string under private_key.
        assert "private_key_file" not in kwargs


def test_open_connection_with_encrypted_key_decrypts(tmp_path):
    pem = _gen_rsa_pem(passphrase=b"s3cret")
    key_path = tmp_path / "key.p8"
    key_path.write_bytes(pem)

    sf = SnowflakeConfig(
        account="abc-xyz",
        user="agent",
        private_key_file=key_path,
        private_key_passphrase="s3cret",
        analyst_services=[],
        agent_services=[],
        other_services=_base_other(),
    )
    gb = _base_gb()

    with patch("greybeam_mcp.greybeam.connection.snowflake.connector.connect") as mock_connect:
        mock_connect.return_value = MagicMock()
        with open_connection(sf, gb):
            pass
        pk = mock_connect.call_args.kwargs["private_key"]
        assert isinstance(pk, bytes) and pk[0] == 0x30


def test_open_connection_with_encrypted_key_wrong_passphrase_raises(tmp_path):
    pem = _gen_rsa_pem(passphrase=b"correct")
    key_path = tmp_path / "key.p8"
    key_path.write_bytes(pem)

    sf = SnowflakeConfig(
        account="abc-xyz",
        user="agent",
        private_key_file=key_path,
        private_key_passphrase="wrong",
        analyst_services=[],
        agent_services=[],
        other_services=_base_other(),
    )
    gb = _base_gb()

    with patch("greybeam_mcp.greybeam.connection.snowflake.connector.connect"):
        with pytest.raises(Exception):  # cryptography raises on bad passphrase
            with open_connection(sf, gb):
                pass


def test_open_connection_pem_text_private_key_loads_to_der(tmp_path):
    """The legacy SNOWFLAKE_PRIVATE_KEY (PEM text) path must also produce DER bytes."""
    pem = _gen_rsa_pem()

    sf = SnowflakeConfig(
        account="abc-xyz",
        user="agent",
        private_key=pem.decode(),
        analyst_services=[],
        agent_services=[],
        other_services=_base_other(),
    )
    gb = _base_gb()

    with patch("greybeam_mcp.greybeam.connection.snowflake.connector.connect") as mock_connect:
        mock_connect.return_value = MagicMock()
        with open_connection(sf, gb):
            pass
        pk = mock_connect.call_args.kwargs["private_key"]
        assert isinstance(pk, bytes) and pk[0] == 0x30
