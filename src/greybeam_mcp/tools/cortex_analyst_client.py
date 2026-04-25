"""Async HTTP client for Snowflake's Cortex Analyst REST API.

Talks directly to the customer's real Snowflake account URL (NOT Greybeam).
Per spec §5.4, only SQL execution returned by Analyst is routed through
Greybeam — the Analyst API call itself is between us and Snowflake.
"""
from __future__ import annotations

import base64
from typing import Any

import httpx


class CortexAnalystClient:
    def __init__(
        self,
        *,
        account: str,
        user: str,
        password: str | None = None,
        token: str | None = None,
        timeout: float = 60.0,
    ):
        self._base = f"https://{account}.snowflakecomputing.com"
        self._user = user
        self._password = password
        self._token = token
        self._timeout = timeout

    def _auth_header(self) -> dict[str, str]:
        """Build the Authorization header.

        Bearer is the production path against real Snowflake, which expects
        either an OAuth access token or a keypair-JWT in `Authorization:
        Bearer <...>` (with `X-Snowflake-Authorization-Token-Type: KEYPAIR_JWT`
        for the JWT case). The Basic branch below is test scaffolding only —
        Snowflake's Cortex Analyst endpoint will reject it with 401 in
        production. Generating a keypair JWT from the configured private key
        is a v1.1 scope decision tracked separately.
        """
        if self._token:
            return {"authorization": f"Bearer {self._token}"}
        if self._password:
            blob = base64.b64encode(f"{self._user}:{self._password}".encode()).decode()
            return {"authorization": f"Basic {blob}"}
        raise RuntimeError("Cortex Analyst client requires password or token auth")

    async def send_message(self, payload: dict[str, Any]) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            resp = await client.post(
                f"{self._base}/api/v2/cortex/analyst/message",
                json=payload,
                headers={**self._auth_header(), "content-type": "application/json"},
            )
            # Read the body INSIDE the context manager so we don't depend on
            # httpx's eager-buffering default; a future switch to streaming
            # responses would break a body-read that happens after aclose().
            if resp.status_code >= 400:
                raise RuntimeError(
                    f"Cortex Analyst returned {resp.status_code}: {resp.text[:200]}"
                )
            return resp.json()
