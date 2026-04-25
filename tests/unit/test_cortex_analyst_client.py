import pytest
import respx
from httpx import Response

from greybeam_mcp.tools.cortex_analyst_client import CortexAnalystClient


@pytest.mark.asyncio
@respx.mock
async def test_post_analyst_message_sends_to_account_url():
    route = respx.post(
        "https://abc-xyz.snowflakecomputing.com/api/v2/cortex/analyst/message"
    ).mock(return_value=Response(200, json={"message": {"content": []}}))

    client = CortexAnalystClient(
        account="abc-xyz",
        password="pw",
        user="agent",
    )
    body = await client.send_message({"messages": [{"role": "user", "content": "x"}]})

    assert route.called
    assert body == {"message": {"content": []}}
    sent = route.calls.last.request
    assert sent.headers["authorization"]


@pytest.mark.asyncio
@respx.mock
async def test_http_error_raises():
    respx.post(
        "https://abc-xyz.snowflakecomputing.com/api/v2/cortex/analyst/message"
    ).mock(return_value=Response(429, text="rate limited"))

    client = CortexAnalystClient(account="abc-xyz", password="pw", user="agent")
    with pytest.raises(RuntimeError, match="429"):
        await client.send_message({"messages": []})


@pytest.mark.asyncio
@respx.mock
async def test_token_auth_uses_bearer_header():
    route = respx.post(
        "https://abc.snowflakecomputing.com/api/v2/cortex/analyst/message"
    ).mock(return_value=Response(200, json={"message": {"content": []}}))

    client = CortexAnalystClient(account="abc", user="agent", token="tok-123")
    await client.send_message({"messages": []})

    auth = route.calls.last.request.headers["authorization"]
    assert auth == "Bearer tok-123"


@pytest.mark.asyncio
async def test_no_credentials_raises_at_call_time():
    client = CortexAnalystClient(account="abc", user="agent")  # no password, no token
    with pytest.raises(RuntimeError, match="password or token"):
        await client.send_message({"messages": []})
