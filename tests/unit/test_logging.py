import io
import json
import logging

from greybeam_mcp.logging_setup import setup_logging, tool_call_log


def test_setup_logging_writes_json_to_stderr(monkeypatch):
    buf = io.StringIO()
    setup_logging(stream=buf, level="INFO")
    log = logging.getLogger("greybeam_mcp.test")
    log.info("hello", extra={"request_id": "r1", "tool_name": "x"})

    line = buf.getvalue().strip().splitlines()[-1]
    payload = json.loads(line)
    assert payload["message"] == "hello"
    assert payload["request_id"] == "r1"
    assert payload["tool_name"] == "x"
    assert payload["level"] == "INFO"


def test_tool_call_log_has_required_keys():
    fields = tool_call_log(
        request_id="r1",
        tool_name="run_snowflake_query",
        route="greybeam",
        latency_ms=42,
        outcome="ok",
        cancelled=False,
        rows_returned=10,
    )
    for key in (
        "request_id",
        "tool_name",
        "route",
        "latency_ms",
        "outcome",
        "cancelled",
        "rows_returned",
    ):
        assert key in fields
    assert fields["error_kind"] is None
    assert fields["error_code"] is None
