from greybeam_mcp.tools.cortex_analyst_parser import parse_analyst_response


def test_parses_text_only():
    raw = {
        "message": {
            "content": [
                {"type": "text", "text": "Here is some explanation."}
            ]
        }
    }
    parsed = parse_analyst_response(raw)
    assert parsed.text == "Here is some explanation."
    assert parsed.sql is None


def test_parses_text_and_sql():
    raw = {
        "message": {
            "content": [
                {"type": "text", "text": "Generated SQL:"},
                {"type": "sql", "statement": "SELECT 1"},
            ]
        }
    }
    parsed = parse_analyst_response(raw)
    assert parsed.text == "Generated SQL:"
    assert parsed.sql == "SELECT 1"


def test_drops_suggestions_and_unknown_types():
    raw = {
        "message": {
            "content": [
                {"type": "text", "text": "ok"},
                {"type": "suggestions", "suggestions": ["a", "b"]},
                {"type": "future_block", "data": "x"},
                {"type": "sql", "statement": "SELECT 2"},
            ]
        }
    }
    parsed = parse_analyst_response(raw)
    assert parsed.text == "ok"
    assert parsed.sql == "SELECT 2"


def test_concatenates_multiple_text_blocks():
    raw = {
        "message": {
            "content": [
                {"type": "text", "text": "Part 1."},
                {"type": "text", "text": "Part 2."},
            ]
        }
    }
    parsed = parse_analyst_response(raw)
    assert parsed.text == "Part 1.\nPart 2."


def test_multiple_sql_blocks_keep_last_and_empty_input_yields_blank():
    # Multiple sql blocks: per implementation contract, last statement wins.
    raw = {
        "message": {
            "content": [
                {"type": "sql", "statement": "SELECT 1"},
                {"type": "sql", "statement": "SELECT 2"},
            ]
        }
    }
    parsed = parse_analyst_response(raw)
    assert parsed.sql == "SELECT 2"
    assert parsed.text == ""

    # Empty content list yields default ParsedAnalyst.
    parsed_empty = parse_analyst_response({"message": {"content": []}})
    assert parsed_empty.text == ""
    assert parsed_empty.sql is None

    # Missing message key also tolerated (fail-safe per upstream parity).
    parsed_missing = parse_analyst_response({})
    assert parsed_missing.text == ""
    assert parsed_missing.sql is None


def test_null_text_and_null_sql_values_are_tolerated():
    """Snowflake may emit explicit nulls for text/statement (e.g., streaming
    partials). The parser must coerce these without raising and must not
    let a null sql block clobber a previously-good statement.
    """
    raw = {
        "message": {
            "content": [
                {"type": "text", "text": None},
                {"type": "text", "text": "real text"},
                {"type": "sql", "statement": "SELECT 1"},
                {"type": "sql", "statement": None},
            ]
        }
    }
    parsed = parse_analyst_response(raw)
    # Null-text coerces to "" and joins with the real one.
    assert parsed.text == "\nreal text"
    # Null-sql does NOT clobber the real prior statement.
    assert parsed.sql == "SELECT 1"
