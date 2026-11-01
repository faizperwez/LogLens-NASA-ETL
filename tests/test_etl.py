from pathlib import Path

import pandas as pd

from loglens.etl import DEFAULT_INPUT, parse_raw_line, prepare_batch, transform_parsed_records


def test_valid_common_log_line_parses():
    line = 'example.com - - [01/Aug/1995:00:00:07 -0400] "GET /images/logo.gif HTTP/1.0" 200 786'
    parsed, rejected = parse_raw_line(line, "test", 1)
    assert rejected is None
    assert parsed is not None
    assert parsed["host"] == "example.com"
    assert parsed["http_method"] == "GET"
    assert parsed["url_path"] == "/images/logo.gif"
    assert parsed["status_raw"] == "200"


def test_malformed_line_is_rejected():
    parsed, rejected = parse_raw_line("not a complete log line", "test", 1)
    assert parsed is None
    assert rejected["rejection_reason"] == "malformed_log_line"


def test_query_string_is_removed_from_resource_path():
    line = 'example.com - - [01/Aug/1995:00:00:07 -0400] "GET /search?q=abc HTTP/1.0" 200 50'
    parsed, rejected = parse_raw_line(line, "test", 1)
    assert rejected is None
    assert parsed["url_path"] == "/search"


def test_dash_response_bytes_is_allowed_as_missing():
    line = 'example.com - - [01/Aug/1995:00:00:07 -0400] "HEAD / HTTP/1.0" 200 -'
    parsed, rejected = parse_raw_line(line, "test", 1)
    assert rejected is None
    clean, bad = transform_parsed_records([parsed])
    assert len(clean) == 1
    assert pd.isna(clean.iloc[0]["bytes_sent"])
    assert len(bad) == 0


def test_invalid_status_and_negative_bytes_are_rejected():
    lines = [
        (1, 'host1.example - - [01/Aug/1995:00:00:07 -0400] "GET / HTTP/1.0" 999 50'),
        (2, 'host2.example - - [01/Aug/1995:00:00:08 -0400] "GET /bad HTTP/1.0" 200 -3'),
    ]
    clean, rejected = prepare_batch(lines, "test")
    assert clean.empty
    assert len(rejected) == 2
    assert "invalid_status_code" in rejected.iloc[0]["rejection_reason"]
    assert "invalid_bytes_sent" in rejected.iloc[1]["rejection_reason"]


def test_bundled_sample_has_both_valid_and_rejected_lines():
    rows = list(enumerate(DEFAULT_INPUT.read_text(encoding="latin-1").splitlines(), start=1))
    clean, rejected = prepare_batch([(number, line) for number, line in rows], "sample-test")
    assert len(clean) > 0
    assert len(rejected) > 0
    assert clean["record_id"].is_unique
    assert clean["status_code"].between(100, 599).all()
    assert (clean["bytes_sent"].dropna() >= 0).all()
