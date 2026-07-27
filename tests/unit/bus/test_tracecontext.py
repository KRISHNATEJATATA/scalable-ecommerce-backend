"""Unit tests for the W3C trace-context helpers."""

from __future__ import annotations

from src.shared.bus.tracecontext import format_traceparent, parse_trace_id


def test_traceparent_round_trips_a_hex_trace_id() -> None:
    trace_id = "0af7651916cd43dd8448eb211c80319c"
    assert parse_trace_id(format_traceparent(trace_id)) == trace_id


def test_non_hex_trace_id_is_normalised_to_32_hex() -> None:
    parsed = parse_trace_id(format_traceparent("req-42"))
    assert parsed is not None and len(parsed) == 32
    assert all(c in "0123456789abcdef" for c in parsed)


def test_each_publish_gets_a_fresh_span_id() -> None:
    tid = "0af7651916cd43dd8448eb211c80319c"
    assert format_traceparent(tid) != format_traceparent(tid)


def test_malformed_traceparent_returns_none() -> None:
    assert parse_trace_id(None) is None
    assert parse_trace_id("") is None
    assert parse_trace_id("garbage") is None
    assert parse_trace_id("00-tooshort-abcd-01") is None
