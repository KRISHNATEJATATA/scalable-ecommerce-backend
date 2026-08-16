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


def test_all_zero_trace_id_is_rejected() -> None:
    """The W3C spec declares an all-zero trace-id invalid — never pin it on a log."""
    assert parse_trace_id(f"00-{'0' * 32}-{'0' * 16}-01") is None


def test_an_empty_or_zero_trace_id_never_formats_to_the_all_zero_id() -> None:
    """Padding an empty id produced the invalid all-zero trace-id; a random one is used."""
    for lost_context in ("", "0", "---", "0000"):
        parsed = parse_trace_id(format_traceparent(lost_context))
        assert parsed is not None, lost_context
        assert parsed.strip("0"), lost_context


def test_producers_never_stamp_an_empty_trace_id() -> None:
    """``current_trace_id`` is the one fallback: outside a request it mints a fresh id."""
    from src.shared.config.logging import current_trace_id, request_id_ctx

    token = request_id_ctx.set("")
    try:
        outside_request = current_trace_id()
    finally:
        request_id_ctx.reset(token)

    assert parse_trace_id(format_traceparent(outside_request)) == outside_request

    token = request_id_ctx.set("0af7651916cd43dd8448eb211c80319c")
    try:
        assert current_trace_id() == "0af7651916cd43dd8448eb211c80319c"  # request id wins
    finally:
        request_id_ctx.reset(token)
