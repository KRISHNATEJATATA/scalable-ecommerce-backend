"""W3C trace-context propagation over the queue hop.

The relay injects the current trace as a ``traceparent`` SQS message attribute
(built from the event envelope's ``trace_id``); the consumer parses it back and
pins it onto :data:`~src.shared.config.logging.request_id_ctx` so every consumer
log line correlates with the request that produced the event.

We format/parse the W3C string by hand — there is no OpenTelemetry SDK wired, and
a 4-field dash-delimited string does not warrant one (YAGNI). The envelope
``trace_id`` in this codebase is a ``uuid4().hex`` request id (32 hex chars); the
normaliser tolerates anything else by padding/truncating to a valid 32-hex id.
"""

from __future__ import annotations

import re
import secrets

# SQS message-attribute name carrying the W3C traceparent header.
TRACEPARENT_ATTR = "traceparent"

_VERSION = "00"
_SAMPLED = "01"
_NON_HEX = re.compile(r"[^0-9a-f]")


def _normalise_trace_id(trace_id: str, width: int = 32) -> str:
    hex_only = _NON_HEX.sub("", (trace_id or "").lower())
    return (hex_only or "0").ljust(width, "0")[:width]


def format_traceparent(trace_id: str) -> str:
    """Build a W3C ``traceparent`` from an envelope ``trace_id``.

    ``00-<32-hex trace-id>-<16-hex span-id>-01``. The span-id is fresh per
    publish (this hop is a new span); sampled flag is always on.
    """
    return f"{_VERSION}-{_normalise_trace_id(trace_id)}-{secrets.token_hex(8)}-{_SAMPLED}"


def parse_trace_id(traceparent: str | None) -> str | None:
    """Extract the 32-hex trace-id from a ``traceparent``; ``None`` if malformed."""
    if not traceparent:
        return None
    parts = traceparent.split("-")
    if len(parts) < 4:
        return None
    trace_id = parts[1]
    if len(trace_id) == 32 and _NON_HEX.sub("", trace_id) == trace_id:
        return trace_id
    return None


if __name__ == "__main__":  # pragma: no cover - runnable self-check
    tid = "0af7651916cd43dd8448eb211c80319c"
    tp = format_traceparent(tid)
    assert parse_trace_id(tp) == tid, tp
    # round-trips a non-hex/short id through normalisation
    assert len(parse_trace_id(format_traceparent("req-42"))) == 32
    # rejects malformed input
    assert parse_trace_id(None) is None
    assert parse_trace_id("garbage") is None
    assert parse_trace_id("00-tooshort-abc-01") is None
    print("tracecontext self-check ok")
