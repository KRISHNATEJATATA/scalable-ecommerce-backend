"""Structured JSON logging configuration with ECS (Elastic Common Schema).

Configures all log output as ECS-compliant JSON via ``ecs-logging`` and
``python-json-logger``.
Per-request context is propagated via :mod:`contextvars` and injected
into every :class:`logging.LogRecord` by :class:`ContextFilter`.

Usage::

    from src.shared.config.logging import setup_logging

    setup_logging()  # call once at startup

Never log secrets or PII — :class:`RedactFilter` strips well-known
sensitive keys automatically.
"""

import logging
import sys
import uuid
from contextvars import ContextVar

import ecs_logging

# Per-request trace id, set by the request-id middleware; empty until then.
request_id_ctx: ContextVar[str] = ContextVar("request_id", default="")


def current_trace_id() -> str:
    """The ambient request trace id, or a fresh one when there is no request.

    Event producers must never stamp an **empty** ``trace_id``: the relay derives
    the W3C ``traceparent`` from it, and an empty value normalises to the all-zero
    trace-id the spec declares invalid. Workers (relay, image worker, reaper) run
    outside any request, so an absent context yields a fresh id — an event traceable
    to one worker pass rather than to nothing.

    Returned as 32 hex chars so it maps 1:1 onto a W3C trace-id with no reformatting.
    """
    return request_id_ctx.get() or uuid.uuid4().hex


# minimal boundary redaction. Full key/PII scrubbing hardens in Phase 9.
_REDACT_KEYS = ("password", "token", "authorization", "secret", "cookie", "jwt")


class ContextFilter(logging.Filter):
    """Inject the current request id onto every record as ``trace_id``."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.trace_id = request_id_ctx.get()
        return True


class RedactFilter(logging.Filter):
    """Redact obvious secrets that slipped into a log message."""

    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage().lower()
        if any(key in msg for key in _REDACT_KEYS):
            record.msg = "[REDACTED: message contained a sensitive key]"
            record.args = ()
        return True


def setup_logging(level: str = "INFO") -> None:
    """Configure ECS-JSON logging to stdout. Idempotent; call once at startup."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(ecs_logging.StdlibFormatter())
    handler.addFilter(ContextFilter())
    handler.addFilter(RedactFilter())

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level.upper())
