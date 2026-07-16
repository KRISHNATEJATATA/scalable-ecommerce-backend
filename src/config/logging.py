"""Structured JSON logging configuration with ECS (Elastic Common Schema).

Configures all log output as ECS-compliant JSON via ``ecs-logging`` and
``python-json-logger``.
Per-request context is propagated via :mod:`contextvars` and injected
into every :class:`logging.LogRecord` by :class:`ContextFilter`.

Usage::

    from src.config.logging import setup_logging

    setup_logging()  # call once at startup

Never log secrets or PII — :class:`RedactFilter` strips well-known
sensitive keys automatically.
"""

import logging
import sys
from contextvars import ContextVar

import ecs_logging

# Per-request trace id, set by the request-id middleware; empty until then.
request_id_ctx: ContextVar[str] = ContextVar("request_id", default="")

# ponytail: minimal boundary redaction. Full key/PII scrubbing hardens in Phase 9.
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
