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
