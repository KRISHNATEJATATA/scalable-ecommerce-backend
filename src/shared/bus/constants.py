"""Static bus topology constants."""

from __future__ import annotations


def topic_name(prefix: str, event_type: str) -> str:
    """SNS topic name for an event type: ``f"{prefix}{EventType}"`` (topic-per-type)."""
    return f"{prefix}{event_type}"
