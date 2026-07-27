"""Event schema registry + validation — the contract-test entry point.

Keyed by ``(type, schema_version)`` so a future ``v2`` of any event coexists with
its ``v1`` and both stay independently validatable (only ``v1`` exists today).
:func:`validate_event` checks a raw dict (as pulled off SQS / stored in the
outbox) by re-parsing it through the registered strict Pydantic model — the same
contract that generates the JSON Schema, so no separate validator library is
needed. :func:`schema_for` exposes that generated JSON Schema for a cross-language
consumer or an OpenAPI/AsyncAPI export.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from src.events.models import (
    DomainEvent,
    OrderPlaced,
    PaymentFailed,
    PaymentSucceeded,
    ProductCreated,
    ProductDeleted,
    ProductUpdated,
    StockReleased,
    StockReserved,
    UserCreated,
    UserDeleted,
)

EVENT_MODELS: tuple[type[DomainEvent], ...] = (
    UserCreated,
    UserDeleted,
    ProductCreated,
    ProductUpdated,
    ProductDeleted,
    StockReserved,
    StockReleased,
    OrderPlaced,
    PaymentSucceeded,
    PaymentFailed,
)


class UnknownEventError(LookupError):
    """No registered event model for a ``(type, schema_version)`` pair."""


def _key(model: type[DomainEvent]) -> tuple[str, int]:
    return (model.model_fields["type"].default, model.model_fields["schema_version"].default)


REGISTRY: dict[tuple[str, int], type[DomainEvent]] = {_key(m): m for m in EVENT_MODELS}


def _lookup(event_type: Any, schema_version: Any) -> type[DomainEvent]:
    model = REGISTRY.get((event_type, schema_version))
    if model is None:
        raise UnknownEventError(f"no event registered for {(event_type, schema_version)!r}")
    return model


def schema_for(event_type: str, schema_version: int) -> dict[str, Any]:
    """Return the generated JSON Schema for one registered ``(type, version)`` pair."""
    return _lookup(event_type, schema_version).model_json_schema()


def validate_event(raw: Mapping[str, Any]) -> None:
    """Validate a raw event dict against its registered contract.

    Raises :class:`UnknownEventError` if the ``(type, schema_version)`` is not
    registered, or ``pydantic.ValidationError`` if the payload violates the
    contract (missing envelope field, wrong type, or an unexpected extra field —
    the model is ``extra="forbid"``).
    """
    _lookup(raw.get("type"), raw.get("schema_version")).model_validate(raw)
