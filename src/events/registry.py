"""Event schema registry + validation — the contract-test entry point.

Keyed by ``(type, schema_version)`` so a ``v2`` of any event coexists with its
``v1`` and both stay independently validatable — the product events are the live
example: producers emit ``v2`` (with the ``product_version`` ordering counter)
while ``v1`` stays registered for messages already in an outbox/queue.
:func:`validate_event` checks a raw event body (as pulled off SQS / stored in the
outbox) by re-parsing it through the registered strict Pydantic model — the same
contract that generates the JSON Schema, so no separate validator library is
needed — and hands back the normalized event for the handler. :func:`schema_for`
exposes that generated JSON Schema for a cross-language consumer or an
OpenAPI/AsyncAPI export.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from src.events.models import (
    DomainEvent,
    OrderPlaced,
    PaymentFailed,
    PaymentSucceeded,
    ProductCreated,
    ProductCreatedV2,
    ProductDeleted,
    ProductDeletedV2,
    ProductUpdated,
    ProductUpdatedV2,
    StockReleased,
    StockReserved,
    UserCreated,
    UserDeleted,
)

EVENT_MODELS: tuple[type[DomainEvent], ...] = (
    UserCreated,
    UserDeleted,
    # Product events: v1 is frozen and no longer produced, but stays registered so
    # messages written before the v2 rollout still validate on the way out of an
    # outbox table or an SQS queue instead of being DLQ'd as unknown.
    ProductCreated,
    ProductUpdated,
    ProductDeleted,
    ProductCreatedV2,
    ProductUpdatedV2,
    ProductDeletedV2,
    StockReserved,
    StockReleased,
    OrderPlaced,
    PaymentSucceeded,
    PaymentFailed,
)


#: The ``schema_version`` producers currently put on the wire, per event type.
#: Every consumer must accept a version **before** any producer emits it, because a
#: consumer that doesn't know a ``(type, schema_version)`` raises
#: :class:`UnknownEventError`, never deletes the message, and the queue DLQs it after
#: ``maxReceiveCount``. So a version bump is a **two-phase, consumer-first rollout**:
#: ship the V-capable consumers (and let them reach steady state), *then* the
#: producers — see ``docs/DEPLOYMENT.md`` § "Rolling out a new event version".
#: Pinned here rather than derived so the bump is an explicit, reviewable diff; a
#: test in ``tests/unit/test_events.py`` fails if production code drifts from it.
PRODUCED_VERSIONS: dict[str, int] = {
    "UserCreated": 1,
    "UserDeleted": 1,
    "ProductCreated": 2,
    "ProductUpdated": 2,
    "ProductDeleted": 2,
    "StockReserved": 1,
    "StockReleased": 1,
    "OrderPlaced": 1,
    "PaymentSucceeded": 1,
    "PaymentFailed": 1,
}


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


#: Envelope fields that carry a ``Literal`` default, so validation-mode schema
#: generation leaves them out of ``required`` — but a message without them is
#: unroutable, and :func:`validate_event` rejects it.
_ROUTING_FIELDS = ("type", "schema_version")


def schema_for(event_type: str, schema_version: int) -> dict[str, Any]:
    """Return the generated JSON Schema for one registered ``(type, version)`` pair.

    **Validation mode**, so the published contract is exactly what
    :func:`validate_event` enforces — serialization mode would advertise every
    ``Decimal`` as a string while the consumer happily accepts a JSON number.
    ``type``/``schema_version`` are appended to ``required`` (defaults, so pydantic
    omits them) on a copy: the schema pydantic returns is cached on the model.
    """
    schema = dict(_lookup(event_type, schema_version).model_json_schema())
    required = schema.get("required", [])
    schema["required"] = [*required, *(f for f in _ROUTING_FIELDS if f not in required)]
    return schema


def validate_event(body: str | bytes) -> dict[str, Any]:
    """Validate a raw event body (the SQS message / outbox payload) and return it normalized.

    Parsed **strictly** and in JSON mode, so the accepted set matches the published
    schema exactly: ``"quantity": "2"`` is a contract violation, not something to
    coerce. (Strict JSON still accepts a ``Decimal`` as either a JSON number or a
    string — both are what ``schema_for`` advertises.)

    Returns the model's ``mode="json"`` dump rather than the caller's dict, so a
    handler works on *validated, normalized* data; handing back the raw body would
    make validation advisory — the handler would still see whatever was on the wire.

    Raises :class:`UnknownEventError` if the ``(type, schema_version)`` is not
    registered, ``json.JSONDecodeError`` if the body is not JSON, or
    ``pydantic.ValidationError`` if it violates the contract (missing envelope
    field, wrong type, or an unexpected extra field — the model is ``extra="forbid"``).
    """
    raw = json.loads(body)  # routing fields first: which contract does this claim to be?
    if not isinstance(raw, Mapping):
        raise UnknownEventError(f"event body is not a JSON object: {type(raw).__name__}")
    model = _lookup(raw.get("type"), raw.get("schema_version"))
    return model.model_validate_json(body, strict=True).model_dump(mode="json")
