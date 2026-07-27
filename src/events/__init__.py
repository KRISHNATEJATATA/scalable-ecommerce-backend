"""Shared, cross-context domain-event contracts.

The single source of truth for every event that crosses a module boundary via
the transactional outbox → SNS/SQS bus. Each event is a strict Pydantic model
(``extra="forbid"``); its **versioned JSON Schema** is ``model_json_schema()`` —
no hand-maintained ``.json`` that can drift from the producing code.

Producers build a typed instance and ``model_dump(mode="json")`` it into the
outbox ``payload``. Consumers (and contract tests) validate a raw dict by
re-parsing it through the registered model with :func:`validate_event`;
:func:`schema_for` exposes the generated JSON Schema for a cross-language
consumer that needs the raw contract.
"""

from __future__ import annotations

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
from src.events.registry import (
    EVENT_MODELS,
    REGISTRY,
    UnknownEventError,
    schema_for,
    validate_event,
)

__all__ = [
    "DomainEvent",
    "UserCreated",
    "UserDeleted",
    "ProductCreated",
    "ProductUpdated",
    "ProductDeleted",
    "StockReserved",
    "StockReleased",
    "OrderPlaced",
    "PaymentSucceeded",
    "PaymentFailed",
    "EVENT_MODELS",
    "REGISTRY",
    "UnknownEventError",
    "schema_for",
    "validate_event",
]
