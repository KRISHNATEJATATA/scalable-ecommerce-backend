"""Versioned domain-event models — the wire contract for the outbox → bus.

Every event is a strict envelope carrying the four required fields
(``event_id``, ``schema_version``, ``trace_id``, ``occurred_at``) plus a ``type``
discriminator and a typed ``data`` payload. ``extra="forbid"`` on both the
envelope and every payload means an unexpected/privileged field makes the
payload *violate* its schema (``additionalProperties: false`` in the generated
JSON Schema) rather than being silently accepted.

``type`` mirrors the outbox ``event_type`` column; ``schema_version`` is a plain
integer bumped on any breaking payload change (a new ``Literal`` subclass, so
both versions stay registered and independently validatable).
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


def _utcnow() -> datetime:
    return datetime.now(UTC)


class _Strict(BaseModel):
    """Base for every event and payload: reject unknown fields."""

    model_config = ConfigDict(extra="forbid")


class DomainEvent(_Strict):
    """Base envelope shared by every domain event.

    Carries ``event_id``, ``trace_id`` and ``occurred_at`` here; each concrete
    subclass adds the remaining two required envelope fields — ``type`` and
    ``schema_version`` — as ``Literal`` defaults, plus its typed ``data`` payload.
    A producer therefore only supplies ``trace_id`` and ``data``.
    """

    event_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    trace_id: str
    occurred_at: datetime = Field(default_factory=_utcnow)


# --- Identity -----------------------------------------------------------------


class UserCreatedData(_Strict):
    user_id: uuid.UUID
    email: str


class UserCreated(DomainEvent):
    type: Literal["UserCreated"] = "UserCreated"
    schema_version: Literal[1] = 1
    data: UserCreatedData


class UserDeletedData(_Strict):
    user_id: uuid.UUID


class UserDeleted(DomainEvent):
    type: Literal["UserDeleted"] = "UserDeleted"
    schema_version: Literal[1] = 1
    data: UserDeletedData


# --- Catalog ------------------------------------------------------------------


class ProductWriteData(_Strict):
    """Shared payload for product create/update (same fields change together)."""

    product_id: uuid.UUID
    merchant_id: uuid.UUID
    name: str
    price: Decimal
    category: str | None = None


class ProductCreated(DomainEvent):
    type: Literal["ProductCreated"] = "ProductCreated"
    schema_version: Literal[1] = 1
    data: ProductWriteData


class ProductUpdated(DomainEvent):
    type: Literal["ProductUpdated"] = "ProductUpdated"
    schema_version: Literal[1] = 1
    data: ProductWriteData


class ProductDeletedData(_Strict):
    product_id: uuid.UUID
    merchant_id: uuid.UUID


class ProductDeleted(DomainEvent):
    type: Literal["ProductDeleted"] = "ProductDeleted"
    schema_version: Literal[1] = 1
    data: ProductDeletedData


# --- Inventory ----------------------------------------------------------------


class StockChangeData(_Strict):
    """Shared payload for a reserve/release against one SKU for one order."""

    sku: str
    order_id: uuid.UUID
    quantity: int = Field(gt=0)


class StockReserved(DomainEvent):
    type: Literal["StockReserved"] = "StockReserved"
    schema_version: Literal[1] = 1
    data: StockChangeData


class StockReleased(DomainEvent):
    type: Literal["StockReleased"] = "StockReleased"
    schema_version: Literal[1] = 1
    data: StockChangeData


# --- Orders -------------------------------------------------------------------


class OrderPlacedLine(_Strict):
    product_id: uuid.UUID
    quantity: int = Field(gt=0)
    unit_price: Decimal


class OrderPlacedData(_Strict):
    order_id: uuid.UUID
    user_id: uuid.UUID
    total: Decimal
    items: list[OrderPlacedLine]


class OrderPlaced(DomainEvent):
    type: Literal["OrderPlaced"] = "OrderPlaced"
    schema_version: Literal[1] = 1
    data: OrderPlacedData


# --- Payments -----------------------------------------------------------------


class PaymentSucceededData(_Strict):
    payment_id: uuid.UUID
    order_id: uuid.UUID
    amount: Decimal
    gateway_ref: str | None = None


class PaymentSucceeded(DomainEvent):
    type: Literal["PaymentSucceeded"] = "PaymentSucceeded"
    schema_version: Literal[1] = 1
    data: PaymentSucceededData


class PaymentFailedData(_Strict):
    payment_id: uuid.UUID
    order_id: uuid.UUID
    amount: Decimal
    reason: str


class PaymentFailed(DomainEvent):
    type: Literal["PaymentFailed"] = "PaymentFailed"
    schema_version: Literal[1] = 1
    data: PaymentFailedData
