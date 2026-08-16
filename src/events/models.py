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
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field


def _utcnow() -> datetime:
    return datetime.now(UTC)


class _Strict(BaseModel):
    """Base for every event and payload: reject unknown fields."""

    model_config = ConfigDict(extra="forbid")


class DomainEvent(_Strict):
    """Base envelope shared by every domain event.

    Carries ``event_id``, ``trace_id`` and ``occurred_at`` here; each concrete
    subclass adds the remaining two envelope fields — ``type`` and
    ``schema_version`` — as ``Literal`` defaults, plus its typed ``data`` payload.

    **No field here has a default.** Defaulting ``event_id``/``occurred_at`` would
    leave them out of the schema's ``required`` list and let a consumer accept an
    event that never carried them — dedupe keys off ``event_id`` and handlers read
    ``occurred_at``, so "absent" must fail validation, not be quietly minted at the
    receiving end. Producers stamp both through :meth:`new`.
    """

    event_id: uuid.UUID
    trace_id: str
    occurred_at: datetime

    @classmethod
    def new(cls, **fields: Any) -> Self:
        """Build an event as it is emitted: stamps ``event_id`` and ``occurred_at``."""
        return cls(event_id=uuid.uuid4(), occurred_at=_utcnow(), **fields)


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
    """v1 payload for product create/update (same fields change together).

    **Frozen.** Superseded by :class:`ProductWriteDataV2`, but kept registered so
    v1 messages already sitting in an outbox table or an SQS queue when v2 shipped
    still validate instead of failing their handler into a DLQ. Nothing produces
    it any more; delete it once no v1 message can be in flight (past the queue's
    retention + any DLQ replay window).
    """

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
    """v1 delete payload. Frozen — see :class:`ProductWriteData`."""

    product_id: uuid.UUID
    merchant_id: uuid.UUID


class ProductDeleted(DomainEvent):
    type: Literal["ProductDeleted"] = "ProductDeleted"
    schema_version: Literal[1] = 1
    data: ProductDeletedData


class ProductWriteDataV2(ProductWriteData):
    """v2 payload — v1 plus the ordering counter (**what producers emit**).

    ``product_version`` is the catalog aggregate's ``version_id`` **after** the
    write that produced this event. SNS topics are standard (unordered) and the
    relay publishes a batch concurrently, so a consumer can legitimately see an
    older update *after* a newer one. ``event_id`` dedup only suppresses exact
    redeliveries and ``schema_version`` versions the *contract* — neither orders
    instances. A projector must therefore keep the last applied version per
    product and drop any event whose ``product_version`` is not greater.

    Adding it is a **breaking** payload change (payloads are ``extra="forbid"``, so
    a v1 consumer would reject the extra field and a v1 message lacking it would
    fail v2 validation), hence a new ``schema_version`` rather than an edit in place.

    **Deliberately not carried: ``description``.** The payload is a *notification*
    of a change plus the fields a consumer projects (cart line display: name, price,
    category) — not a replica of the row. A `description`-only edit still emits
    ``ProductUpdated``, so cache invalidation and re-fetch are correct; nothing today
    projects the description. Adding it later is a v3, not an in-place edit.
    """

    product_version: int = Field(ge=1)


class ProductCreatedV2(DomainEvent):
    type: Literal["ProductCreated"] = "ProductCreated"
    schema_version: Literal[2] = 2
    data: ProductWriteDataV2


class ProductUpdatedV2(DomainEvent):
    type: Literal["ProductUpdated"] = "ProductUpdated"
    schema_version: Literal[2] = 2
    data: ProductWriteDataV2


class ProductDeletedDataV2(ProductDeletedData):
    """v2 delete payload — a **tombstone**, ordered by the same counter.

    Carries ``product_version`` for the same reason as the write payload: without
    it, an update published before the delete but delivered after it would
    resurrect a removed product in a downstream projection.
    """

    product_version: int = Field(ge=1)


class ProductDeletedV2(DomainEvent):
    type: Literal["ProductDeleted"] = "ProductDeleted"
    schema_version: Literal[2] = 2
    data: ProductDeletedDataV2


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
