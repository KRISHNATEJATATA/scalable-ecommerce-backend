"""Phase 6 contract tests: every domain event validates against its own JSON
Schema, and a violating payload is rejected on both the producer and consumer
side.

The registry generates each schema from its Pydantic model, so these tests are
the guard that a producer can't emit — and a consumer can't accept — a payload
that violates the versioned contract.
"""

from __future__ import annotations

import uuid
from decimal import Decimal

import pytest
from pydantic import ValidationError

from src.events import (
    EVENT_MODELS,
    OrderPlaced,
    PaymentSucceeded,
    ProductCreated,
    StockReserved,
    UnknownEventError,
    UserCreated,
    validate_event,
)
from src.events.models import (
    OrderPlacedData,
    OrderPlacedLine,
    PaymentSucceededData,
    ProductWriteData,
    StockChangeData,
    UserCreatedData,
)

_EXPECTED = {
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
}

_ENVELOPE = {"event_id", "schema_version", "trace_id", "occurred_at"}


def _sample() -> UserCreated:
    return UserCreated(trace_id="t-1", data=UserCreatedData(user_id=uuid.uuid4(), email="a@b.com"))


def test_registry_covers_every_listed_event() -> None:
    assert {m.model_fields["type"].default for m in EVENT_MODELS} == _EXPECTED


def test_every_event_carries_the_four_envelope_fields() -> None:
    for model in EVENT_MODELS:
        assert _ENVELOPE <= set(model.model_fields), model.__name__


@pytest.mark.parametrize(
    "event",
    [
        _sample(),
        StockReserved(trace_id="t", data=StockChangeData(sku="SKU-1", order_id=uuid.uuid4(), quantity=2)),
        ProductCreated(
            trace_id="t",
            data=ProductWriteData(product_id=uuid.uuid4(), merchant_id=uuid.uuid4(), name="x", price=Decimal("9.99")),
        ),
        OrderPlaced(
            trace_id="t",
            data=OrderPlacedData(
                order_id=uuid.uuid4(),
                user_id=uuid.uuid4(),
                total=Decimal("9.99"),
                items=[OrderPlacedLine(product_id=uuid.uuid4(), quantity=1, unit_price=Decimal("9.99"))],
            ),
        ),
        PaymentSucceeded(
            trace_id="t",
            data=PaymentSucceededData(payment_id=uuid.uuid4(), order_id=uuid.uuid4(), amount=Decimal("9.99")),
        ),
    ],
)
def test_producer_output_validates(event) -> None:
    validate_event(event.model_dump(mode="json"))


def test_consumer_rejects_unexpected_field() -> None:
    raw = _sample().model_dump(mode="json")
    raw["role"] = "admin"  # a privileged/unexpected field must not slip through
    with pytest.raises(ValidationError):
        validate_event(raw)


def test_consumer_rejects_missing_envelope_field() -> None:
    raw = _sample().model_dump(mode="json")
    del raw["trace_id"]
    with pytest.raises(ValidationError):
        validate_event(raw)


def test_unknown_type_or_version_raises() -> None:
    good = _sample().model_dump(mode="json")
    with pytest.raises(UnknownEventError):
        validate_event({**good, "type": "NopeEvent"})
    with pytest.raises(UnknownEventError):
        validate_event({**good, "schema_version": 999})


def test_request_dto_ignores_unexpected_privileged_field() -> None:
    """Mass-assignment guard (AC1): a body field the DTO doesn't declare is dropped, not bound."""
    from src.identity.api.schemas import CreateUserRequest

    dto = CreateUserRequest.model_validate({"email": "a@b.com", "role": "admin"})
    assert not hasattr(dto, "role")
