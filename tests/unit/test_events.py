"""Phase 6 contract tests: every domain event validates against its own JSON
Schema, and a violating payload is rejected on both the producer and consumer
side.

The registry generates each schema from its Pydantic model, so these tests are
the guard that a producer can't emit — and a consumer can't accept — a payload
that violates the versioned contract.
"""

from __future__ import annotations

import json
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
from src.events.registry import schema_for

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
    return UserCreated.new(trace_id="t-1", data=UserCreatedData(user_id=uuid.uuid4(), email="a@b.com"))


def test_registry_covers_every_listed_event() -> None:
    assert {m.model_fields["type"].default for m in EVENT_MODELS} == _EXPECTED


def test_every_event_carries_the_four_envelope_fields() -> None:
    for model in EVENT_MODELS:
        assert _ENVELOPE <= set(model.model_fields), model.__name__


def test_envelope_fields_are_required_in_the_generated_schema() -> None:
    """The wire contract must demand every envelope field — a default is not a promise."""
    for model in EVENT_MODELS:
        schema = schema_for(model.model_fields["type"].default, model.model_fields["schema_version"].default)
        assert _ENVELOPE | {"type", "data"} <= set(schema["required"]), model.__name__


def test_schema_and_consumer_agree_on_decimals() -> None:
    """Serialization mode would publish Decimals as strings while the consumer takes
    numbers — the published schema must describe what ``validate_event`` accepts."""
    total = schema_for("OrderPlaced", 1)["$defs"]["OrderPlacedData"]["properties"]["total"]
    assert "number" in [option.get("type") for option in total["anyOf"]]

    raw = OrderPlaced.new(
        trace_id="t",
        data=OrderPlacedData(
            order_id=uuid.uuid4(),
            user_id=uuid.uuid4(),
            total=Decimal("19.99"),
            items=[OrderPlacedLine(product_id=uuid.uuid4(), quantity=1, unit_price=Decimal("19.99"))],
        ),
    ).model_dump(mode="json")
    raw["data"]["total"] = 19.99  # a JSON number, as a cross-language producer sends
    raw["data"]["items"][0]["unit_price"] = 19.99
    validate_event(json.dumps(raw))


def test_optional_payload_fields_are_not_schema_required() -> None:
    """The envelope's required-defaults rule must not leak into payloads."""
    product = schema_for("ProductCreated", 1)["$defs"]["ProductWriteData"]
    assert "category" not in product.get("required", [])
    payment = schema_for("PaymentSucceeded", 1)["$defs"]["PaymentSucceededData"]
    assert "gateway_ref" not in payment.get("required", [])
    # ...and payloads still reject unknown fields (extra="forbid" is inherited).
    assert product["additionalProperties"] is False


@pytest.mark.parametrize("field", sorted(_ENVELOPE - {"schema_version"}))
def test_consumer_rejects_any_missing_envelope_field(field: str) -> None:
    """No envelope field may be minted at the receiving end (dedupe keys off event_id)."""
    raw = _sample().model_dump(mode="json")
    del raw[field]
    with pytest.raises(ValidationError):
        validate_event(json.dumps(raw))


@pytest.mark.parametrize(
    "event",
    [
        _sample(),
        StockReserved.new(trace_id="t", data=StockChangeData(sku="SKU-1", order_id=uuid.uuid4(), quantity=2)),
        ProductCreated.new(
            trace_id="t",
            data=ProductWriteData(product_id=uuid.uuid4(), merchant_id=uuid.uuid4(), name="x", price=Decimal("9.99")),
        ),
        OrderPlaced.new(
            trace_id="t",
            data=OrderPlacedData(
                order_id=uuid.uuid4(),
                user_id=uuid.uuid4(),
                total=Decimal("9.99"),
                items=[OrderPlacedLine(product_id=uuid.uuid4(), quantity=1, unit_price=Decimal("9.99"))],
            ),
        ),
        PaymentSucceeded.new(
            trace_id="t",
            data=PaymentSucceededData(payment_id=uuid.uuid4(), order_id=uuid.uuid4(), amount=Decimal("9.99")),
        ),
    ],
)
def test_producer_output_validates(event) -> None:
    validate_event(event.model_dump_json())


def test_consumer_rejects_coercible_wrong_types_and_returns_normalized_data() -> None:
    """Strict JSON mode: ``"2"`` is a contract violation, not an int to coerce."""
    event = StockReserved.new(trace_id="t", data=StockChangeData(sku="SKU-1", order_id=uuid.uuid4(), quantity=2))
    raw = json.loads(event.model_dump_json())
    raw["data"]["quantity"] = "2"  # JSON Schema says integer; coercion would hide that
    with pytest.raises(ValidationError):
        validate_event(json.dumps(raw))

    # The handler works on the validated model's data, not on whatever was on the wire.
    assert validate_event(event.model_dump_json())["data"]["quantity"] == 2


def test_consumer_rejects_unexpected_field() -> None:
    raw = _sample().model_dump(mode="json")
    raw["role"] = "admin"  # a privileged/unexpected field must not slip through
    with pytest.raises(ValidationError):
        validate_event(json.dumps(raw))


def test_consumer_rejects_missing_envelope_field() -> None:
    """A missing routing field can't even be looked up — unknown, not merely invalid."""
    raw = _sample().model_dump(mode="json")
    del raw["schema_version"]
    with pytest.raises(UnknownEventError):
        validate_event(json.dumps(raw))


def test_unknown_type_or_version_raises() -> None:
    good = _sample().model_dump(mode="json")
    with pytest.raises(UnknownEventError):
        validate_event(json.dumps({**good, "type": "NopeEvent"}))
    with pytest.raises(UnknownEventError):
        validate_event(json.dumps({**good, "schema_version": 999}))


def test_request_dto_ignores_unexpected_privileged_field() -> None:
    """Mass-assignment guard (AC1): a body field the DTO doesn't declare is dropped, not bound."""
    from src.identity.api.schemas import CreateUserRequest

    dto = CreateUserRequest.model_validate({"email": "a@b.com", "role": "admin"})
    assert not hasattr(dto, "role")
