"""Phase 6 contract tests: every domain event validates against its own JSON
Schema, and a violating payload is rejected on both the producer and consumer
side.

The registry generates each schema from its Pydantic model, so these tests are
the guard that a producer can't emit — and a consumer can't accept — a payload
that violates the versioned contract.
"""

from __future__ import annotations

import json
import re
import uuid
from collections.abc import Callable
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from src.events import (
    EVENT_MODELS,
    OrderPlaced,
    ProductCreatedV2,
    StockReserved,
    UnknownEventError,
    UserCreated,
    validate_event,
)
from src.events.models import (
    OrderPlacedData,
    OrderPlacedLine,
    ProductWriteDataV2,
    StockChangeData,
    UserCreatedData,
)
from src.events.registry import PRODUCED_VERSIONS, REGISTRY, schema_for

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


def test_registry_build_rejects_duplicate_type_version_pairs() -> None:
    """A copy-pasted ``Literal`` version must fail loudly, not silently shadow the old model."""
    from src.events.registry import _build_registry  # private helper under test

    with pytest.raises(RuntimeError, match="duplicate event registration"):
        _build_registry((UserCreated, UserCreated))

    # The real registry built clean and keys V2 correctly.
    key = (ProductCreatedV2.model_fields["type"].default, ProductCreatedV2.model_fields["schema_version"].default)
    assert REGISTRY[key] is ProductCreatedV2


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


# --- every registered contract is enumerated, producer AND consumer side ------
#
# The registry is keyed by ``(type, schema_version)`` and grows every time a new
# event type or version lands. A hand-written test list grows only when someone
# remembers — a v3 payload added to ``EVENT_MODELS`` without a contract test
# would ship with no proof that producer output validates or that a violating
# payload is rejected. So the enumeration is *driven by the registry*: the map
# below must cover every ``(type, version)`` in ``REGISTRY``, and the test fails
# with instructions when a new entry is added without a sample.
#
#: ``(type, schema_version) -> callable(**data) -> payload dict``. The factory
#: receives fresh UUIDs (product/merchant/user ids) so samples never collide.
_EVENT_SAMPLES: dict[tuple[str, int], Callable[..., dict[str, Any]]] = {
    ("UserCreated", 1): lambda user_id, merchant_id: {
        "user_id": user_id,
        "email": "a@b.com",
    },
    ("UserDeleted", 1): lambda user_id, merchant_id: {"user_id": user_id},
    ("ProductCreated", 1): lambda user_id, merchant_id: {
        "product_id": user_id,
        "merchant_id": merchant_id,
        "name": "widget",
        "price": "9.99",
        "category": "tools",
    },
    ("ProductUpdated", 1): lambda user_id, merchant_id: {
        "product_id": user_id,
        "merchant_id": merchant_id,
        "name": "widget",
        "price": "9.99",
    },
    ("ProductDeleted", 1): lambda user_id, merchant_id: {"product_id": user_id, "merchant_id": merchant_id},
    ("ProductCreated", 2): lambda user_id, merchant_id: {
        "product_id": user_id,
        "merchant_id": merchant_id,
        "name": "widget",
        "price": "9.99",
        "product_version": 1,
    },
    ("ProductUpdated", 2): lambda user_id, merchant_id: {
        "product_id": user_id,
        "merchant_id": merchant_id,
        "name": "widget",
        "price": "9.99",
        "product_version": 2,
    },
    ("ProductDeleted", 2): lambda user_id, merchant_id: {
        "product_id": user_id,
        "merchant_id": merchant_id,
        "product_version": 1,
    },
    ("StockReserved", 1): lambda user_id, merchant_id: {
        "sku": "SKU-1",
        "order_id": user_id,
        "quantity": 2,
    },
    ("StockReleased", 1): lambda user_id, merchant_id: {
        "sku": "SKU-1",
        "order_id": user_id,
        "quantity": 2,
    },
    ("OrderPlaced", 1): lambda user_id, merchant_id: {
        "order_id": user_id,
        "user_id": user_id,
        "total": "19.99",
        "items": [{"product_id": merchant_id, "quantity": 1, "unit_price": "9.99"}],
    },
    ("PaymentSucceeded", 1): lambda user_id, merchant_id: {
        "payment_id": user_id,
        "order_id": merchant_id,
        "amount": "9.99",
    },
    ("PaymentFailed", 1): lambda user_id, merchant_id: {
        "payment_id": user_id,
        "order_id": merchant_id,
        "amount": "9.99",
        "reason": "card_declined",
    },
}


def _event_body(event_type: str, schema_version: int) -> dict[str, Any]:
    """A valid full envelope for one registry entry, via its sample factory."""
    data = _EVENT_SAMPLES[(event_type, schema_version)](uuid.uuid4(), uuid.uuid4())
    model = REGISTRY[(event_type, schema_version)]
    return model.new(trace_id="t-enum", data=data).model_dump(mode="json")


def test_every_registry_entry_has_a_sample() -> None:
    """The enumeration map covers the whole registry — a newly added event type
    or ``schema_version`` fails here until ``_EVENT_SAMPLES`` grows an entry."""
    missing = [key for key in REGISTRY if key not in _EVENT_SAMPLES]
    assert not missing, (
        f"event contract(s) {missing} registered without a contract-test sample: "
        "add an entry to _EVENT_SAMPLES in tests/unit/test_events.py"
    )
    assert set(_EVENT_SAMPLES) == set(REGISTRY)  # no stale samples either


@pytest.mark.parametrize(
    ("event_type", "schema_version"),
    sorted(REGISTRY),  # sorted for a stable, readable parametrize list
)
def test_producer_output_validates_for_every_registered_event(event_type: str, schema_version: int) -> None:
    """The exact wire payload a producer emits passes ``validate_event``."""
    body = _event_body(event_type, schema_version)
    assert body["type"] == event_type and body["schema_version"] == schema_version
    normalized = validate_event(json.dumps(body))
    assert normalized["event_id"] == body["event_id"]


@pytest.mark.parametrize(
    ("event_type", "schema_version"),
    sorted(REGISTRY),
)
def test_consumer_rejects_violations_for_every_registered_event(event_type: str, schema_version: int) -> None:
    """Consumer side of the contract, per registered version: a wrong-type payload
    field, an unexpected extra field, and a missing envelope field each fail."""
    body = _event_body(event_type, schema_version)
    model = REGISTRY[(event_type, schema_version)]
    data_fields = model.model_fields["data"].annotation.model_fields

    with pytest.raises(ValidationError):  # a string where the payload wants an int/uuid/number
        bad_type = json.loads(json.dumps(body))
        first = next(iter(data_fields))
        bad_type["data"][first] = ["wrong-type"]
        validate_event(json.dumps(bad_type))

    with pytest.raises(ValidationError):  # extra="forbid" — privileged/unexpected fields
        extra = json.loads(json.dumps(body))
        extra["data"]["role"] = "admin"
        validate_event(json.dumps(extra))

    with pytest.raises(UnknownEventError):  # missing routing field is unroutable, not merely invalid
        unroutable = json.loads(json.dumps(body))
        del unroutable["schema_version"]
        validate_event(json.dumps(unroutable))


# --- product event versioning (v1 frozen, v2 live) -------------------------

# A ``ProductUpdated`` exactly as it was written to ``catalog.outbox`` (or shipped
# to SQS) before the v2 rollout: no ``product_version``. Hard-coded rather than
# built from a model, so it stays a record of the *old wire format* even if the
# v1 model is ever touched.
_V1_ON_THE_WIRE = {
    "type": "ProductUpdated",
    "schema_version": 1,
    "event_id": "0f9d5f9c-4a1a-4a5e-9a4d-3f5a1f4b2c11",
    "trace_id": "trace-abc",
    "occurred_at": "2026-01-01T00:00:00Z",
    "data": {
        "product_id": "6a1f0b5e-1d2c-4f3a-8b7c-9d0e1f2a3b4c",
        "merchant_id": "7b2f1c6d-2e3d-4a4b-9c8d-0e1f2a3b4c5d",
        "name": "widget",
        "price": "9.99",
        "category": "tools",
    },
}


@pytest.mark.parametrize("event_type", ["ProductCreated", "ProductUpdated", "ProductDeleted"])
def test_both_product_event_versions_stay_registered(event_type: str) -> None:
    """v1 must not be de-registered when v2 ships: messages produced before the
    rollout can still be sitting in an outbox table or an SQS queue (plus a DLQ
    replay window), and an unregistered version is an ``UnknownEventError`` — the
    handler raises and the message redrives to the DLQ."""
    assert (event_type, 1) in REGISTRY
    assert (event_type, 2) in REGISTRY


def test_v1_product_message_still_validates_after_the_v2_rollout() -> None:
    """The compatibility guarantee: an in-flight v1 payload keeps validating."""
    normalized = validate_event(json.dumps(_V1_ON_THE_WIRE))

    assert normalized["schema_version"] == 1
    assert "product_version" not in normalized["data"]  # v1 never carried one


def test_adding_product_version_to_v1_would_have_broken_in_flight_messages() -> None:
    """Why the ``schema_version`` bump was required rather than editing v1.

    Payloads are ``extra="forbid"``, so the two directions each fail: a v1 message
    can't satisfy a v1 model that gained a required field, and a v2 message (with
    the extra field) can't validate against the v1 contract.
    """
    v2_body = dict(_V1_ON_THE_WIRE, data={**_V1_ON_THE_WIRE["data"], "product_version": 3})

    with pytest.raises(ValidationError):  # v2 payload against the v1 contract
        validate_event(json.dumps(v2_body))
    with pytest.raises(ValidationError):  # ...and a v1 payload against the v2 contract
        validate_event(json.dumps({**_V1_ON_THE_WIRE, "schema_version": 2}))

    # Same body routed to v2 (where the field belongs) validates.
    assert validate_event(json.dumps({**v2_body, "schema_version": 2}))["data"]["product_version"] == 3


def test_v2_product_version_must_be_a_positive_int() -> None:
    """``ge=1``: version 0 is not a real aggregate version, so it can't order anything."""
    with pytest.raises(ValidationError):
        ProductWriteDataV2(
            product_id=uuid.uuid4(), merchant_id=uuid.uuid4(), name="x", price=Decimal("1.00"), product_version=0
        )


_SRC = Path(__file__).resolve().parents[2] / "src"

#: ``{class name: (event type, schema_version)}`` for every registered model.
_MODEL_KEYS = {m.__name__: key for key, m in REGISTRY.items()}


def _versions_constructed_in_production_code() -> dict[str, set[int]]:
    """Scan ``src/`` (minus the contracts package) for ``<EventModel>.new(`` calls.

    Producers are the only place events are built, and they always go through
    ``.new()``, so this is what actually reaches the bus — as opposed to a constant
    that can silently drift from the code.
    """
    pattern = re.compile(rf"\b({'|'.join(map(re.escape, _MODEL_KEYS))})\.new\(")
    produced: dict[str, set[int]] = {}
    for path in _SRC.rglob("*.py"):
        if path.parent.name == "events":  # the models/registry themselves
            continue
        for name in pattern.findall(path.read_text(encoding="utf-8")):
            event_type, version = _MODEL_KEYS[name]
            produced.setdefault(event_type, set()).add(version)
    return produced


def test_pinned_producer_versions_are_registered() -> None:
    """A producer may only emit a ``(type, version)`` some consumer can parse."""
    assert {t for t, _ in REGISTRY} == set(PRODUCED_VERSIONS)
    for event_type, version in PRODUCED_VERSIONS.items():
        assert (event_type, version) in REGISTRY


def test_production_code_emits_exactly_the_pinned_version() -> None:
    """Consumer-first rollout guard.

    Bumping a producer to a new ``schema_version`` while an older consumer task is
    still running means that consumer raises ``UnknownEventError``, never deletes
    the message, and the queue DLQs it after ``maxReceiveCount``. So the order is
    fixed: deploy V-capable **consumers** first, then flip producers. This test
    fails on the producer half of that change unless ``PRODUCED_VERSIONS`` is
    updated in the same diff, which is the reviewable checkpoint for the rule (see
    ``docs/DEPLOYMENT.md`` § "Rolling out a new event version").
    """
    produced = _versions_constructed_in_production_code()

    # Canary: the scan matches a literal ``<ModelClass>.new(``, so a producer
    # refactored behind an alias (``_EVENT = ProductUpdatedV2`` … ``_EVENT.new(``)
    # or built with a plain constructor would make this test iterate over nothing
    # and pass a version bump it exists to catch. Assert it still sees its subject.
    assert {"ProductCreated", "ProductUpdated", "ProductDeleted"} <= produced.keys(), produced

    for event_type, versions in produced.items():
        assert versions == {PRODUCED_VERSIONS[event_type]}, (
            f"{event_type} is produced at {sorted(versions)} but pinned to "
            f"{PRODUCED_VERSIONS[event_type]}; deploy V2-capable consumers before bumping producers"
        )


def test_request_dto_ignores_unexpected_privileged_field() -> None:
    """Mass-assignment guard (AC1): a body field the DTO doesn't declare is dropped, not bound."""
    from src.identity.api.schemas import CreateUserRequest

    dto = CreateUserRequest.model_validate({"email": "a@b.com", "role": "admin"})
    assert not hasattr(dto, "role")
