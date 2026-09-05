"""The outbox payload-size guard (SNS/SQS 256 KB cap).

One oversized row would sit unpublished ahead of its schema's lane in the relay
and block that module's events indefinitely, so the mixin refuses it at insert
time — for every module's ``Outbox`` model, current and future.
"""

from __future__ import annotations

import pytest

from src.catalog.adapters.db.models import Outbox as CatalogOutbox
from src.identity.adapters.db.models import Outbox as IdentityOutbox
from src.inventory.adapters.db.models import Outbox as InventoryOutbox
from src.orders.adapters.db.models import Outbox as OrdersOutbox
from src.payments.adapters.db.models import Outbox as PaymentsOutbox
from src.shared.db.outbox import MAX_OUTBOX_PAYLOAD_BYTES, OutboxPayloadTooLargeError

_ALL_OUTBOXES = (CatalogOutbox, IdentityOutbox, InventoryOutbox, OrdersOutbox, PaymentsOutbox)

_OVERLIMIT = "x" * (MAX_OUTBOX_PAYLOAD_BYTES + 1)


@pytest.mark.parametrize("model", _ALL_OUTBOXES, ids=lambda m: m.__module__.split(".")[-3])
def test_every_modules_outbox_rejects_an_oversized_payload(model) -> None:  # noqa: ANN001
    with pytest.raises(OutboxPayloadTooLargeError):
        model(event_type="Test", payload=_OVERLIMIT)


@pytest.mark.parametrize("model", _ALL_OUTBOXES, ids=lambda m: m.__module__.split(".")[-3])
def test_a_payload_at_the_cap_is_accepted(model) -> None:  # noqa: ANN001
    row = model(event_type="Test", payload="x" * MAX_OUTBOX_PAYLOAD_BYTES)
    assert row.payload == "x" * MAX_OUTBOX_PAYLOAD_BYTES
