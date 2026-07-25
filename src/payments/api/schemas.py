"""Payments wire schema — the response shape for a payment row.

``from_attributes`` lets the service build this straight off the domain
``Payment`` dataclass. ``status`` is a plain ``str`` (the column is free-form,
not an enum), passed through unchanged.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict


class PaymentResponse(BaseModel):
    """The ``payments.payments`` row as returned by the service layer."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    order_id: uuid.UUID
    status: str
    amount: Decimal
    gateway_ref: str | None
    created_at: datetime
    updated_at: datetime
