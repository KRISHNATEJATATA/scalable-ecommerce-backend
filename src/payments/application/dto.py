"""Payments application DTO — the service layer's output shape.

Lives in ``application`` (not ``api``) so the service never depends on the
outer API layer (layers contract: api -> application -> domain). ``api``
re-exports this for route type hints / OpenAPI.
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
