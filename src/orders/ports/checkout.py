"""Ports for the checkout saga's cross-module calls.

Implemented at the composition root (``src/shared/container.py``) over the
inventory/payments/cart services — orders never names another module (the
``module-independence`` contract forbids even application-layer imports between
modules). Each port speaks the saga's language:

* :class:`BasketPort` — the pre-checkout lines to buy, and clearing them after.
* :class:`StockHoldsPort` — hold/commit/release stock for one order.
* :class:`ChargePort` — charge for one order, and look the attempt back up.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Protocol


@dataclass(frozen=True, slots=True)
class CheckoutLine:
    """One buyable line: the cart's snapshot (never live catalog data)."""

    product_id: uuid.UUID
    name: str
    unit_price: Decimal
    quantity: int


@dataclass(frozen=True, slots=True)
class ChargeResult:
    """The saga-relevant outcome of one payment attempt."""

    status: str  # "succeeded" | "failed" | "pending"

    @property
    def succeeded(self) -> bool:
        """The money moved (the only state that completes a checkout)."""
        return self.status == "succeeded"

    @property
    def failed(self) -> bool:
        """The attempt reached a terminal non-success (declined, abandoned)."""
        return self.status == "failed"


class BasketPort(Protocol):
    async def get_lines(self, user_id: uuid.UUID) -> list[CheckoutLine]:
        """The user's current buyable lines (cart snapshots), or ``[]`` when empty."""
        ...

    async def clear(self, user_id: uuid.UUID) -> None:
        """Empty the basket after a successful checkout."""
        ...


class StockHoldsPort(Protocol):
    async def reserve(self, sku: str, qty: int, order_id: uuid.UUID) -> uuid.UUID:
        """Hold ``qty`` of ``sku`` for ``order_id``; raises ``InsufficientStockError`` (409) when refused."""
        ...

    async def release_for_order(self, order_id: uuid.UUID) -> int:
        """Release every still-``held`` reservation of one order (compensation); returns how many."""
        ...

    async def commit_for_order(self, order_id: uuid.UUID) -> int:
        """Consume every still-``held`` reservation of one order (success); returns how many."""
        ...


class ChargePort(Protocol):
    async def charge(
        self, *, order_id: uuid.UUID, idempotency_key: str, amount: Decimal, payment_token: str
    ) -> ChargeResult:
        """Charge ``amount`` for ``order_id``; idempotent on ``idempotency_key`` (no double charge)."""
        ...

    async def find_by_idempotency_key(self, idempotency_key: str) -> ChargeResult | None:
        """The recorded outcome for ``idempotency_key``, or ``None`` if never charged."""
        ...


@dataclass(frozen=True, slots=True)
class IdempotencyRecord:
    """One fast-path replay answer: the pinned body hash plus the stored response."""

    body_hash: str
    status: int
    response: dict


class IdempotencyPort(Protocol):
    """Valkey fast path for ``Idempotency-Key`` replays (a cache, never the truth)."""

    async def get(self, user_id: uuid.UUID, idempotency_key: str) -> IdempotencyRecord | None:
        """The stored replay answer, or ``None`` on miss/eviction/failure."""
        ...

    async def put(
        self,
        user_id: uuid.UUID,
        idempotency_key: str,
        *,
        body_hash: str,
        status: int,
        response: Any,
    ) -> None:
        """Store the replay answer (``response`` serializes to JSON)."""
        ...
