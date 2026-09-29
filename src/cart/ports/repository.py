"""Port (Protocol) for the cart repository.

Implemented by ``adapters/valkey/repository.ValkeyCartRepository``. Every
mutation is a single atomic Valkey operation (a Lua script over the cart hash
plus its rolling TTL) — never a ``GET``-then-``SET`` across round-trips, so two
devices mutating the same user's cart cannot lose each other's lines.
"""

from __future__ import annotations

import uuid
from typing import Protocol

from src.cart.domain.cart import Cart


class CartRepositoryPort(Protocol):
    async def get_cart(self, user_id: uuid.UUID) -> Cart | None:
        """The user's cart, or ``None`` when it holds nothing (absent or expired)."""
        ...

    async def add_item(
        self,
        user_id: uuid.UUID,
        *,
        product_id: uuid.UUID,
        name: str,
        unit_price: str,
        image_url: str | None,
        quantity: int,
        max_items: int,
        max_per_line: int,
        product_version: int | None = None,
    ) -> Cart:
        """Add the line or increment it, atomically.

        An increment past ``max_per_line`` clamps to the cap (never rejects a
        well-formed add); a *new* line past ``max_items`` raises
        :class:`InvalidCartOperationError` (→ 400). Returns the resulting cart.
        ``product_version`` lets the adapter ignore projections older than the
        catalog snapshot; ``None`` supports legacy unversioned callers.
        """
        ...

    async def set_quantity(
        self,
        user_id: uuid.UUID,
        *,
        product_id: uuid.UUID,
        quantity: int,
        max_per_line: int,
    ) -> Cart | None:
        """Set the line quantity (``0`` removes the line), atomically.

        ``None`` when the line is not in the cart; an *empty* ``Cart`` when the
        op itself emptied it (removing the last line succeeds — only a missing
        line 404s). Over-cap quantities are rejected by the caller (400), not
        clamped here — clamping is the increment path's rule only.
        """
        ...

    async def remove_item(self, user_id: uuid.UUID, *, product_id: uuid.UUID) -> Cart | None:
        """Remove the line, atomically. Idempotent: an absent line is a no-op.

        ``None`` when the cart is empty afterwards (or was all along) — the
        caller answers the empty cart, never 404.
        """
        ...

    async def clear_cart(self, user_id: uuid.UUID) -> None:
        """Empty the whole cart."""
        ...

    async def consume_lines(self, user_id: uuid.UUID, *, lines: list[tuple[uuid.UUID, int]]) -> None:
        """Subtract purchased ``(product_id, quantity)`` pairs, atomically.

        Checkout's success path: decrement each purchased line (a line that
        reaches zero is dropped; a cart left holding only
        meta is deleted) — but lines added or changed while the checkout ran
        survive untouched, and absent lines are a no-op.
        """
        ...

    async def refresh_product(
        self,
        product_id: uuid.UUID,
        *,
        name: str,
        unit_price: str,
        product_version: int | None,
    ) -> int:
        """Record the latest update for lazy reconciliation on cart read.

        Older events and updates after a tombstone are no-ops. Returns 1 if
        the product projection changed, otherwise 0.
        """
        ...

    async def prune_product(self, product_id: uuid.UUID) -> int:
        """Record a permanent tombstone; each cart prunes its line on read.

        Always wins, regardless of versions. Returns 1 if newly recorded.
        """
        ...
