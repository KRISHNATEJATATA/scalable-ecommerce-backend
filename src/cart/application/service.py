"""Cart use-cases — per-user basket over the Valkey repository port.

Thin policy shell: boundary validation (settings-driven caps → 400) and the
product-liveness checks (unknown/soft-deleted product, or a product gone since
the line was added → 404) sit here; the atomic read-modify-write lives in the
adapter's Lua scripts. Returns Pydantic responses, never domain or driver rows.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from fastapi import HTTPException, status

from src.cart.application.dto import CartItemResponse, CartResponse
from src.cart.domain.cart import Cart
from src.cart.ports.products import CartProductPort
from src.cart.ports.repository import CartRepositoryPort
from src.shared.errors.exceptions import InvalidCartOperationError

_NOT_FOUND = HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="product not found")
_LINE_NOT_FOUND = HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="line not in cart")


def _to_response(cart: Cart | None) -> CartResponse:
    """Map a domain cart (or its absence) to the wire shape.

    An absent/expired cart is the empty cart (200), never 404 — with a fresh
    ``updated_at`` since there is no stored stamp to report.
    """
    if cart is None:
        return CartResponse(items=[], updated_at=datetime.now(UTC))
    return CartResponse(
        items=[
            CartItemResponse(
                product_id=uuid.UUID(line.product_id),
                name=line.name,
                unit_price=line.unit_price,
                image_url=line.image_url,
                quantity=line.quantity,
            )
            for line in cart.items
        ],
        updated_at=datetime.fromisoformat(cart.updated_at) if cart.updated_at else datetime.now(UTC),
    )


class CartService:
    """Add/update/remove/read use-cases over one user's Valkey cart."""

    def __init__(
        self,
        repo: CartRepositoryPort,
        products: CartProductPort,
        *,
        max_items: int,
        max_qty_per_line: int,
    ) -> None:
        self._repo = repo
        self._products = products
        self._max_items = max_items
        self._max_qty_per_line = max_qty_per_line

    def _check_quantity(self, quantity: int) -> None:
        """Reject a malformed quantity at the trust boundary (400, not 422)."""
        if quantity < 1 or quantity > self._max_qty_per_line:
            raise InvalidCartOperationError(f"quantity {quantity} out of range (1..{self._max_qty_per_line})")

    async def get_cart(self, user_id: uuid.UUID) -> CartResponse:
        """Return the caller's cart (empty when absent or expired)."""
        return _to_response(await self._repo.get_cart(user_id))

    async def add_item(self, user_id: uuid.UUID, *, product_id: uuid.UUID, quantity: int) -> CartResponse:
        """Add the line or increment it (increment past the cap clamps, never 400s).

        Unknown or soft-deleted product → 404: a cart must never reference a
        product that cannot be checked out.
        """
        self._check_quantity(quantity)
        snapshot = await self._products.get_snapshot(product_id)
        if snapshot is None:
            raise _NOT_FOUND
        cart = await self._repo.add_item(
            user_id,
            product_id=product_id,
            name=snapshot.name,
            unit_price=str(snapshot.unit_price),
            image_url=snapshot.image_url,
            quantity=quantity,
            max_items=self._max_items,
            max_per_line=self._max_qty_per_line,
        )
        return _to_response(cart)

    async def set_quantity(self, user_id: uuid.UUID, *, product_id: uuid.UUID, quantity: int) -> CartResponse:
        """Set the line quantity; ``0`` removes the line.

        404 when the line is not in the cart — including a ``0`` for an absent
        line — or when its product is gone (the stale line is pruned as part of
        answering). Over-cap quantities → 400. Absent-vs-present is decided by
        the atomic set-quantity op itself, not checked-then-removed.
        """
        if quantity < 0 or quantity > self._max_qty_per_line:
            raise InvalidCartOperationError(f"quantity {quantity} out of range (0..{self._max_qty_per_line})")
        if await self._products.get_snapshot(product_id) is None:
            await self._repo.remove_item(user_id, product_id=product_id)
            raise _NOT_FOUND
        cart = await self._repo.set_quantity(
            user_id, product_id=product_id, quantity=quantity, max_per_line=self._max_qty_per_line
        )
        if cart is None:
            raise _LINE_NOT_FOUND
        return _to_response(cart)

    async def remove_item(self, user_id: uuid.UUID, *, product_id: uuid.UUID) -> CartResponse:
        """Remove the line. Idempotent: an absent line still answers 200."""
        return _to_response(await self._repo.remove_item(user_id, product_id=product_id))

    async def clear_cart(self, user_id: uuid.UUID) -> None:
        """Empty the whole cart."""
        await self._repo.clear_cart(user_id)
