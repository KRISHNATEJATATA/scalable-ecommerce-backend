"""Cart HTTP routes — the caller's own Valkey basket.

Routes stay thin: authenticate via the shared dependency (any role — every
route operates on the caller's own cart through ``CurrentUserDep``, which also
JIT-provisions the local user row), call the service, map ``None`` → 404.
No pagination (carts are small). All errors are RFC 9457 Problem Details.
"""

from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, status

from src.cart.api.schemas import CartAddItem, CartResponse, CartUpdateItem
from src.cart.application.service import CartService
from src.shared.container import CurrentUserDep, get_cart_service

router = APIRouter(prefix="/cart", tags=["cart"])

CartServiceDep = Annotated[CartService, Depends(get_cart_service)]


@router.get("", response_model=CartResponse)
async def get_cart(service: CartServiceDep, caller: CurrentUserDep) -> CartResponse:
    """Return the caller's cart. An empty cart is 200, never 404."""
    return await service.get_cart(caller.id)


@router.post("/items", response_model=CartResponse)
async def add_item(body: CartAddItem, service: CartServiceDep, caller: CurrentUserDep) -> CartResponse:
    """Add the line or increment it (clamped to the per-line cap).

    400 when the quantity is out of range or the cart is full; 404 when the
    product is unknown or soft-deleted.
    """
    return await service.add_item(caller.id, product_id=body.product_id, quantity=body.quantity)


@router.patch("/items/{product_id}", response_model=CartResponse)
async def set_quantity(
    product_id: uuid.UUID, body: CartUpdateItem, service: CartServiceDep, caller: CurrentUserDep
) -> CartResponse:
    """Set the line quantity; ``0`` removes the line.

    400 when out of range; 404 when the line is not in the cart, or its product
    is gone (the stale line is pruned as part of answering).
    """
    return await service.set_quantity(caller.id, product_id=product_id, quantity=body.quantity)


@router.delete("/items/{product_id}", response_model=CartResponse)
async def remove_item(product_id: uuid.UUID, service: CartServiceDep, caller: CurrentUserDep) -> CartResponse:
    """Remove the line. Idempotent: an absent line still answers 200."""
    return await service.remove_item(caller.id, product_id=product_id)


@router.delete("", status_code=status.HTTP_204_NO_CONTENT, response_model=None)
async def clear_cart(service: CartServiceDep, caller: CurrentUserDep) -> None:
    """Clear the whole cart."""
    await service.clear_cart(caller.id)
