"""Orders HTTP routes — checkout plus the ownership-checked order history.

Routes stay thin: authenticate via the shared dependencies (any role — every
route operates on the caller's own orders through ``CurrentUserDep``), call the
saga/service, map ``None`` → 404. Ownership is enforced in the service layer,
not here. All errors are RFC 9457 Problem Details; money is decimal-as-string.
"""

from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request, status

from src.orders.api.schemas import CheckoutRequest, OrderExecutionResponse, OrderResponse
from src.orders.application.checkout_saga import CheckoutSaga
from src.orders.application.service import OrdersService
from src.orders.domain.order import OrderStatus
from src.shared.api.query import reject_unknown_query_params
from src.shared.auth.dependencies import PrincipalDep
from src.shared.auth.principal import Principal
from src.shared.container import CurrentUserDep, get_checkout_saga, get_orders_service
from src.shared.db.pagination import DEFAULT_LIMIT, MAX_LIMIT, PageParams, PageResponse
from src.shared.errors.exceptions import InvalidQueryParamError

router = APIRouter(prefix="/orders", tags=["orders"])
checkout_router = APIRouter(prefix="/checkout", tags=["checkout"])

OrdersServiceDep = Annotated[OrdersService, Depends(get_orders_service)]
CheckoutSagaDep = Annotated[CheckoutSaga, Depends(get_checkout_saga)]

_NOT_FOUND = HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="order not found")

# Every query param the history listing understands; anything else is a 400
# (like the product listing) rather than a silently unfiltered page.
_LIST_QUERY_PARAMS = frozenset({"limit", "sort", "cursor", "status"})

_STATUS_VALUES = {
    "pending": OrderStatus.PENDING,
    "paid": OrderStatus.PAID,
    "shipped": OrderStatus.SHIPPED,
    "cancelled": OrderStatus.CANCELLED,
}

# Header(...) without a default is required: a missing Idempotency-Key answers
# 422, and an empty one is rejected the same way — the key is what makes a
# retry safe to send, so checkout refuses to run without it. The max keeps the
# derived gateway key (``checkout:{user_id}:{key}`` → 46-char prefix) inside
# payments' VARCHAR(255): an oversized key must be a 422 the client fixes, not
# a DataError 500 from the DB after the order row already exists.
IdempotencyKeyDep = Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=200)]


@checkout_router.post("", response_model=OrderResponse, status_code=status.HTTP_201_CREATED)
async def checkout(
    body: CheckoutRequest,
    saga: CheckoutSagaDep,
    caller: CurrentUserDep,
    idempotency_key: IdempotencyKeyDep,
) -> OrderResponse:
    """Run the cart through the checkout saga (reserve → charge → commit → paid).

    Same key + same body replays the stored ``201``; same key + different body
    → ``409``; stock failure → ``409`` with the order already cancelled and
    compensation run (show "out of stock", not a retry loop); replaying an
    already-cancelled checkout re-raises its ``409`` — a retry needs a new
    key. Generate a new key when the cart changes — the key pins the first
    request it was sent with.
    """
    order, _created = await saga.checkout(
        user_id=caller.id, idempotency_key=idempotency_key, payment_token=body.payment_token
    )
    return order


@router.get("", response_model=PageResponse[OrderResponse])
async def list_orders(
    request: Request,
    service: OrdersServiceDep,
    caller: CurrentUserDep,
    limit: Annotated[int, Query(ge=1, le=MAX_LIMIT)] = DEFAULT_LIMIT,
    sort: Annotated[str, Query(description="field name, optional leading '-' for descending")] = "-created_at",
    cursor: str | None = None,
    status_filter: Annotated[str | None, Query(alias="status")] = None,
) -> PageResponse[OrderResponse]:
    """Keyset-paginated history of the caller's own orders, optionally filtered by status."""
    reject_unknown_query_params(request, _LIST_QUERY_PARAMS)
    order_status: OrderStatus | None = None
    if status_filter is not None:
        try:
            order_status = _STATUS_VALUES[status_filter]
        except KeyError:
            raise InvalidQueryParamError("status", status_filter) from None
    return await service.list_orders(caller.id, PageParams(limit=limit, sort=sort, cursor=cursor), order_status)


@router.get("/{order_id}", response_model=OrderResponse)
async def get_order(
    order_id: uuid.UUID,
    service: OrdersServiceDep,
    caller: CurrentUserDep,
    principal: PrincipalDep,
) -> OrderResponse:
    """Fetch one of the caller's orders; another user's id → 403."""
    order = await service.get_order_detail(user_id=caller.id, order_id=order_id, is_admin=_is_admin(principal))
    if order is None:
        raise _NOT_FOUND
    return order


@router.get("/{order_id}/execution", response_model=OrderExecutionResponse)
async def get_order_execution(
    order_id: uuid.UUID,
    service: OrdersServiceDep,
    caller: CurrentUserDep,
    principal: PrincipalDep,
) -> OrderExecutionResponse:
    """The order's checkout-saga execution trace: its journal, oldest step first (read-only).

    What the checkout actually did — steps, per-attempt statuses, timestamps — as the
    saga wrote it into ``orders.saga_log``. Same ownership rule as the order GET
    (another user's id → 403); a pure read with no side effects.
    """
    execution = await service.get_execution(user_id=caller.id, order_id=order_id, is_admin=_is_admin(principal))
    if execution is None:
        raise _NOT_FOUND
    return execution


@router.post("/{order_id}/cancel", response_model=OrderResponse)
async def cancel_order(
    order_id: uuid.UUID,
    service: OrdersServiceDep,
    caller: CurrentUserDep,
    principal: PrincipalDep,
) -> OrderResponse:
    """Cancel the caller's own ``pending`` order (releases its holds).

    Already-``cancelled`` is idempotent (returns it); ``paid``/``shipped`` →
    ``409`` — refunds are the future reverse saga, not this endpoint.
    """
    order = await service.cancel_order(user_id=caller.id, order_id=order_id, is_admin=_is_admin(principal))
    if order is None:
        raise _NOT_FOUND
    return order


def _is_admin(principal: Principal) -> bool:
    """Ownership bypass (never a role-membership gate — see ``require_role``)."""
    return principal.is_admin
