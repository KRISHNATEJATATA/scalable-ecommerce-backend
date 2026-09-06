"""Orders use-cases: reads plus the ownership-checked cancel.

``list_orders`` is always scoped to ``user_id`` by the repo. Single-order reads
and cancel verify ``order.user_id == caller`` in this layer (never the route),
with ``admin`` bypassing ownership but never the role gate — the same shape as
catalog's ``_assert_owner``. ORM rows never cross the boundary: everything
returns a Pydantic response schema, and a missing row is ``None`` (the route
maps it to 404).
"""

from __future__ import annotations

import uuid

from src.orders.application.dto import OrderResponse
from src.orders.application.mappers import to_domain
from src.orders.domain.order import OrderStatus
from src.orders.ports.checkout import StockHoldsPort
from src.orders.ports.repository import OrdersRepositoryPort
from src.shared.db.pagination import PageParams, PageResponse
from src.shared.errors.exceptions import AuthorizationError, OrderStateConflictError


class OrdersService:
    """Read-side use-cases plus cancellation over the order aggregate."""

    def __init__(self, repo: OrdersRepositoryPort, holds: StockHoldsPort | None = None) -> None:
        self._repo = repo
        self._holds = holds

    async def get_order(self, order_id: uuid.UUID) -> OrderResponse | None:
        """Resolve an order (with its lines) by id, or ``None`` if absent.

        Unscoped legacy read — routes prefer :meth:`get_order_detail`.
        """
        row = await self._repo.get_order(order_id)
        if row is None:
            return None
        return OrderResponse.model_validate(to_domain(row))

    async def get_order_detail(
        self, *, user_id: uuid.UUID, order_id: uuid.UUID, is_admin: bool
    ) -> OrderResponse | None:
        """Resolve one order for its owner (``admin`` bypasses); another user's id → 403."""
        row = await self._repo.get_order(order_id)
        if row is None:
            return None
        self._assert_owner(row.user_id, user_id, is_admin)
        return OrderResponse.model_validate(to_domain(row))

    async def list_orders(
        self, user_id: uuid.UUID, params: PageParams, status: OrderStatus | None = None
    ) -> PageResponse[OrderResponse]:
        """Return a keyset page of a user's orders, optionally filtered by status."""
        page = await self._repo.list_orders(user_id, params, status)
        items = [OrderResponse.model_validate(to_domain(row)) for row in page.items]
        return PageResponse(items=items, next_cursor=page.next_cursor)

    async def cancel_order(self, *, user_id: uuid.UUID, order_id: uuid.UUID, is_admin: bool) -> OrderResponse | None:
        """Cancel the caller's own ``pending`` order (``admin`` bypasses ownership).

        The guarded ``pending → cancelled`` flip is won **first**; holds are
        released only afterwards. A cancelled order with an orphaned hold
        self-heals (the reaper releases it after the TTL), but a released hold
        on an order that a concurrent checkout drive marks ``paid`` would give
        sold stock back to the shelf — so the flip, which the guarded UPDATE
        serializes against the saga's own ``mark_paid``, must decide first.
        Cancelling an already-``cancelled`` order is idempotent (returns it,
        and finishes any cleanup a crashed cancel left behind); a
        ``paid``/``shipped`` order is rejected — its undo is the future
        returns/refunds reverse saga, not a cancel flag.
        """
        if self._holds is None:  # pragma: no cover - misconfiguration guard
            raise RuntimeError("orders service requires stock holds to cancel")
        row = await self._repo.get_order(order_id)
        if row is None:
            return None
        self._assert_owner(row.user_id, user_id, is_admin)
        if row.status == OrderStatus.CANCELLED:
            await self._holds.release_for_order(order_id)  # finish a crash between flip and release
            return OrderResponse.model_validate(to_domain(row))
        if row.status != OrderStatus.PENDING:
            raise OrderStateConflictError(f"only pending orders can be cancelled (order is {row.status})")
        # A charge may be in flight or already landed (a crashed drive leaves
        # the journal row behind, a live one writes ``started`` before the
        # gateway call): cancelling now could take money without an order.
        # Those orders belong to the recovery poller/reconciler; the caller
        # retries once the outcome settles.
        charge_state = await self._repo.latest_saga_step(order_id, "charge")
        if charge_state in ("started", "completed"):
            raise OrderStateConflictError("payment for this order is in progress; try again once it settles")
        updated = await self._repo.transition_status(
            order_id, expect=[OrderStatus.PENDING], to_status=OrderStatus.CANCELLED
        )
        if updated is None:
            # Lost a race with the saga settling the order itself — re-read the
            # settled truth rather than reporting a cancel that didn't land.
            final = await self._repo.get_order(order_id)
            if final is None or final.status != OrderStatus.CANCELLED:
                raise OrderStateConflictError("the order settled while cancelling; re-read it")
            return OrderResponse.model_validate(to_domain(final))
        await self._holds.release_for_order(order_id)
        await self._repo.log_saga_step(order_id, "cancel", "completed")
        final = await self._repo.get_order(order_id)
        if final is None:  # defensive: the row we just cancelled must re-read
            raise RuntimeError(f"cancelled order {order_id} not found after flip")
        return OrderResponse.model_validate(to_domain(final))

    @staticmethod
    def _assert_owner(owner_id: uuid.UUID, caller_id: uuid.UUID, is_admin: bool) -> None:
        """A consumer may only read/cancel their own orders; ``admin`` bypasses ownership."""
        if owner_id != caller_id and not is_admin:
            raise AuthorizationError("not the owner of this order")
