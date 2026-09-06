"""Orders read + checkout-write repository.

Reads (``list_orders``/``get_order``) are always scoped and eager-load items.
Writes serve the checkout saga, each step **one transaction**:

* :meth:`create_pending_order` — the ``pending`` order + its snapshotted lines
  + the first ``saga_log`` row. A concurrent retry under the same
  ``(user_id, idempotency_key)`` hits the composite UNIQUE, rolls back, and is
  handed the winner's row — exactly one order per key, decided in the DB.
* :meth:`transition_status` — a guarded ``status IN (...) → to`` UPDATE. Zero
  rows means the lifecycle refused (already settled or already cancelled): the
  caller sees ``None``, not a clobbered state. The ``OrderPlaced`` outbox row
  rides the same transaction as the ``pending → paid`` flip.
* :meth:`claim_stuck_pending` — the recovery poller's lease: ``pending`` orders
  past the step timeout are touched (``updated_at = now()``) under
  ``FOR UPDATE SKIP LOCKED`` and returned, so concurrent poller replicas split
  the batch instead of double-settling a saga.

Returns ORM rows, never response schemas — services map them.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal

from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import text

from src.orders.adapters.db.models import SCHEMA, Order, OrderItem, OrderStatus, Outbox, SagaLog
from src.shared.db.outbox import OutboxMessage
from src.shared.db.pagination import Page, PageParams, apply_keyset, build_page, decode_cursor
from src.shared.errors.exceptions import InvalidQueryParamError

_SORT_COLUMNS = {"created_at": Order.created_at}

# SQLSTATEs, not constraint names: names drift with migrations, these are standard.
_UNIQUE_VIOLATION = "23505"


def _sqlstate(exc: IntegrityError) -> str | None:
    """The SQLSTATE behind a SQLAlchemy ``IntegrityError`` (asyncpg nests one level deep)."""
    for candidate in (exc.orig, getattr(exc.orig, "__cause__", None)):
        state = getattr(candidate, "sqlstate", None) or getattr(candidate, "pgcode", None)
        if state:
            return str(state)
    return None


# `SCHEMA` is a fixed module constant, never user input, so interpolating it
# into the identifier position is safe (noqa: S608).
#
# The NOT EXISTS is the liveness heartbeat: a live drive journals every step and
# each step is bounded by the step timeout, so its newest ``saga_log`` row is
# always younger than the cutoff. Only orders whose journal has gone quiet — a
# true crash — are claimable; without this guard the poller could claim an
# in-flight checkout that is merely slow and cancel a charge that then succeeds.
_CLAIM_STUCK_SQL = text(
    f"WITH claimed AS ("  # noqa: S608
    f"SELECT o.id FROM {SCHEMA}.orders o "  # noqa: S608
    "WHERE o.status = 'pending' AND o.updated_at < :cutoff "
    f"AND NOT EXISTS (SELECT 1 FROM {SCHEMA}.saga_log s "  # noqa: S608
    "WHERE s.order_id = o.id AND s.updated_at >= :cutoff) "
    "ORDER BY o.updated_at FOR UPDATE SKIP LOCKED LIMIT :batch"
    f") UPDATE {SCHEMA}.orders o SET updated_at = now() "  # noqa: S608
    "FROM claimed WHERE o.id = claimed.id RETURNING o.id"
)


class OrdersRepository:
    """Implements :class:`src.orders.ports.repository.OrdersRepositoryPort`."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    # --- reads ------------------------------------------------------

    async def list_orders(
        self, user_id: uuid.UUID, params: PageParams, status: OrderStatus | None = None
    ) -> Page[Order]:
        if params.sort_field not in _SORT_COLUMNS:
            raise InvalidQueryParamError("sort", params.sort_field)
        sort_col = _SORT_COLUMNS[params.sort_field]

        stmt = select(Order).where(Order.user_id == user_id).options(selectinload(Order.items))
        if status is not None:
            stmt = stmt.where(Order.status == status)

        cursor = decode_cursor(params.cursor, "timestamptz") if params.cursor else None
        stmt = apply_keyset(stmt, sort_col, Order.id, params, cursor)

        rows = list((await self._session.execute(stmt)).scalars().all())
        return build_page(rows, params, key_of=lambda order: (getattr(order, params.sort_field), order.id))

    async def get_order(self, order_id: uuid.UUID) -> Order | None:
        stmt = select(Order).where(Order.id == order_id).options(selectinload(Order.items))
        return (await self._session.execute(stmt)).scalar_one_or_none()

    async def get_by_idempotency(self, user_id: uuid.UUID, idempotency_key: str) -> Order | None:
        """The order already placed under ``(user_id, key)``, with lines, or ``None``."""
        stmt = (
            select(Order)
            .where(Order.user_id == user_id, Order.idempotency_key == idempotency_key)
            .options(selectinload(Order.items))
        )
        return (await self._session.execute(stmt)).scalar_one_or_none()

    # --- checkout writes --------------------------------------------

    async def create_pending_order(
        self,
        *,
        user_id: uuid.UUID,
        idempotency_key: str,
        body_hash: str,
        total: Decimal,
        lines: list[tuple[uuid.UUID, str, Decimal, int]],
    ) -> tuple[Order, bool]:
        """Insert the ``pending`` order + snapshotted lines; a key replay returns the winner.

        Returns ``(order, created)``: exactly one row per ``(user_id, key)``
        survives a concurrent double-submit, decided by the composite UNIQUE —
        the loser rolls back and reads the winner's row instead of failing.
        Any other integrity failure is re-raised, never mistranslated into a
        replay answer.
        """
        order = Order(
            user_id=user_id,
            idempotency_key=idempotency_key,
            idempotency_body_hash=body_hash,
            status=OrderStatus.PENDING,
            total=total,
        )
        self._session.add(order)
        await self._session.flush()  # assign the id for the lines below
        for product_id, product_name, unit_price, quantity in lines:
            self._session.add(
                OrderItem(
                    order_id=order.id,
                    product_id=product_id,
                    product_name=product_name,
                    unit_price=unit_price,
                    quantity=quantity,
                )
            )
        self._session.add(SagaLog(order_id=order.id, step="create", status="completed"))
        try:
            await self._session.commit()
        except IntegrityError as exc:
            await self._session.rollback()
            if _sqlstate(exc) != _UNIQUE_VIOLATION:
                raise
            existing = await self.get_by_idempotency(user_id, idempotency_key)
            if existing is None:  # defensive: conflict reported but the winner isn't visible
                raise RuntimeError(f"idempotency conflict for {idempotency_key!r} but no order found") from exc
            return existing, False
        row = await self.get_order(order.id)
        if row is None:  # defensive: the order we just inserted must be re-readable
            raise RuntimeError(f"inserted order {order.id} not found after commit")
        return row, True

    async def transition_status(
        self,
        order_id: uuid.UUID,
        *,
        expect: list[OrderStatus],
        to_status: OrderStatus,
        outbox: OutboxMessage | None = None,
    ) -> Order | None:
        """Guarded ``status IN expect → to`` flip; ``None`` when the lifecycle refused.

        Raw UPDATE (not the ORM unit-of-work) so a crash-recovery replay racing a
        live request serializes in the DB — the loser matches zero rows instead
        of clobbering. The outbox row (``OrderPlaced`` on ``→ paid``) commits in
        the same transaction as the flip, from values that don't include the
        status itself, so it can never announce a state that didn't land.
        """
        row = (
            await self._session.execute(
                update(Order)
                .where(Order.id == order_id, Order.status.in_(expect))
                .values(status=to_status)
                .returning(Order.id)
                # Bypassed-ORM read afterwards (see below), so don't expire the
                # identity map into a synchronous lazy-load (MissingGreenlet).
                .execution_options(synchronize_session=False)
            )
        ).first()
        if row is None:
            await self._session.rollback()
            return None
        if outbox is not None:
            self._session.add(Outbox(event_type=outbox.event_type, payload=outbox.payload))
        await self._session.commit()
        # Re-read, then refresh the mutated columns: ``get_order`` may hand back
        # the identity-map instance holding pre-UPDATE values (the raw statement
        # bypassed the ORM unit of work). Only columns are refreshed — the items
        # collection stays as ``selectinload`` loaded it (``lazy="raise"`` would
        # reject a lazily re-fetched relationship).
        updated = await self.get_order(order_id)
        if updated is not None:
            await self._session.refresh(updated, attribute_names=["status", "updated_at"])
        return updated

    async def log_saga_step(self, order_id: uuid.UUID, step: str, status: str) -> None:
        """Journal one saga step attempt (recovery + compensation read this, not the code path)."""
        self._session.add(SagaLog(order_id=order_id, step=step, status=status))
        await self._session.commit()

    async def latest_saga_step(self, order_id: uuid.UUID, step: str) -> str | None:
        """The most recent journal status for one step (``None`` if never attempted).

        The cancel endpoint's guard: a ``charge`` row stuck at ``started`` or
        ``completed`` means money may be moving or has moved for this still-
        ``pending`` order, so cancelling it is refused.
        """
        stmt = (
            select(SagaLog.status)
            .where(SagaLog.order_id == order_id, SagaLog.step == step)
            .order_by(SagaLog.created_at.desc())
            .limit(1)
        )
        return (await self._session.execute(stmt)).scalar_one_or_none()

    async def has_recent_saga_activity(self, order_id: uuid.UUID, *, since: datetime) -> bool:
        """Whether any journal row for the order was written after ``since``.

        The recovery poller's post-claim re-check: between claiming a quiet
        order and settling it, a client retry can start driving the same order
        (its resume path journals immediately). One indexed lookup per claimed
        order closes that claim→settle gap instead of racing it.
        """
        stmt = select(SagaLog.id).where(SagaLog.order_id == order_id, SagaLog.updated_at >= since).limit(1)
        return (await self._session.execute(stmt)).scalar_one_or_none() is not None

    async def rollback(self) -> None:
        """Drop any half-finished transaction (recovery's per-order error boundary).

        A failed ``execute`` leaves the session's transaction aborted; every
        later statement would fail with ``PendingRollbackError`` and poison the
        rest of the recovery batch. Roll back so the batch can continue.
        """
        await self._session.rollback()

    async def claim_stuck_pending(self, *, cutoff: datetime, batch_size: int) -> list[Order]:
        """Lease ``pending`` orders older than ``cutoff`` for the recovery poller.

        The ``updated_at = now()`` touch *is* the lease: a replica that runs
        while we settle sees a fresh timestamp and skips the row, and
        ``SKIP LOCKED`` keeps two replicas starting the same instant from
        claiming the same order. Orders whose ``saga_log`` shows activity newer
        than the cutoff are left alone (a live drive — see the heartbeat note
        on :data:`_CLAIM_STUCK_SQL`). Returns the claimed orders with lines loaded.
        """
        ids = (await self._session.execute(_CLAIM_STUCK_SQL, {"cutoff": cutoff, "batch": batch_size})).scalars().all()
        await self._session.commit()
        claimed: list[Order] = []
        for order_id in ids:
            row = await self.get_order(order_id)
            if row is not None:
                claimed.append(row)
        return claimed
