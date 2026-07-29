"""Inventory repository — the oversell guard drops to a raw atomic CAS.

``try_reserve_decrement`` is the single-statement conditional decrement that
prevents overselling: it only bumps ``reserved`` when free stock covers the
qty, so a losing concurrent racer matches 0 rows and is rejected. Returns the
affected rowcount (1 = reserved, 0 = rejected).

Everything else here composes that primitive into the full reservation
lifecycle, each step **one transaction**:

* :meth:`reserve` — ``reservations`` row + CAS decrement + ``StockReserved``
  outbox row. Either all three land or none do, so the bus can never announce a
  hold that isn't in the table (no dual-write).
* :meth:`release` / :meth:`release_expired` — give the hold back (``reserved -=
  qty``) + ``StockReleased`` outbox row. Both are guarded on ``status = 'held'``,
  so a replayed release updates zero rows and emits no second event.
* :meth:`commit_reservation` — payment succeeded: the hold becomes a real
  deduction (``on_hand -= qty``, ``reserved -= qty``) and stops being reapable.

Every path takes the same lock order — **reservation row first, inventory row
second** — so two of them racing the same line queue behind each other instead
of deadlocking.

Returns ORM rows / rowcounts, not domain entities — added when inventory domain
behavior beyond the status machine arrives.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import text

from src.inventory.adapters.db.models import SCHEMA, Inventory, Outbox, Reservation
from src.inventory.domain.reservation import ReservationStatus
from src.inventory.ports.repository import OutboxFactory
from src.shared.db.outbox import OutboxMessage
from src.shared.errors.exceptions import (
    InvalidReservationError,
    ReservationConflictError,
    StockMutationError,
)

log = logging.getLogger(__name__)

#: Statuses that still own stock, so a second reservation for the line is a duplicate.
#: ``released`` is absent: that line legitimately may be reserved again.
_ACTIVE_STATUSES = (ReservationStatus.HELD.value, ReservationStatus.COMMITTED.value)

# SQLSTATEs, not constraint names: names drift with migrations, these are standard.
_UNIQUE_VIOLATION = "23505"
_FOREIGN_KEY_VIOLATION = "23503"
_CHECK_VIOLATION = "23514"


def _sqlstate(exc: IntegrityError) -> str | None:
    """The SQLSTATE behind a SQLAlchemy ``IntegrityError``, or ``None`` if unavailable.

    The driver error sits at ``.orig``, but the asyncpg dialect wraps its exception
    one level deeper, so the real code may be on ``.orig.__cause__``. ``pgcode`` is
    checked too so this keeps working under a psycopg-based driver (Alembic's).
    """
    for candidate in (exc.orig, getattr(exc.orig, "__cause__", None)):
        state = getattr(candidate, "sqlstate", None) or getattr(candidate, "pgcode", None)
        if state:
            return str(state)
    return None


# `SCHEMA` is a fixed module constant, never user input, so interpolating it into
# the identifier position of these statements is safe (noqa: S608).
_DECREMENT_SQL = text(
    f"UPDATE {SCHEMA}.inventory "  # noqa: S608
    "SET reserved = reserved + :qty, version = version + 1 "
    "WHERE sku = :sku AND on_hand - reserved >= :qty"
)

# The status transition goes first: it is the idempotency gate. RETURNING hands
# back the (sku, qty, order_id) to undo, so no second SELECT is needed.
_MARK_RELEASED_SQL = text(
    f"UPDATE {SCHEMA}.reservations SET status = :released, updated_at = now() "  # noqa: S608
    "WHERE id = :id AND status = :held RETURNING sku, qty, order_id"
)

_MARK_COMMITTED_SQL = text(
    f"UPDATE {SCHEMA}.reservations SET status = :committed, updated_at = now() "  # noqa: S608
    "WHERE id = :id AND status = :held RETURNING sku, qty"
)

# `reserved >= :qty` is belt-and-braces with the CHECK constraint: a release can
# never drive `reserved` negative, it would just match no rows.
_UNRESERVE_SQL = text(
    f"UPDATE {SCHEMA}.inventory "  # noqa: S608
    "SET reserved = reserved - :qty, version = version + 1 "
    "WHERE sku = :sku AND reserved >= :qty"
)

_CONSUME_SQL = text(
    f"UPDATE {SCHEMA}.inventory "  # noqa: S608
    "SET on_hand = on_hand - :qty, reserved = reserved - :qty, version = version + 1 "
    "WHERE sku = :sku AND reserved >= :qty AND on_hand >= :qty"
)

# FOR UPDATE SKIP LOCKED so N reaper replicas never claim the same expired hold.
_CLAIM_EXPIRED_SQL = text(
    f"SELECT id, sku, qty, order_id FROM {SCHEMA}.reservations "  # noqa: S608
    "WHERE status = :held AND expires_at <= now() "
    "ORDER BY expires_at FOR UPDATE SKIP LOCKED LIMIT :batch"
)

_MARK_RELEASED_BATCH_SQL = text(
    f"UPDATE {SCHEMA}.reservations SET status = :released, updated_at = now() "  # noqa: S608
    "WHERE id = ANY(:ids)"
)

#: The ``OutboxFactory`` contract lives in ``ports`` and is imported above.


class InventoryRepository:
    """Implements :class:`src.inventory.ports.repository.InventoryRepositoryPort`."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_by_sku(self, sku: str) -> Inventory | None:
        """The stock row for ``sku``, or ``None`` if the SKU has no inventory."""
        stmt = select(Inventory).where(Inventory.sku == sku)
        return (await self._session.execute(stmt)).scalar_one_or_none()

    async def try_reserve_decrement(self, sku: str, qty: int) -> int:
        """The atomic conditional decrement; rowcount 1 = reserved, 0 = rejected."""
        result = await self._session.execute(_DECREMENT_SQL, {"sku": sku, "qty": qty})
        return result.rowcount

    # --- reservation lifecycle: state change + outbox row in ONE transaction ---

    async def reserve(
        self,
        *,
        sku: str,
        qty: int,
        order_id: uuid.UUID,
        expires_at: datetime,
        outbox: OutboxMessage,
    ) -> Reservation | None:
        """Hold ``qty`` of ``sku`` for ``order_id``; ``None`` when stock doesn't cover it.

        The ``held`` reservation row, the conditional decrement and the
        ``StockReserved`` outbox row commit together. N concurrent callers racing
        the last unit therefore produce exactly one reservation: the losers match
        0 rows on the decrement and their INSERT rolls back with it.

        **Order matters: the reservation is flushed _before_ the decrement.** Every
        other path here (:meth:`release`, :meth:`commit_reservation`,
        :meth:`release_expired`) locks the reservation row first and the inventory
        row second, so reserving in the opposite order would let a retry racing a
        release deadlock on the pair. One lock order everywhere, no cycle.

        Flushing first also makes the rejection unambiguous. Our own hold is not
        committed yet and so contributes nothing to ``reserved``: a retry of a line
        that already holds stock trips ``uq_reservations_active_order_sku`` on the
        INSERT and returns the existing reservation (idempotent at any stock
        level), which leaves ``rowcount = 0`` on the decrement meaning exactly one
        thing — genuinely insufficient stock.

        The uniqueness guard spans ``held`` **and** ``committed``, so a retry that
        arrives after payment already consumed the hold is still deduplicated
        rather than deducting the stock a second time. A retry for a *different*
        quantity is a caller contradiction, not stock pressure, and raises
        :class:`ReservationConflictError`.

        A failed INSERT is **not** assumed to be that duplicate: the SQLSTATE
        decides. A foreign-key violation means the SKU has no stock row at all
        (nothing to hold → ``None``), a check violation means the request itself is
        invalid (:class:`InvalidReservationError`), and anything unrecognised is
        re-raised rather than mistranslated into a stock answer.
        """
        # Retries once, because the conflicting row can be released between our
        # failed INSERT and the lookup — then the line is genuinely free and the
        # INSERT that just failed would now succeed. Bounded at two attempts: a
        # second loss means real churn on the line, and the caller can retry.
        for _ in range(2):
            reservation = Reservation(
                sku=sku,
                qty=qty,
                order_id=order_id,
                expires_at=expires_at,
                status=ReservationStatus.HELD.value,
            )
            self._session.add(reservation)
            try:
                await self._session.flush()
            except IntegrityError as exc:
                await self._session.rollback()
                state = _sqlstate(exc)
                if state == _FOREIGN_KEY_VIOLATION:
                    # No inventory row for this SKU, so there is nothing to hold.
                    # Reported as insufficient stock, not a 500: to the caller an
                    # unstocked SKU and a sold-out one are the same unavailability.
                    log.info("reservation rejected: no inventory row for sku=%s", sku)
                    return None
                if state == _CHECK_VIOLATION:
                    raise InvalidReservationError(
                        f"invalid reservation for {sku!r}: quantity {qty} must be positive"
                    ) from exc
                if state != _UNIQUE_VIOLATION:
                    raise  # not ours to interpret — surface the real cause
                existing = await self._find_active(order_id, sku, qty)
                if existing is not None:
                    return existing
                continue  # the conflicting reservation was released mid-flight; retry
            if await self.try_reserve_decrement(sku, qty) == 0:
                await self._session.rollback()
                return None
            self._session.add(self._outbox_row(outbox))
            await self._session.commit()
            await self._session.refresh(reservation)
            return reservation
        return None

    async def release(self, reservation_id: uuid.UUID, outbox_factory: OutboxFactory) -> bool:
        """Return a held reservation's stock; ``False`` if it wasn't ``held`` (no-op replay).

        Used by saga compensation. The ``StockReleased`` outbox row is written only
        when the status transition actually lands, so a duplicated compensation
        emits exactly one event.
        """
        row = (
            await self._session.execute(
                _MARK_RELEASED_SQL,
                {
                    "id": reservation_id,
                    "held": ReservationStatus.HELD.value,
                    "released": ReservationStatus.RELEASED.value,
                },
            )
        ).first()
        if row is None:
            await self._session.rollback()
            return False
        await self._require_one(_UNRESERVE_SQL, {"sku": row.sku, "qty": row.qty}, what="release unreserve")
        self._session.add(self._outbox_row(outbox_factory(row.sku, row.order_id, row.qty)))
        await self._session.commit()
        return True

    async def commit_reservation(self, reservation_id: uuid.UUID) -> bool:
        """Turn a hold into a real deduction on payment success; ``False`` on replay.

        Without this the reaper would eventually release a *paid* order's stock back
        into the pool. No event is emitted: there is no ``StockCommitted`` contract
        and the order/payment events already announce the outcome.
        """
        row = (
            await self._session.execute(
                _MARK_COMMITTED_SQL,
                {
                    "id": reservation_id,
                    "held": ReservationStatus.HELD.value,
                    "committed": ReservationStatus.COMMITTED.value,
                },
            )
        ).first()
        if row is None:
            await self._session.rollback()
            return False
        await self._require_one(_CONSUME_SQL, {"sku": row.sku, "qty": row.qty}, what="commit consume")
        await self._session.commit()
        return True

    async def release_expired(self, *, batch_size: int, outbox_factory: OutboxFactory) -> int:
        """Reaper pass: release every hold past ``expires_at``; returns how many.

        Claim + stock give-back + status flip + outbox rows are one transaction, and
        the claim takes ``FOR UPDATE SKIP LOCKED``, so concurrent reaper replicas
        split the batch instead of double-releasing a row. (The session's implicit
        transaction is the unit — same as every other write here — so the row locks
        are held until the commit at the end.)
        """
        rows = (
            await self._session.execute(_CLAIM_EXPIRED_SQL, {"held": ReservationStatus.HELD.value, "batch": batch_size})
        ).all()
        if not rows:
            await self._session.rollback()
            return 0
        for row in rows:
            await self._require_one(_UNRESERVE_SQL, {"sku": row.sku, "qty": row.qty}, what="reaper unreserve")
            self._session.add(self._outbox_row(outbox_factory(row.sku, row.order_id, row.qty)))
        await self._session.execute(
            _MARK_RELEASED_BATCH_SQL,
            {"released": ReservationStatus.RELEASED.value, "ids": [row.id for row in rows]},
        )
        await self._session.commit()
        return len(rows)

    async def _find_active(self, order_id: uuid.UUID, sku: str, qty: int) -> Reservation | None:
        """This order line's existing non-released reservation, or ``None`` if it's gone.

        Normally reached after the uniqueness guard rejected our INSERT, so a row
        is there — ``held`` (in flight) or ``committed`` (payment already consumed
        it). Both are legitimate retry answers; returning the committed one is what
        stops a late retry from re-reserving and deducting the stock twice.

        ``None`` is a real outcome, not an impossibility: compensation or the
        reaper can release the conflicting row between the failed INSERT and this
        lookup, which leaves the line free. The caller retries rather than crashing
        on a missing row.

        A quantity mismatch is *not* an out-of-stock condition — the caller changed
        its mind about a line it already holds — so it raises
        :class:`ReservationConflictError` rather than being reported as an oversell
        block. The caller must release the stale hold first.
        """
        stmt = select(Reservation).where(
            Reservation.order_id == order_id,
            Reservation.sku == sku,
            Reservation.status.in_(_ACTIVE_STATUSES),
        )
        existing = (await self._session.execute(stmt)).scalar_one_or_none()
        if existing is not None and existing.qty != qty:
            raise ReservationConflictError(sku, existing.qty, qty)
        return existing

    async def _require_one(self, sql, params: dict, *, what: str) -> None:
        """Execute a stock mutation that must affect exactly one row, or fail loudly.

        ``_UNRESERVE_SQL``/``_CONSUME_SQL`` are guarded (``reserved >= :qty`` etc.),
        so 0 rows means the guard refused: the counters disagree with the
        reservation we just transitioned. Silently continuing would commit the
        status flip and the ``StockReleased`` event while the stock never moved —
        permanently losing those units. Roll back and surface it instead.
        """
        result = await self._session.execute(sql, params)
        if result.rowcount != 1:
            await self._session.rollback()
            raise StockMutationError(f"{what} affected {result.rowcount} rows, expected 1: {params}")

    @staticmethod
    def _outbox_row(outbox: OutboxMessage) -> Outbox:
        return Outbox(event_type=outbox.event_type, payload=outbox.payload)
