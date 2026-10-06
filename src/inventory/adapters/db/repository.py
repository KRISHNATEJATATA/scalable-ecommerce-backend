"""Inventory repository — the oversell guard drops to a raw atomic CAS.

``try_reserve_decrement`` is the single-statement conditional decrement that
prevents overselling: it only bumps ``reserved`` when free stock covers the
qty, so a losing concurrent racer matches 0 rows and is rejected. Returns the
affected rowcount (1 = reserved, 0 = rejected).

Everything else here composes that primitive into the full reservation
lifecycle, each step **one transaction**:

* :meth:`reserve` / :meth:`reserve_many` — ``reservations`` row(s) + CAS
  decrement(s) + ``StockReserved`` outbox row(s). Either all of them land or none
  do, so the bus can never announce a hold that isn't in the table (no
  dual-write). The batch form is the checkout saga's reserve step: one
  transaction per *order*, not per cart line, with SKU-sorted lock order.
* :meth:`release` / :meth:`release_expired` — give the hold back (``reserved -=
  qty``) + ``StockReleased`` outbox row. Both are guarded on ``status = 'held'``,
  so a replayed release updates zero rows and emits no second event.
* :meth:`commit_reservation` — payment succeeded: the hold becomes a real
  deduction (``on_hand -= qty``, ``reserved -= qty``) and stops being reapable.

Every path takes the same lock order — **reservation row first, inventory row
second** — so two of them racing the same line queue behind each other instead
of deadlocking.

Returns frozen domain snapshots (:mod:`src.inventory.adapters.db.mappers`) plus
bare rowcounts/flags, never ORM rows, and every read uses ``populate_existing`` so
a snapshot always reflects the database — not an identity-map copy left over from
an earlier read on this session (the guarded raw ``UPDATE``s bypass the ORM unit
of work, so that copy goes stale).
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime
from typing import Any, cast

from sqlalchemy import ColumnElement, Select, func, select
from sqlalchemy.engine import CursorResult
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import text

from src.inventory.adapters.db.mappers import reservation_to_domain, to_domain
from src.inventory.adapters.db.models import SCHEMA, Inventory, Outbox, Reservation
from src.inventory.domain.inventory import Inventory as DomainInventory
from src.inventory.domain.reservation import Reservation as DomainReservation
from src.inventory.domain.reservation import ReservationStatus
from src.inventory.ports.repository import OutboxFactory, StockRejection
from src.shared.db.outbox import OutboxMessage
from src.shared.db.unit_of_work import UnitOfWork
from src.shared.errors.exceptions import (
    InvalidReservationError,
    ReservationConflictError,
    ReservationContendedError,
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


def _inventory_select(*where: ColumnElement[bool]) -> Select[Any]:
    """``select(Inventory)`` that refreshes rows from the DB instead of answering from the identity map."""
    return select(Inventory).where(*where).execution_options(populate_existing=True)


def _reservation_select(*where: ColumnElement[bool]) -> Select[Any]:
    """``select(Reservation)`` that refreshes rows from the DB instead of answering from the identity map."""
    return select(Reservation).where(*where).execution_options(populate_existing=True)


# `SCHEMA` is a fixed module constant, never user input, so interpolating it into
# the identifier position of these statements is safe (noqa: S608).
_DECREMENT_SQL = text(
    f"UPDATE {SCHEMA}.inventory "  # noqa: S608
    "SET reserved = reserved + :qty, version = version + 1 "
    "WHERE sku = :sku AND on_hand - reserved >= :qty"
)

# The merchant/admin stock upsert, one statement. The insert path seeds a fresh
# row (reserved = 0, version = 1); the conflict path re-points ``on_hand`` only
# when the row's current ``reserved`` still fits under the new value — the same
# guard as the ``ck_inventory_reserved_lte_on_hand`` CHECK, so a decrement
# racing this UPDATE cannot push ``reserved`` past it (the winner's write is the
# one Postgres applies). ``version`` bumps only when ``on_hand`` actually
# changes, so a same-value re-PUT lands the identical row (true idempotency).
# WHERE false → no row RETURNED → the service reads it as "held units exceed
# the requested on_hand".
_UPSERT_SQL = text(
    f"INSERT INTO {SCHEMA}.inventory (sku, on_hand, reserved, version) "  # noqa: S608
    "VALUES (:sku, :on_hand, 0, 1) "
    f"ON CONFLICT (sku) DO UPDATE SET on_hand = EXCLUDED.on_hand, "  # noqa: S608
    f"version = CASE WHEN {SCHEMA}.inventory.on_hand IS DISTINCT FROM EXCLUDED.on_hand "  # noqa: S608
    f"THEN {SCHEMA}.inventory.version + 1 ELSE {SCHEMA}.inventory.version END "  # noqa: S608
    f"WHERE {SCHEMA}.inventory.reserved <= EXCLUDED.on_hand "  # noqa: S608
    "RETURNING sku, on_hand, reserved, version"
)

# Version-guarded twin of ``_UPSERT_SQL``: the conflict path additionally
# requires ``inventory.version = :expected_version``, so a stale writer's
# UPDATE matches no row (empty RETURNING → ``None`` → the service answers 412,
# while the stock guard above still answers 409). The insert is suppressed
# unless the row already exists (``WHERE EXISTS``): a precondition names a
# version that must currently exist, so a versioned write for a missing row
# inserts nothing and likewise answers 412. One statement, so the
# existence check and the insert/conflict write share a snapshot — no
# check-then-insert race. Same-value re-PUTs keep the no-bump ``CASE``,
# so idempotency is identical in both variants.
_UPSERT_SQL_VERSION_GUARDED = text(
    f"INSERT INTO {SCHEMA}.inventory (sku, on_hand, reserved, version) "  # noqa: S608
    f"SELECT CAST(:sku AS VARCHAR(64)), :on_hand, 0, 1 WHERE EXISTS "  # noqa: S608
    f"(SELECT 1 FROM {SCHEMA}.inventory WHERE sku = :sku) "  # noqa: S608
    f"ON CONFLICT (sku) DO UPDATE SET on_hand = EXCLUDED.on_hand, "  # noqa: S608
    f"version = CASE WHEN {SCHEMA}.inventory.on_hand IS DISTINCT FROM EXCLUDED.on_hand "  # noqa: S608
    f"THEN {SCHEMA}.inventory.version + 1 ELSE {SCHEMA}.inventory.version END "  # noqa: S608
    f"WHERE {SCHEMA}.inventory.reserved <= EXCLUDED.on_hand "  # noqa: S608
    f"AND {SCHEMA}.inventory.version = :expected_version "  # noqa: S608
    "RETURNING sku, on_hand, reserved, version"
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

# One order's holds (no SKIP LOCKED: the saga recovery poller already owns the
# order via its lease on orders.orders, so no second claimant can be here).
_CLAIM_ORDER_SQL = text(
    f"SELECT id, sku, qty, order_id FROM {SCHEMA}.reservations "  # noqa: S608
    "WHERE order_id = :order_id AND status = :held FOR UPDATE"
)

_CLAIM_ORDER_COMMITTED_SQL = text(
    f"SELECT id, sku, qty, order_id FROM {SCHEMA}.reservations "  # noqa: S608
    "WHERE order_id = :order_id AND status = :committed FOR UPDATE"
)

# The inverse of ``_CONSUME_SQL``: a consumed unit goes back on the shelf.
_RESTOCK_SQL = text(
    f"UPDATE {SCHEMA}.inventory "  # noqa: S608
    "SET on_hand = on_hand + :qty, version = version + 1 "
    "WHERE sku = :sku"
)

_MARK_COMMITTED_BATCH_SQL = text(
    f"UPDATE {SCHEMA}.reservations SET status = :committed, updated_at = now() "  # noqa: S608
    "WHERE id = ANY(:ids)"
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


class _BatchRaced(Exception):
    """Internal: this attempt lost the uniqueness race — retry after re-reading."""

    def __init__(self, sku: str) -> None:
        super().__init__(sku)
        self.sku = sku


class _BatchRejection(Exception):
    """Internal: stock refused this attempt's line — unwinds the savepoint, reported as ``StockRejection``."""

    def __init__(self, sku: str, qty: int) -> None:
        super().__init__(sku, qty)
        self.sku = sku
        self.qty = qty


class InventoryRepository:
    """Implements :class:`src.inventory.ports.repository.InventoryRepositoryPort`."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        # Bound to the request's unit of work; the repo participates but never
        # commits it — the service opens the boundary, so a use-case composing
        # several writes stays one atomic unit.
        self.uow = UnitOfWork(session)

    async def get_by_sku(self, sku: str) -> DomainInventory | None:
        """The stock row for ``sku`` as a frozen snapshot, or ``None`` if the SKU has no inventory."""
        row = (await self._session.execute(_inventory_select(Inventory.sku == sku))).scalar_one_or_none()
        return to_domain(row) if row is not None else None

    async def upsert_stock(self, sku: str, on_hand: int, expected_version: int | None = None) -> DomainInventory | None:
        """Seed or re-point the stock row for ``sku``; ``None`` if live holds exceed ``on_hand``.

        One atomic statement: a fresh SKU inserts (``reserved = 0``), an
        existing row's ``on_hand`` is replaced only while its current
        ``reserved`` fits under the new value, so in-flight checkouts can never
        be erased by an upsert. The ``RETURNING`` row *is* the post-write state —
        mapped straight back, no second query. The service commits the
        enclosing unit of work — the repo only flushes.

        ``expected_version`` is an opt-in compare-and-swap predicate: when set,
        the statement inserts nothing for a missing row and the conflict path
        additionally requires the row's current ``version`` to equal it, so a
        stale writer matches no row and gets ``None`` (the service
        disambiguates 412 vs 409). When ``None`` (default) the unconditional
        statement runs unchanged.
        """
        if expected_version is None:
            row = (await self._session.execute(_UPSERT_SQL, {"sku": sku, "on_hand": on_hand})).first()
        else:
            row = (
                await self._session.execute(
                    _UPSERT_SQL_VERSION_GUARDED,
                    {"sku": sku, "on_hand": on_hand, "expected_version": expected_version},
                )
            ).first()
        if row is None:
            # The guard refused: no write to undo, and the enclosing unit of
            # work owns the transaction — just answer, never roll back here.
            return None
        await self._session.flush()
        # The RETURNING row is a plain ``Row`` (text() DML), not an ORM instance, so it
        # is built by hand — ``to_domain`` is typed to ``InventoryRow``.
        return DomainInventory(sku=row.sku, on_hand=row.on_hand, reserved=row.reserved, version=row.version)

    async def get_many_by_skus(self, skus: list[str]) -> dict[str, DomainInventory]:
        """The stock rows for ``skus`` as ``{sku: snapshot}`` (one ``WHERE sku IN`` query)."""
        if not skus:
            return {}
        rows = (await self._session.execute(_inventory_select(Inventory.sku.in_(skus)))).scalars().all()
        return {row.sku: to_domain(row) for row in rows}

    async def try_reserve_decrement(self, sku: str, qty: int) -> int:
        """The atomic conditional decrement; rowcount 1 = reserved, 0 = rejected."""
        # DML via execute() is a CursorResult at runtime; the static type is the
        # broader Result (whose ``rowcount`` the stubs hide).
        result = cast(CursorResult[Any], await self._session.execute(_DECREMENT_SQL, {"sku": sku, "qty": qty}))
        return int(result.rowcount or 0)

    # --- reservation lifecycle: state change + outbox row in ONE transaction ---

    async def reserve(
        self,
        *,
        sku: str,
        qty: int,
        order_id: uuid.UUID,
        expires_at: datetime,
        outbox: OutboxMessage,
    ) -> DomainReservation | None:
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

        Implemented as the one-line case of :meth:`reserve_many` — one copy of the
        tricky idempotency/lock-order logic, not two that can drift apart.
        """
        result = await self.reserve_many(
            lines=[(sku, qty)],
            order_id=order_id,
            expires_at=expires_at,
            outbox_factory=lambda _sku, _order_id, _qty: outbox,
        )
        if isinstance(result, StockRejection):
            return None
        return result[0]

    async def reserve_many(
        self,
        *,
        lines: list[tuple[str, int]],
        order_id: uuid.UUID,
        expires_at: datetime,
        outbox_factory: OutboxFactory,
    ) -> list[DomainReservation] | StockRejection:
        """Hold every ``(sku, qty)`` line for ``order_id`` in ONE all-or-nothing transaction.

        The checkout saga's reserve step, batched: the per-line loop it replaces
        spent one transaction — connection checkout, commit, outbox flush — per
        cart line (up to 50), stretching the step's latency, connection occupancy
        and lock exposure under concurrency. Here all reservation rows, their CAS
        decrements and their ``StockReserved`` outbox rows commit together: either
        the whole basket is held or none of it is, so a rejected line leaves
        nothing partial behind for the saga to compensate.

        Lines are worked **sorted by SKU**, and each line keeps the module-wide
        lock order (its fresh reservation row first, the inventory row second via
        the decrement). Sorting gives every batch the same global acquisition
        sequence for the inventory rows, so two overlapping batches queue instead
        of cycling — no parallelism is introduced (one session, one transaction),
        which keeps the deadlock surface at exactly this ordering guarantee.

        **All-or-nothing makes a batch replay complete or absent, never partial.**
        Like the single-line path, the INSERT is what anchors a retry: it
        serializes against the partial unique index, so it blocks on — and then
        sees the final fate of — an in-flight release of the line's previous row.
        Only a unique violation triggers the lookup pass: lines found active
        (``held``, or ``committed`` after payment consumed them) at the same
        quantity are returned as-is and their stock is NOT decremented again; a
        quantity mismatch is a caller contradiction
        (:class:`ReservationConflictError`); still-missing lines are inserted,
        decremented and given an outbox row, so a retried batch neither
        double-deducts nor double-announces. (Reading the active set *before*
        inserting would not be anchored: a stale read could wave a line through
        as "already held" while a releaser was concurrently releasing it.)

        Fresh rows are flushed one at a time, not bulk-inserted: the per-row
        IntegrityError is what attributes a rejection to *its* line — a
        foreign-key violation means that SKU has no stock row (nothing to hold →
        :class:`StockRejection`, the batch form of ``reserve``'s ``None``), a
        check violation means the request itself is invalid
        (:class:`InvalidReservationError`), and a unique violation is the
        uniqueness race, retried once after a rollback-and-reread exactly like the
        single-line path (bounded at two attempts; a second loss is real churn →
        :class:`ReservationContendedError`). Unrecognised SQLSTATEs re-raise
        rather than being mistranslated into a stock answer.

        The return is the placed rows refreshed after the flush — one primary-key
        read per line, the same cost the single-line path paid, while the batch's
        win is the single connection and transaction. Refresh by PK, not a re-read
        of the active set: a row compensation flipped to ``released`` in the race
        window still refreshes fine, where a set re-read would lose it. The
        service commits the enclosing unit of work.
        """
        if not lines:
            return []
        ordered = sorted(lines, key=lambda line: line[0])
        skus = [sku for sku, _qty in ordered]
        # Retries once, because the conflicting row can be released between our
        # failed INSERT and the lookup — then the line is genuinely free and the
        # INSERT that just failed would now succeed. Bounded at two attempts: a
        # second loss means real churn on the line, and the caller can retry.
        raced_sku = skus[0]
        for attempt in range(2):
            # First attempt inserts blind (the INSERT itself is the serialization
            # point against in-flight releases); only a unique violation earns
            # the lookup pass, which then knows every conflicting row's fate.
            existing = await self._active_map(order_id, skus) if attempt else {}
            try:
                # One savepoint per attempt: a lost race, a refused line or a
                # conflict unwinds only this attempt — never the caller's
                # surrounding unit of work, which owns the commit.
                async with self._session.begin_nested():
                    placed: list[Reservation] = []
                    for sku, qty in ordered:
                        current = existing.get(sku)
                        if current is not None:
                            held_qty = current.qty  # read BEFORE any unwind expires the row
                            if held_qty != qty:
                                # Raising unwinds the savepoint, so earlier-sorted
                                # lines of this attempt never leak into the
                                # caller's transaction as phantom holds.
                                raise ReservationConflictError(sku, held_qty, qty)
                            placed.append(current)  # earlier attempt's hold — don't deduct its stock twice
                            continue
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
                            state = _sqlstate(exc)
                            if state == _FOREIGN_KEY_VIOLATION:
                                # No inventory row for this SKU, so there is nothing to hold.
                                # Reported as insufficient stock, not a 500: to the caller an
                                # unstocked SKU and a sold-out one are the same unavailability.
                                log.info("reservation rejected: no inventory row for sku=%s", sku)
                                raise _BatchRejection(sku, qty) from exc
                            if state == _CHECK_VIOLATION:
                                raise InvalidReservationError(
                                    f"invalid reservation for {sku!r}: quantity {qty} must be positive"
                                ) from exc
                            if state != _UNIQUE_VIOLATION:
                                raise  # not ours to interpret — surface the real cause
                            raced_sku = sku
                            # A duplicate landed between our read and this INSERT;
                            # unwind the savepoint, reread and retry.
                            raise _BatchRaced(sku) from exc
                        if await self.try_reserve_decrement(sku, qty) == 0:
                            # The oversell guard refused this line: unwind the
                            # WHOLE batch so no partial holds leak into the saga's
                            # compensation.
                            raise _BatchRejection(sku, qty)
                        self._session.add(self._outbox_row(outbox_factory(sku, order_id, qty)))
                        placed.append(reservation)
            except _BatchRaced:
                continue
            except _BatchRejection as rejection:
                return StockRejection(rejection.sku, rejection.qty)
            for reservation in placed:
                await self._session.refresh(reservation)
            return [reservation_to_domain(row) for row in placed]
        # Both attempts lost the uniqueness race: real churn on this order's
        # lines. Raised, not returned as a StockRejection — a rejection means
        # *stock* refused the request (the oversell counter's meaning), and
        # contention says nothing about stock levels.
        raise ReservationContendedError(raced_sku)

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
            # No-op replay: no write to undo, and the enclosing unit of work
            # owns the transaction — just answer, never roll back here.
            return False
        await self._require_one(_UNRESERVE_SQL, {"sku": row.sku, "qty": row.qty}, what="release unreserve")
        self._session.add(self._outbox_row(outbox_factory(row.sku, row.order_id, row.qty)))
        await self._session.flush()
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
            # No-op replay: no write to undo — just answer inside the caller's transaction.
            return False
        await self._require_one(_CONSUME_SQL, {"sku": row.sku, "qty": row.qty}, what="commit consume")
        await self._session.flush()
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
            # Empty sweep: no locks taken, nothing written — the caller's
            # transaction commits (nothing) on exit.
            return 0
        for row in rows:
            await self._require_one(_UNRESERVE_SQL, {"sku": row.sku, "qty": row.qty}, what="reaper unreserve")
            self._session.add(self._outbox_row(outbox_factory(row.sku, row.order_id, row.qty)))
        await self._session.execute(
            _MARK_RELEASED_BATCH_SQL,
            {"released": ReservationStatus.RELEASED.value, "ids": [row.id for row in rows]},
        )
        await self._session.flush()
        return len(rows)

    async def release_for_order(self, order_id: uuid.UUID, outbox_factory: OutboxFactory) -> int:
        """Release every still-``held`` reservation of one order; returns how many.

        The saga's compensation path (payment failed, or the recovery poller
        settling a crashed checkout): same one-transaction shape as the reaper
        sweep, scoped to the order instead of expiry. A replay finds no ``held``
        rows and releases nothing — idempotent, emits nothing.
        """
        rows = (
            await self._session.execute(_CLAIM_ORDER_SQL, {"order_id": order_id, "held": ReservationStatus.HELD.value})
        ).all()
        if not rows:
            # No rollback on the empty replay: a zero-row claim holds no locks,
            # and the caller's transaction may already hold work (the cancel's
            # guarded flip) that must survive. The next statement's flush, or
            # the service's commit, closes the unit of work.
            return 0
        for row in rows:
            await self._require_one(_UNRESERVE_SQL, {"sku": row.sku, "qty": row.qty}, what="order release unreserve")
            self._session.add(self._outbox_row(outbox_factory(row.sku, row.order_id, row.qty)))
        await self._session.execute(
            _MARK_RELEASED_BATCH_SQL,
            {"released": ReservationStatus.RELEASED.value, "ids": [row.id for row in rows]},
        )
        await self._session.flush()
        return len(rows)

    async def restock_for_order(self, order_id: uuid.UUID, outbox_factory: OutboxFactory) -> int:
        """Reverse every ``committed`` reservation of one order; returns how many.

        The counterpart of :meth:`commit_for_order` for an order that died *after*
        its holds were consumed (a cancel won the guarded ``pending → paid`` flip
        following a full commit). ``release_for_order`` only claims ``held`` rows,
        so without this the units stay deducted on a cancelled, refunded order.
        Claim + ``on_hand += qty`` + ``committed → released`` flip + ``StockReleased``
        outbox rows are one transaction; a replay finds no ``committed`` rows and
        does nothing — idempotent, emits nothing. The caller must only invoke it
        for an order that is terminally not ``paid``: the inventory module cannot
        see order status, so that guard lives in the saga.
        """
        rows = (
            await self._session.execute(
                _CLAIM_ORDER_COMMITTED_SQL, {"order_id": order_id, "committed": ReservationStatus.COMMITTED.value}
            )
        ).all()
        if not rows:
            # Same no-rollback empty replay as release_for_order (see there).
            return 0
        for row in rows:
            await self._require_one(_RESTOCK_SQL, {"sku": row.sku, "qty": row.qty}, what="order restock")
            self._session.add(self._outbox_row(outbox_factory(row.sku, row.order_id, row.qty)))
        await self._session.execute(
            _MARK_RELEASED_BATCH_SQL,
            {"released": ReservationStatus.RELEASED.value, "ids": [row.id for row in rows]},
        )
        await self._session.flush()
        return len(rows)

    async def commit_for_order(self, order_id: uuid.UUID, *, expected: int) -> int:
        """Consume the order's ``held`` reservations, all-or-nothing; returns the order's committed total.

        The saga's success path when the payment already succeeded but the crash
        came before the per-line commits (or the recovery poller finishing a
        crashed checkout). No event: the order/payment events announce the
        outcome. A replay finds no ``held`` rows — idempotent.

        ``expected`` is the order's line count. Consumption is **all-or-nothing**:
        when the already-``committed`` rows plus the still-``held`` ones fall
        short of it (a hold was reaped or released before the payment
        confirmed), nothing is consumed. The caller reacts to that shortfall by
        refunding and compensating, and compensation only gives back ``held``
        rows — a ``committed`` row would have its stock deducted for good on an
        order that ends up cancelled. The surviving holds stay ``held`` so the
        compensation's release returns them to the pool.

        The return is **how many of the order's reservations are in ``committed``
        status after this call**, not how many rows this call itself moved. The
        distinction is what makes the answer retry-safe: a timed-out first
        attempt may have committed server-side after its caller gave up, so a
        replay's zero-rows claim would otherwise read as "nothing consumed" —
        a false shortfall the caller would compensate a fully-consumed order
        over. Counting the end-state (after the claim, so a concurrent committer's
        landed rows are visible) makes every attempt agree. A concurrent committer
        is not a hazard: the ``FOR UPDATE`` claim serializes same-order work — the
        second claimant blocks on the row locks, then re-checks the ``held``
        predicate and matches nothing, so its count sees the first's committed rows.
        """
        rows = (
            await self._session.execute(_CLAIM_ORDER_SQL, {"order_id": order_id, "held": ReservationStatus.HELD.value})
        ).all()
        # Read after the claim, in the same transaction: READ COMMITTED gives a
        # fresh snapshot per statement, so another committer's landed rows count.
        already_committed = int(
            (
                await self._session.execute(
                    select(func.count())
                    .select_from(Reservation)
                    .where(Reservation.order_id == order_id, Reservation.status == ReservationStatus.COMMITTED.value)
                )
            ).scalar_one()
        )
        if not rows:
            return already_committed
        if already_committed + len(rows) < expected:
            # Nothing was written: the service's commit on exit only drops the
            # claim's row locks. Never roll back here — the caller's loaded
            # rows would expire (the release path avoids it for the same reason).
            return already_committed
        for row in rows:
            await self._require_one(_CONSUME_SQL, {"sku": row.sku, "qty": row.qty}, what="order commit consume")
        await self._session.execute(
            _MARK_COMMITTED_BATCH_SQL,
            {"committed": ReservationStatus.COMMITTED.value, "ids": [row.id for row in rows]},
        )
        await self._session.flush()
        return already_committed + len(rows)

    async def _active_map(self, order_id: uuid.UUID, skus: list[str]) -> dict[str, Reservation]:
        """This order's existing non-released reservations for ``skus``, as ``{sku: ORM row}``.

        ORM-internal — this map never crosses the port (``reserve_many`` consumes it
        and maps the rows it hands back), so the frozen-snapshot contract holds.

        A row present here — ``held`` (in flight) or ``committed`` (payment already
        consumed it) — is a legitimate retry answer; returning the committed one is
        what stops a late retry from re-reserving and deducting the stock twice. A
        SKU absent from the map is genuinely free: compensation or the reaper may
        have released it since any earlier attempt.
        """
        stmt = _reservation_select(
            Reservation.order_id == order_id,
            Reservation.sku.in_(skus),
            Reservation.status.in_(_ACTIVE_STATUSES),
        )
        rows = (await self._session.execute(stmt)).scalars().all()
        return {row.sku: row for row in rows}

    async def _require_one(self, sql, params: dict, *, what: str) -> None:
        """Execute a stock mutation that must affect exactly one row, or fail loudly.

        ``_UNRESERVE_SQL``/``_CONSUME_SQL`` are guarded (``reserved >= :qty`` etc.),
        so 0 rows means the guard refused: the counters disagree with the
        reservation we just transitioned. Silently continuing would commit the
        status flip and the ``StockReleased`` event while the stock never moved —
        permanently losing those units. Raise and let the enclosing unit of work
        roll back instead.
        """
        result = cast(CursorResult[Any], await self._session.execute(sql, params))
        rowcount = int(result.rowcount or 0)
        if rowcount != 1:
            raise StockMutationError(f"{what} affected {rowcount} rows, expected 1: {params}")

    @staticmethod
    def _outbox_row(outbox: OutboxMessage) -> Outbox:
        return Outbox(event_type=outbox.event_type, payload=outbox.payload)
