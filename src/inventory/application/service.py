"""Inventory use-cases — the oversell-defense entry point.

Called in-process by the checkout saga (no external caller reserves stock
directly; the only HTTP surface is the merchant/admin stock upsert) and by the
`service`-role reaper. Every caller goes Service → Repository; nothing outside
this layer touches the repository. Each method is a thin policy shell over one
atomic repository transaction:

* :meth:`reserve` — stamps the TTL and raises :class:`InsufficientStockError`
  (→ RFC 9457 409) when the atomic decrement rejects *and* the order has no live
  hold for that line, bumping the oversell-blocked counter. That rejection is the
  invariant working, not an error to paper over.
* :meth:`release` — saga compensation; idempotent.
* :meth:`commit_reservation` — payment succeeded, the hold becomes a deduction.
* :meth:`release_expired` — the reaper's sweep.

The repository hands back frozen domain snapshots (never live ORM rows), and
everything here returns a Pydantic ``*Response``.
"""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime, timedelta

from src.inventory.application.dto import InventoryResponse, ReservationResponse
from src.inventory.application.metrics import (
    oversell_blocked_total,
    reaper_released_total,
    reservation_conflict_total,
)
from src.inventory.application.outbox import stock_released_outbox, stock_reserved_outbox
from src.inventory.ports.ownership import StockOwnershipPort
from src.inventory.ports.repository import InventoryRepositoryPort, StockRejection
from src.shared.errors.exceptions import (
    AuthorizationError,
    InsufficientStockError,
    InvalidReservationError,
    PreconditionFailedError,
    ReservationConflictError,
    StockBelowReservedError,
)

log = logging.getLogger(__name__)


class InventoryService:
    """Read + reservation use-cases over the inventory stock model."""

    def __init__(
        self,
        repo: InventoryRepositoryPort,
        *,
        reservation_ttl_seconds: int,
        ownership: StockOwnershipPort | None = None,
    ) -> None:
        self._repo = repo
        self._ttl = timedelta(seconds=reservation_ttl_seconds)
        # Wired only on the HTTP path (the container); direct constructions —
        # seed script, reaper, saga recovery — are operator/internal paths that
        # never serve an external caller, so they skip the ownership gate.
        self._ownership = ownership

    async def get_by_sku(self, sku: str) -> InventoryResponse | None:
        """Resolve a stock row by SKU, or ``None`` if absent."""
        row = await self._repo.get_by_sku(sku)
        if row is None:
            return None
        return InventoryResponse.model_validate(row)

    async def get_many_by_skus(self, skus: list[str]) -> dict[str, InventoryResponse]:
        """Resolve stock rows for ``skus`` as ``{sku: response}`` (one query).

        SKUs with no row are absent from the map — the caller (a read
        projection) reports those as unknown, never as zero.
        """
        rows = await self._repo.get_many_by_skus(skus)
        return {sku: InventoryResponse.model_validate(snapshot) for sku, snapshot in rows.items()}

    async def upsert_stock(
        self,
        sku: str,
        on_hand: int,
        *,
        caller_id: uuid.UUID | None = None,
        is_admin: bool = False,
        if_match: int | None = None,
    ) -> InventoryResponse | None:
        """Seed or re-point a SKU's ``on_hand`` (the merchant/admin stock upsert).

        Idempotent per value: re-PUT with the same ``on_hand`` lands the same
        state. Raises :class:`StockBelowReservedError` when the row's live
        holds would exceed the new ``on_hand`` — reserved units belong to
        checkouts in flight and may not be erased. No event: nothing consumes
        stock *levels* (``StockReserved``/``StockReleased`` announce lifecycle
        transitions), and the catalog composes ``available`` fresh on every
        read, so there is nothing to invalidate.

        Ownership gate (only when the port is wired — always on the HTTP path):
        the SKU must resolve to a live catalog product, and a merchant may only
        re-point their *own* product's stock — ``admin`` bypasses ownership,
        never the route's role gate. An unresolvable SKU (unknown, soft-deleted,
        or not a product id) returns ``None`` (route → 404); a cross-merchant
        write raises :class:`AuthorizationError` (403) before any state moves.
        """
        if self._ownership is not None:
            owner_id = await self._ownership.merchant_id_for_sku(sku)
            if owner_id is None:
                return None
            if owner_id != caller_id and not is_admin:
                raise AuthorizationError("not the owner of this product")
        # Service-owned boundary: the upsert's write commits here, not in the repo.
        async with self._repo.uow.transaction():
            row = await self._repo.upsert_stock(sku, on_hand, expected_version=if_match)
            if row is None:
                # A guard refused: the version predicate (stale → 412,
                # including a versioned write for a row that does not exist)
                # or the reserved<=on_hand stock guard (→ 409). Re-read to
                # tell them apart — 412 beats 409 when both could apply.
                current = await self._repo.get_by_sku(sku)
                if if_match is not None and (current is None or current.version != if_match):
                    raise PreconditionFailedError(
                        f"the stock for {sku!r} changed since it was read (If-Match mismatch); re-read it and re-apply"
                    )
                raise StockBelowReservedError(sku, on_hand, current.reserved if current else 0)
            return InventoryResponse.model_validate(row)

    async def reserve(self, sku: str, qty: int, order_id: uuid.UUID) -> ReservationResponse:
        """Hold ``qty`` of ``sku`` for ``order_id`` until the TTL expires.

        Idempotent per order line: a retry returns the existing reservation rather
        than placing a second one — including after the line was *committed*, which
        is what stops a late retry from deducting the stock twice. Raises
        :class:`InsufficientStockError` when free stock doesn't cover a *new*
        request (including the losing side of a race for the last unit, exactly the
        oversell the atomic decrement exists to block), and
        :class:`ReservationConflictError` when the line already holds a different
        quantity — a caller contradiction, counted separately so it can't inflate
        the oversell signal. A non-positive ``qty`` is rejected up front as
        :class:`InvalidReservationError` (400): no stock level makes it valid, and
        it would otherwise reach the DB only to trip ``ck_reservations_qty_positive``.
        """
        if qty <= 0:
            raise InvalidReservationError(f"invalid reservation for {sku!r}: quantity {qty} must be positive")
        try:
            # Service-owned boundary: the hold + decrement + outbox row commit
            # here. The repository only flushes, so a caller composing several
            # writes (the cancel path) stays one atomic unit.
            async with self._repo.uow.transaction():
                row = await self._repo.reserve(
                    sku=sku,
                    qty=qty,
                    order_id=order_id,
                    expires_at=datetime.now(UTC) + self._ttl,
                    outbox=stock_reserved_outbox(sku, order_id, qty),
                )
        except ReservationConflictError:
            reservation_conflict_total.inc()
            log.info("reservation conflict: order line for sku=%s re-reserved with qty=%s", sku, qty)
            raise
        if row is None:
            oversell_blocked_total.inc()
            log.info("reservation rejected: insufficient stock for sku=%s qty=%s", sku, qty)
            raise InsufficientStockError(sku, qty)
        return ReservationResponse.model_validate(row)

    async def reserve_many(self, lines: list[tuple[str, int]], order_id: uuid.UUID) -> list[ReservationResponse]:
        """Hold every ``(sku, qty)`` line for ``order_id`` in one all-or-nothing transaction.

        The checkout saga's reserve step: the batch form of :meth:`reserve`, so a
        cart-full of lines costs one transaction instead of one per line. Same
        contract per line — idempotent retry (an already-held line is returned,
        not re-deducted), :class:`InsufficientStockError` (409) naming the first
        line whose stock fell short (or has no stock row) with the whole batch
        rolled back, :class:`ReservationConflictError` when a line is re-reserved
        at a different quantity (counted apart from the oversell signal), and
        :class:`InvalidReservationError` (400) for a non-positive quantity, which
        no stock level would make valid. SKUs must be unique across ``lines``
        (the cart keys lines by product, so they are): a duplicate is a caller
        contradiction no reservation state can satisfy twice, rejected as invalid.
        """
        if not lines:
            return []
        seen: set[str] = set()
        for sku, qty in lines:
            if qty <= 0:
                raise InvalidReservationError(f"invalid reservation for {sku!r}: quantity {qty} must be positive")
            if sku in seen:
                raise InvalidReservationError(f"invalid reservation batch: sku {sku!r} appears more than once")
            seen.add(sku)
        try:
            # Service-owned boundary: the whole two-attempt loop runs inside
            # this unit of work — a direct repository caller no longer exists,
            # and a composing caller (cancel, saga) nests into it as a savepoint.
            async with self._repo.uow.transaction():
                rows = await self._repo.reserve_many(
                    lines=lines,
                    order_id=order_id,
                    expires_at=datetime.now(UTC) + self._ttl,
                    outbox_factory=stock_reserved_outbox,
                )
        except ReservationConflictError:
            reservation_conflict_total.inc()
            log.info("reservation conflict: order line re-reserved at a different quantity (order=%s)", order_id)
            raise
        if isinstance(rows, StockRejection):
            oversell_blocked_total.inc()
            log.info("batch reservation rejected: insufficient stock for sku=%s qty=%s", rows.sku, rows.qty)
            raise InsufficientStockError(rows.sku, rows.qty)
        return [ReservationResponse.model_validate(snapshot) for snapshot in rows]

    async def release(self, reservation_id: uuid.UUID) -> bool:
        """Give a held reservation's stock back (saga compensation); ``False`` on replay."""
        async with self._repo.uow.transaction():
            return await self._repo.release(reservation_id, stock_released_outbox)

    async def release_for_order(self, order_id: uuid.UUID) -> int:
        """Release every still-``held`` reservation of one order (saga compensation).

        Returns how many were released. Owns no counter: compensation volume is
        visible through the saga's own compensation-rate signal, and a replay
        releasing nothing is the normal (not the alertable) case.
        """
        async with self._repo.uow.transaction():
            return await self._repo.release_for_order(order_id, stock_released_outbox)

    async def restock_for_order(self, order_id: uuid.UUID) -> int:
        """Give back the stock of an order's ``committed`` reservations (saga compensation).

        For an order that was cancelled after its holds were consumed — the
        ``held``-only :meth:`release_for_order` cannot reach them. Returns how
        many were reversed; a replay reverses nothing. Must only be called for
        an order that is terminally not ``paid``.
        """
        async with self._repo.uow.transaction():
            return await self._repo.restock_for_order(order_id, stock_released_outbox)

    async def commit_for_order(self, order_id: uuid.UUID, *, expected: int) -> int:
        """Consume the order's still-``held`` reservations, all-or-nothing (saga success).

        The recovery poller's finish for a checkout whose payment succeeded but
        whose per-line commits never ran. ``expected`` is the order's line count;
        a shortfall consumes nothing so the caller's compensation can release the
        survivors. Returns how many of the order's reservations are in
        ``committed`` status after the call — the end-state count (not the
        per-call rowcount) so a replay of an already-committed order reports its
        lines instead of a false shortfall.
        """
        async with self._repo.uow.transaction():
            return await self._repo.commit_for_order(order_id, expected=expected)

    async def commit_reservation(self, reservation_id: uuid.UUID) -> bool:
        """Consume a held reservation on payment success; ``False`` on replay."""
        async with self._repo.uow.transaction():
            return await self._repo.commit_reservation(reservation_id)

    async def release_expired(self, batch_size: int) -> int:
        """Release every hold past its TTL; returns how many (the reaper's use-case).

        Owns the reaper's observable outcome — counter and log line — so the worker
        stays a transport/scheduling shell. The counter reaches Prometheus through
        the worker exporter (`src/shared/observability/worker_metrics.py`); reaper
        *liveness* is still the expired-hold backlog alert (`docs/RUNBOOK.md` §8),
        since a dead worker reports nothing at all.
        """
        async with self._repo.uow.transaction():
            released = await self._repo.release_expired(batch_size=batch_size, outbox_factory=stock_released_outbox)
        if released:
            reaper_released_total.inc(released)
            log.info("released %d expired reservation(s)", released)
        return released
