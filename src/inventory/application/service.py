"""Inventory use-cases — the oversell-defense entry point.

Called in-process by the checkout saga (there is no inventory HTTP route: no
external caller reserves stock directly) and by the `service`-role reaper. Every
caller goes Service → Repository; nothing outside this layer touches the
repository. Each method is a thin policy shell over one atomic repository
transaction:

* :meth:`reserve` — stamps the TTL and raises :class:`InsufficientStockError`
  (→ RFC 9457 409) when the atomic decrement rejects *and* the order has no live
  hold for that line, bumping the oversell-blocked counter. That rejection is the
  invariant working, not an error to paper over.
* :meth:`release` — saga compensation; idempotent.
* :meth:`commit_reservation` — payment succeeded, the hold becomes a deduction.
* :meth:`release_expired` — the reaper's sweep.

ORM rows never leave here: everything returns a Pydantic ``*Response``.
"""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime, timedelta

from src.inventory.application.dto import InventoryResponse, ReservationResponse
from src.inventory.application.mappers import reservation_to_domain, to_domain
from src.inventory.application.metrics import (
    oversell_blocked_total,
    reaper_released_total,
    reservation_conflict_total,
)
from src.inventory.application.outbox import stock_released_outbox, stock_reserved_outbox
from src.inventory.ports.repository import InventoryRepositoryPort
from src.shared.errors.exceptions import (
    InsufficientStockError,
    InvalidReservationError,
    ReservationConflictError,
)

log = logging.getLogger(__name__)


class InventoryService:
    """Read + reservation use-cases over the inventory stock model."""

    def __init__(self, repo: InventoryRepositoryPort, *, reservation_ttl_seconds: int) -> None:
        self._repo = repo
        self._ttl = timedelta(seconds=reservation_ttl_seconds)

    async def get_by_sku(self, sku: str) -> InventoryResponse | None:
        """Resolve a stock row by SKU, or ``None`` if absent."""
        row = await self._repo.get_by_sku(sku)
        if row is None:
            return None
        return InventoryResponse.model_validate(to_domain(row))

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
        return ReservationResponse.model_validate(reservation_to_domain(row))

    async def release(self, reservation_id: uuid.UUID) -> bool:
        """Give a held reservation's stock back (saga compensation); ``False`` on replay."""
        return await self._repo.release(reservation_id, stock_released_outbox)

    async def commit_reservation(self, reservation_id: uuid.UUID) -> bool:
        """Consume a held reservation on payment success; ``False`` on replay."""
        return await self._repo.commit_reservation(reservation_id)

    async def release_expired(self, batch_size: int) -> int:
        """Release every hold past its TTL; returns how many (the reaper's use-case).

        Owns the reaper's observable outcome — counter and log line — so the worker
        stays a transport/scheduling shell. The counter reaches Prometheus through
        the worker exporter (`src/shared/observability/worker_metrics.py`); reaper
        *liveness* is still the expired-hold backlog alert (`docs/RUNBOOK.md` §8),
        since a dead worker reports nothing at all.
        """
        released = await self._repo.release_expired(batch_size=batch_size, outbox_factory=stock_released_outbox)
        if released:
            reaper_released_total.inc(released)
            log.info("released %d expired reservation(s)", released)
        return released
