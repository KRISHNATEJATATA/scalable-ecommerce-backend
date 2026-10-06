"""Port (Protocol) for the inventory repository.

Implemented by ``adapters/db/repository.InventoryRepository``. Covers the raw
conditional-decrement CAS primitive plus the reservation lifecycle composed on
top of it (hold + TTL, release, commit, expiry sweep) — each a single
transaction that carries its own outbox row.

Every read returns a frozen domain :class:`Inventory`/:class:`Reservation`
snapshot of the committed row, never a live ORM instance — the adapter maps before
returning, so callers hold no session state and need no knowledge of SQLAlchemy's
identity map or expiry rules.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from src.inventory.domain.inventory import Inventory
from src.inventory.domain.reservation import Reservation
from src.shared.db.outbox import OutboxMessage
from src.shared.db.unit_of_work import UnitOfWorkPort

#: Builds the outbox message for one released hold, from ``(sku, order_id, qty)``.
#: Defined here (the contract), imported by the adapter — never redeclared.
#: ``stock_released_outbox`` satisfies it directly.
OutboxFactory = Callable[[str, uuid.UUID, int], OutboxMessage]


@dataclass(frozen=True, slots=True)
class StockRejection:
    """The batch reserve's stock-refusal answer: which line's free stock fell short.

    Returned, not raised, so it stays distinguishable from a caller contradiction
    (:class:`ReservationConflictError`) and line churn
    (:class:`ReservationContendedError`) — the service maps it to
    :class:`InsufficientStockError` and the oversell counter, exactly like the
    single-line ``reserve`` returning ``None``.
    """

    sku: str
    qty: int


class InventoryRepositoryPort(Protocol):
    @property
    def uow(self) -> UnitOfWorkPort:
        """The unit of work this repository is bound to. Repositories
        participate in it but never commit it; the application service opens the
        boundary with ``async with repo.uow.transaction():``."""
        ...

    async def get_by_sku(self, sku: str) -> Inventory | None:
        """The stock row for ``sku`` as a frozen snapshot, or ``None`` if the SKU has no inventory."""
        ...

    async def upsert_stock(self, sku: str, on_hand: int, expected_version: int | None = None) -> Inventory | None:
        """Create the stock row for ``sku`` (or re-point an existing one's ``on_hand``).

        Idempotent: re-PUT with the same value lands the same state. ``None``
        when the row exists and its ``reserved`` exceeds the requested
        ``on_hand`` — live holds may not be erased, the caller must raise
        ``on_hand`` or wait for the holds to release. ``None`` also when a
        versioned write names a row that does not exist (stale precondition).

        ``expected_version`` is an opt-in compare-and-swap predicate: when set,
        the statement inserts nothing for a missing row and the conflict path
        additionally requires the row's current ``version`` to equal it, so a
        stale writer matches no row and gets ``None`` (the caller
        disambiguates 412 vs 409).
        """
        ...

    async def get_many_by_skus(self, skus: list[str]) -> dict[str, Inventory]:
        """The stock rows for ``skus`` as ``{sku: snapshot}``, in one query.

        SKUs with no row are simply absent from the map (unknown, not zero).
        Exists so a product listing attaches availability with one stock query
        instead of one per item; the single-row :meth:`get_by_sku` stays for its
        current callers.
        """
        ...

    async def try_reserve_decrement(self, sku: str, qty: int) -> int:
        """The raw CAS primitive: bump ``reserved`` only if free stock covers ``qty``.

        Returns the affected rowcount — 1 = reserved, 0 = rejected (the oversell
        guard firing). Composed by :meth:`reserve`; callers outside this module use
        the reservation lifecycle instead.
        """
        ...

    async def reserve(
        self,
        *,
        sku: str,
        qty: int,
        order_id: uuid.UUID,
        expires_at: datetime,
        outbox: OutboxMessage,
    ) -> Reservation | None:
        """``None`` = insufficient stock (the oversell guard's answer, counted as such).

        Raises ``ReservationConflictError`` when the order line already holds a
        different quantity (a caller contradiction), and
        :class:`ReservationContendedError` when repeated uniqueness races mean the
        line is under churn — transient pressure that must not be reported, or
        counted, as a stock rejection."""
        ...

    async def reserve_many(
        self,
        *,
        lines: list[tuple[str, int]],
        order_id: uuid.UUID,
        expires_at: datetime,
        outbox_factory: OutboxFactory,
    ) -> list[Reservation] | StockRejection:
        """Hold every ``(sku, qty)`` line for ``order_id`` in ONE all-or-nothing transaction.

        The checkout saga's reserve step: up to a cart-full of lines placed as one
        batch instead of N sequential transactions — one connection checkout, one
        commit, and no partial holds to compensate when a line is rejected (the
        whole batch rolls back). Lines are worked in SKU-sorted order so every
        batch takes the inventory row locks in the same global sequence — the
        deterministic lock order that keeps two overlapping batches from
        deadlocking (no parallelism is introduced, so there is no new deadlock
        surface beyond ordering). ``lines`` must have unique SKUs.

        Returns the order's active reservation rows for the lines (freshly placed
        plus any an earlier attempt already landed — a retry never deducts their
        stock twice), or a :class:`StockRejection` naming the line whose free
        stock (or missing stock row) refused the batch. Raises
        ``ReservationConflictError`` (line already held at a different quantity)
        and :class:`ReservationContendedError` (repeated uniqueness races), with
        the same meaning as :meth:`reserve`.
        """
        ...

    async def release(self, reservation_id: uuid.UUID, outbox_factory: OutboxFactory) -> bool:
        """Give a held reservation's stock back (saga compensation).

        ``False`` when the row wasn't ``held`` — a replayed compensation is a no-op
        and emits no second ``StockReleased``. Raises ``StockMutationError`` if the
        guarded stock give-back matches no row.
        """
        ...

    async def commit_reservation(self, reservation_id: uuid.UUID) -> bool:
        """Turn a hold into a real deduction on payment success (``on_hand -= qty``).

        ``False`` on replay (the row was no longer ``held``). No event: the order and
        payment events already announce the outcome.
        """
        ...

    async def release_expired(self, *, batch_size: int, outbox_factory: OutboxFactory) -> int:
        """Reaper sweep: release up to ``batch_size`` holds past ``expires_at``.

        Returns how many were released. Claims with ``FOR UPDATE SKIP LOCKED`` so
        concurrent reaper replicas split the batch instead of double-releasing.
        """
        ...

    async def release_for_order(self, order_id: uuid.UUID, outbox_factory: OutboxFactory) -> int:
        """Release every still-``held`` reservation of one order (saga compensation).

        Returns how many were released; ``0`` on replay (nothing was ``held``).
        """
        ...

    async def restock_for_order(self, order_id: uuid.UUID, outbox_factory: OutboxFactory) -> int:
        """Reverse every ``committed`` reservation of one order (``on_hand += qty``).

        For an order that died after its holds were consumed. One transaction,
        emits ``StockReleased`` per row; returns how many were reversed, ``0`` on
        replay (nothing was ``committed``). Callers must never invoke it for a
        ``paid`` order.
        """
        ...

    async def commit_for_order(self, order_id: uuid.UUID, *, expected: int) -> int:
        """Consume the order's still-``held`` reservations, all-or-nothing (saga success).

        ``expected`` is the order's line count: when the committed + held rows
        fall short of it, **nothing is consumed** (the survivors stay ``held``
        for the compensation's release — consuming them would deduct stock on an
        order that is about to be cancelled).

        Returns how many of the order's reservations are in ``committed`` status
        after the call — the retry-safe end-state, not the per-call rowcount
        (a replay reports the lines committed earlier, never a false ``0``).
        No event emitted.
        """
        ...
