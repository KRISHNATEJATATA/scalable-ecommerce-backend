"""Port (Protocol) for the inventory repository.

Implemented by ``adapters/db/repository.InventoryRepository``. Covers the raw
conditional-decrement CAS primitive plus the reservation lifecycle composed on
top of it (hold + TTL, release, commit, expiry sweep) — each a single
transaction that carries its own outbox row.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from datetime import datetime
from typing import Any, Protocol

from src.shared.db.outbox import OutboxMessage

# Row return types are the adapter's ORM rows, typed as Any because ports must
# not import adapters (ports <- adapters). Upgrade to a domain type once
# inventory grows behavior beyond the reservation status machine.

#: Builds the outbox message for one released hold, from ``(sku, order_id, qty)``.
#: Defined here (the contract), imported by the adapter — never redeclared.
#: ``stock_released_outbox`` satisfies it directly.
OutboxFactory = Callable[[str, uuid.UUID, int], OutboxMessage]


class InventoryRepositoryPort(Protocol):
    async def get_by_sku(self, sku: str) -> Any | None:
        """The stock row for ``sku``, or ``None`` if the SKU has no inventory."""
        ...

    async def get_many_by_skus(self, skus: list[str]) -> dict[str, Any]:
        """The stock rows for ``skus`` as ``{sku: row}``, in one query.

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
    ) -> Any | None:
        """``None`` = insufficient stock (the oversell guard's answer, counted as such).

        Raises ``ReservationConflictError`` when the order line already holds a
        different quantity (a caller contradiction), and
        :class:`ReservationContendedError` when repeated uniqueness races mean the
        line is under churn — transient pressure that must not be reported, or
        counted, as a stock rejection."""
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

    async def commit_for_order(self, order_id: uuid.UUID) -> int:
        """Consume every still-``held`` reservation of one order (saga success).

        Returns how many were consumed; ``0`` on replay. No event emitted.
        """
        ...
