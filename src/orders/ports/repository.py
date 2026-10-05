"""Port (Protocol) for the orders repository — reads plus the checkout writes.

``user_id`` scope is repo-applied ownership, never a client filter. The write
side serves the checkout saga: ``create_pending_order`` deduplicates on the
composite ``(user_id, idempotency_key)`` UNIQUE, ``transition_status`` is a
guarded lifecycle flip, and ``claim_stuck_pending`` leases crashed sagas to the
recovery poller.

Every read returns a frozen domain :class:`Order` snapshot of the *committed*
row, never a live ORM instance — callers hold no session state and need no
knowledge of SQLAlchemy's identity map or expiry rules.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Protocol

from src.orders.domain.order import Order, OrderStatus, SagaStep
from src.shared.db.outbox import OutboxMessage
from src.shared.db.pagination import Page, PageParams
from src.shared.db.unit_of_work import UnitOfWorkPort


class OrdersRepositoryPort(Protocol):
    @property
    def uow(self) -> UnitOfWorkPort:
        """The unit of work this repository is bound to. Repositories
        participate in it but never commit it; the application service opens the
        boundary with ``async with repo.uow.transaction():``."""
        ...

    async def list_orders(
        self, user_id: uuid.UUID, params: PageParams, status: OrderStatus | None = None
    ) -> Page[Order]: ...

    async def get_order(self, order_id: uuid.UUID) -> Order | None:
        """The order's committed state with lines, or ``None`` — always fresh from the DB."""
        ...

    async def get_order_status(self, order_id: uuid.UUID) -> OrderStatus | None:
        """The order's committed status alone (a single-column read), or ``None`` if it never existed."""
        ...

    async def get_by_idempotency(self, user_id: uuid.UUID, idempotency_key: str) -> Order | None:
        """The order already placed under ``(user_id, key)``, with lines, or ``None``."""
        ...

    async def create_pending_order(
        self,
        *,
        user_id: uuid.UUID,
        idempotency_key: str,
        body_hash: str,
        total: Decimal,
        lines: list[tuple[uuid.UUID, str, Decimal, int]],
        user_email: str = "",
    ) -> tuple[Order, bool]:
        """Insert the ``pending`` order + lines; a key replay returns ``(winner, False)``.

        ``user_email`` is the buyer's checkout-time address, snapshotted onto
        the row so ``OrderPlaced`` can carry it ("" = unknown — legacy/tests;
        the event then omits it and consumers fall back to the recipients
        table).
        """
        ...

    async def transition_status(
        self,
        order_id: uuid.UUID,
        *,
        expect: list[OrderStatus],
        to_status: OrderStatus,
        outbox: OutboxMessage | None = None,
    ) -> Order | None:
        """Guarded lifecycle flip; ``None`` when the current status wasn't in ``expect``."""
        ...

    async def log_saga_step(self, order_id: uuid.UUID, step: str, status: str) -> None:
        """Journal one saga step attempt for recovery/compensation."""
        ...

    async def latest_saga_step(self, order_id: uuid.UUID, step: str) -> str | None:
        """The most recent journal status for one step, or ``None`` if never attempted."""
        ...

    async def list_saga_steps(self, order_id: uuid.UUID) -> list[SagaStep]:
        """The order's full journal in execution order (oldest first)."""
        ...

    async def has_recent_saga_activity(self, order_id: uuid.UUID, *, since: datetime) -> bool:
        """Whether any journal row for the order was written after ``since``."""
        ...

    async def rollback(self) -> None:
        """Drop any half-finished transaction on the underlying session."""
        ...

    async def claim_stuck_pending(self, *, cutoff: datetime, batch_size: int) -> list[Order]:
        """Lease quiet ``pending`` orders older than ``cutoff`` (SKIP LOCKED, heartbeat-guarded)."""
        ...

    async def claim_cancelled_with_pending_refund(self, *, cutoff: datetime, batch_size: int) -> list[Order]:
        """Cancelled orders whose journaled refund intent has no terminal marker yet.

        The recovery poller's refund-retry claim: a refund that raised on a
        terminal order left ``refund: requested`` in the journal, and these rows
        are re-claimed (under the same lease discipline as the pending claim)
        until the money is confirmed back (``completed``) or the provider's
        refusal is recorded (``refused``) — the re-claim *is* the retry.
        """
        ...
