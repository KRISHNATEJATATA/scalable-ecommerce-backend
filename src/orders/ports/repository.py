"""Port (Protocol) for the orders repository — reads plus the checkout writes.

``user_id`` scope is repo-applied ownership, never a client filter. The write
side serves the checkout saga: ``create_pending_order`` deduplicates on the
composite ``(user_id, idempotency_key)`` UNIQUE, ``transition_status`` is a
guarded lifecycle flip, and ``claim_stuck_pending`` leases crashed sagas to the
recovery poller.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any, Protocol

from src.shared.db.outbox import OutboxMessage


class OrdersRepositoryPort(Protocol):
    async def list_orders(self, user_id: uuid.UUID, params: Any, status: Any | None = None) -> Any: ...

    async def get_order(self, order_id: uuid.UUID) -> Any | None: ...

    async def get_by_idempotency(self, user_id: uuid.UUID, idempotency_key: str) -> Any | None:
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
    ) -> tuple[Any, bool]:
        """Insert the ``pending`` order + lines; a key replay returns ``(winner, False)``."""
        ...

    async def transition_status(
        self,
        order_id: uuid.UUID,
        *,
        expect: list[Any],
        to_status: Any,
        outbox: OutboxMessage | None = None,
    ) -> Any | None:
        """Guarded lifecycle flip; ``None`` when the current status wasn't in ``expect``."""
        ...

    async def log_saga_step(self, order_id: uuid.UUID, step: str, status: str) -> None:
        """Journal one saga step attempt for recovery/compensation."""
        ...

    async def latest_saga_step(self, order_id: uuid.UUID, step: str) -> str | None:
        """The most recent journal status for one step, or ``None`` if never attempted."""
        ...

    async def list_saga_steps(self, order_id: uuid.UUID) -> Any:
        """The order's full journal in execution order (oldest first)."""
        ...

    async def has_recent_saga_activity(self, order_id: uuid.UUID, *, since: datetime) -> bool:
        """Whether any journal row for the order was written after ``since``."""
        ...

    async def rollback(self) -> None:
        """Drop any half-finished transaction on the underlying session."""
        ...

    async def claim_stuck_pending(self, *, cutoff: datetime, batch_size: int) -> list[Any]:
        """Lease quiet ``pending`` orders older than ``cutoff`` (SKIP LOCKED, heartbeat-guarded)."""
        ...
