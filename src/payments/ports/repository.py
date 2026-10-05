"""Port (Protocol) for the payments repository.

Implemented by ``adapters/db/repository.PaymentsRepository``. Two halves:

- read side: every attempt for an order as a keyset page
  (retries share ``order_id``; only ``idempotency_key`` is unique);
- write side: idempotent charge-row creation, **guarded terminal
  transitions** that emit their event in the same transaction, and the
  reconciliation poll's candidate query.

Reads return frozen domain :class:`Payment` snapshots of the committed row, never
a live ORM instance — the adapter maps before returning, so callers hold no
session state.
"""

from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable, Mapping
from decimal import Decimal
from typing import Any, Protocol

from src.payments.domain.payment import Payment
from src.shared.db.outbox import OutboxMessage
from src.shared.db.pagination import Page, PageParams
from src.shared.db.unit_of_work import UnitOfWorkPort

#: Builds the outbox message for one applied transition, from the updated row's
#: own RETURNING values — so the event carries post-update state and is written
#: inside the transition's transaction (the catalog image-flip pattern).
PaymentOutboxFactory = Callable[[Mapping[str, Any]], OutboxMessage]
PaymentSucceededHook = Callable[[uuid.UUID], Awaitable[None]]


class PaymentsRepositoryPort(Protocol):
    @property
    def uow(self) -> UnitOfWorkPort:
        """The unit of work this repository is bound to. Repositories
        participate in it but never commit it; the application service opens the
        boundary with ``async with repo.uow.transaction():``."""
        ...

    async def has_succeeded_for_order(self, order_id: uuid.UUID) -> bool:
        """Read whether a captured payment remains outstanding for this order."""
        ...

    async def list_by_order_id(self, order_id: uuid.UUID, params: PageParams) -> Page[Payment]: ...

    async def create_pending(
        self, *, order_id: uuid.UUID, idempotency_key: str, amount: Decimal
    ) -> tuple[Payment, bool]:
        """Insert a ``pending`` row deduplicated on ``idempotency_key``.

        Returns ``(row, created)``: a replay under the same key returns the
        existing row with ``created=False``, so a retried checkout resumes or
        short-circuits instead of inserting a second attempt."""
        ...

    async def get_by_idempotency_key(self, idempotency_key: str) -> Payment | None:
        """The payment minted under this key, or ``None`` (webhook lookup path)."""
        ...

    async def get(self, payment_id: uuid.UUID) -> Payment | None: ...

    async def transition(
        self,
        payment_id: uuid.UUID,
        *,
        to_status: str,
        gateway_ref: str | None = None,
        failure_reason: str | None = None,
        outbox_factory: PaymentOutboxFactory | None = None,
        expect: str | None = None,
        on_succeeded: PaymentSucceededHook | None = None,
    ) -> Payment | None:
        """Apply one outcome to a payment still in ``expect`` (default: ``pending``); ``None`` if it moved on.

        The UPDATE is guarded on the expected status — ``pending`` for a charge
        outcome (duplicate/out-of-order webhook → no-op), ``succeeded`` for the
        saga's refund leg — and RETURNs the updated row; when it lands,
        ``outbox_factory(row)`` supplies the ``PaymentSucceeded`` /
        ``PaymentFailed`` / ``PaymentRefunded`` message written **in the same
        transaction** — state change and announcement are atomic. ``on_succeeded``
        journals a cancelled order's refund intent before that same commit."""
        ...

    async def due_for_reconciliation(
        self, *, grace_seconds: int, max_age_seconds: int, batch_size: int
    ) -> list[Payment]:
        """Oldest still-``pending`` payments inside the ``[grace, max_age]`` window.

        Deliberately a plain read holding no locks: the poll only asks the gateway
        what happened, and the guarded transition makes concurrent pollers safe —
        the loser updates zero rows. Ordering by ``created_at`` keeps the oldest
        stuck rows at the front of every pass. Rows older than ``max_age`` are
        excluded here and handed to :meth:`abandonable` instead, so one orphaned
        row can never consume a batch slot on every pass forever."""
        ...

    async def abandonable(self, *, max_age_seconds: int, batch_size: int) -> list[Payment]:
        """Oldest still-``pending`` payments past ``max_age``: abandonment candidates.

        Listing a row here decides nothing — only an affirmative gateway "never
        saw it" plus the guarded terminal transition retires it, so concurrent
        pollers stay safe and a merely unreachable gateway abandons nothing."""
        ...
