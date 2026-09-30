"""Payments repository

The write side is built around one invariant: **a payment's outcome is decided
exactly once**. Row creation deduplicates on ``idempotency_key``; outcome
application is a raw UPDATE guarded on ``status = 'pending'`` that RETURNs the
updated row, and the ``PaymentSucceeded``/``PaymentFailed`` outbox row is written
from that RETURNING row **inside the same transaction** — so "the payment
succeeded" and "the bus will announce it" are one atomic fact, and a duplicate or
out-of-order webhook updates zero rows and emits nothing.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import ColumnElement, Select, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.payments.adapters.db.mappers import to_domain
from src.payments.adapters.db.models import Outbox, Payment
from src.payments.domain.payment import Payment as DomainPayment
from src.payments.domain.payment import PaymentStatus
from src.payments.ports.repository import PaymentOutboxFactory, PaymentSucceededHook
from src.shared.db.outbox import OutboxMessage
from src.shared.db.pagination import Page, PageParams, apply_keyset, build_page, decode_cursor
from src.shared.errors.exceptions import InvalidQueryParamError

_SORT_COLUMNS = {"created_at": Payment.created_at}


def _payment_select(*where: ColumnElement[bool]) -> Select[Any]:
    """``select(Payment)`` that refreshes rows from the DB instead of answering from the identity map."""
    return select(Payment).where(*where).execution_options(populate_existing=True)


class PaymentsRepository:
    """Implements :class:`src.payments.ports.repository.PaymentsRepositoryPort`."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    # --- reads ------------------------------------------------------

    async def list_by_order_id(self, order_id: uuid.UUID, params: PageParams) -> Page[DomainPayment]:
        """Every payment attempt for an order, keyset-paginated (newest first by default)."""
        if params.sort_field not in _SORT_COLUMNS:
            raise InvalidQueryParamError("sort", params.sort_field)
        sort_col = _SORT_COLUMNS[params.sort_field]

        stmt = _payment_select(Payment.order_id == order_id)
        cursor = decode_cursor(params.cursor, "timestamptz") if params.cursor else None
        stmt = apply_keyset(stmt, sort_col, Payment.id, params, cursor)

        rows = [to_domain(row) for row in (await self._session.execute(stmt)).scalars().all()]
        return build_page(rows, params, key_of=lambda payment: (getattr(payment, params.sort_field), payment.id))

    async def get(self, payment_id: uuid.UUID) -> DomainPayment | None:
        """The payment as committed in the DB, or ``None``."""
        row = (await self._session.execute(_payment_select(Payment.id == payment_id))).scalar_one_or_none()
        return to_domain(row) if row is not None else None

    async def get_by_idempotency_key(self, idempotency_key: str) -> DomainPayment | None:
        """The payment minted under this key as committed in the DB, or ``None``."""
        stmt = _payment_select(Payment.idempotency_key == idempotency_key)
        row = (await self._session.execute(stmt)).scalar_one_or_none()
        return to_domain(row) if row is not None else None

    async def has_succeeded_for_order(self, order_id: uuid.UUID) -> bool:
        """Read the committed payment outcome without taking a payment-row lock."""
        stmt = select(Payment.id).where(Payment.order_id == order_id, Payment.status == PaymentStatus.SUCCEEDED.value)
        return (await self._session.execute(stmt.limit(1))).scalar_one_or_none() is not None

    # --- writes -----------------------------------------------------

    async def create_pending(
        self, *, order_id: uuid.UUID, idempotency_key: str, amount: Decimal
    ) -> tuple[DomainPayment, bool]:
        """Insert a ``pending`` attempt; a replay of the key returns the existing row.

        ``ON CONFLICT DO NOTHING`` + re-select keeps the uniqueness race in the DB:
        two concurrent first-requests each either insert or read the winner's row —
        exactly one of them reports ``created=True``."""
        stmt = (
            pg_insert(Payment)
            .values(
                order_id=order_id,
                idempotency_key=idempotency_key,
                amount=amount,
                status=PaymentStatus.PENDING.value,
            )
            .on_conflict_do_nothing(constraint="uq_payments_idempotency_key")
            .returning(Payment.id)
        )
        inserted_id = (await self._session.execute(stmt)).scalar_one_or_none()
        if inserted_id is None:
            existing = await self.get_by_idempotency_key(idempotency_key)
            if existing is None:  # defensive: conflict reported but the winner isn't visible
                raise RuntimeError(f"idempotency conflict for {idempotency_key!r} but no row found")
            return existing, False
        await self._session.commit()
        row = await self.get(inserted_id)
        if row is None:  # defensive: the row we just inserted must be re-readable
            raise RuntimeError(f"inserted payment {inserted_id} not found after commit")
        return row, True

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
    ) -> DomainPayment | None:
        """Apply an outcome once; ``None`` when the payment was no longer in ``expect``.

        Raw guarded UPDATE (not the ORM unit-of-work) so a concurrent webhook,
        refund and reconciliation racing the same row serialize in the DB — the
        loser matches zero rows and returns ``None`` instead of clobbering. The
        guard is ``pending`` by default (charge outcomes are decided exactly
        once); the saga's refund leg passes ``succeeded`` so only a real refund
        wins. The outbox factory is fed the update's own RETURNING values and
        its message is written before the commit, so the event can never
        announce a state that didn't land."""
        stmt = (
            update(Payment)
            .where(Payment.id == payment_id, Payment.status == (expect or PaymentStatus.PENDING.value))
            .values(status=to_status, gateway_ref=gateway_ref, failure_reason=failure_reason)
            .returning(Payment.id, Payment.order_id, Payment.amount, Payment.gateway_ref, Payment.failure_reason)
            # The caller re-reads the row after the commit (populate_existing), so
            # in-session synchronization of the identity map is redundant work.
            .execution_options(synchronize_session=False)
        )
        row = (await self._session.execute(stmt)).mappings().first()
        if row is None:
            # Lost the guarded flip — the payment was already final (a concurrent
            # webhook/charge landed first). Roll back while the transaction is
            # still open, mirroring the orders repo's ``transition_status``.
            await self._session.rollback()
            return None
        if outbox_factory is not None:
            self._session.add(self._outbox_row(outbox_factory(row)))
        if on_succeeded is not None:
            try:
                await on_succeeded(row["order_id"])
            except Exception:
                await self._session.rollback()
                raise
        await self._session.commit()
        return await self.get(payment_id)

    async def due_for_reconciliation(
        self, *, grace_seconds: int, max_age_seconds: int, batch_size: int
    ) -> list[DomainPayment]:
        """Oldest still-``pending`` payments inside the ``[grace, max_age]`` window.

        The upper bound is what keeps one orphaned row from consuming a batch slot
        on every pass forever: rows older than ``max_age`` are handed to
        :meth:`abandonable` instead, where an affirmative gateway "never saw it"
        retires them. Deliberately a plain read holding no locks: the sweep only
        *asks* the gateway what happened, and the guarded transition makes
        concurrent pollers safe — the loser updates zero rows. The cutoffs are
        computed app-side; the poll interval dwarfs any tolerable clock skew."""
        now = datetime.now(UTC)
        stmt = (
            _payment_select(
                Payment.status == PaymentStatus.PENDING.value,
                Payment.created_at <= now - timedelta(seconds=grace_seconds),
                Payment.created_at > now - timedelta(seconds=max_age_seconds),
            )
            .order_by(Payment.created_at)
            .limit(batch_size)
        )
        return [to_domain(row) for row in (await self._session.execute(stmt)).scalars().all()]

    async def abandonable(self, *, max_age_seconds: int, batch_size: int) -> list[DomainPayment]:
        """Oldest still-``pending`` payments past ``max_age``: abandonment candidates.

        Same plain-read shape as :meth:`due_for_reconciliation` — listing a row
        here decides nothing; only an affirmative gateway "never saw it" plus the
        guarded transition retires it, so a merely unreachable gateway abandons
        nothing and concurrent pollers stay safe."""
        cutoff = datetime.now(UTC) - timedelta(seconds=max_age_seconds)
        stmt = (
            _payment_select(Payment.status == PaymentStatus.PENDING.value, Payment.created_at <= cutoff)
            .order_by(Payment.created_at)
            .limit(batch_size)
        )
        return [to_domain(row) for row in (await self._session.execute(stmt)).scalars().all()]

    @staticmethod
    def _outbox_row(outbox: OutboxMessage) -> Outbox:
        return Outbox(event_type=outbox.event_type, payload=outbox.payload)
