"""Compose the payment-success callback without cross-module imports in payments."""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from src.orders.adapters.db.repository import OrdersRepository
from src.payments.adapters.db.repository import PaymentsRepository
from src.payments.ports.repository import PaymentSucceededHook


def cancelled_order_refund_hook(session: AsyncSession) -> PaymentSucceededHook:
    """Journal a cancelled order's refund intent in the payment transition's session."""
    return OrdersRepository(session).journal_refund_if_cancelled


def orders_repository_with_payment_guard(session: AsyncSession) -> OrdersRepository:
    """Check for an already-captured payment before committing a cancellation."""
    return OrdersRepository(session, has_succeeded_payment=PaymentsRepository(session).has_succeeded_for_order)
