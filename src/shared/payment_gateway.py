"""The one place the payment gateway is constructed — for **every** process.

Both the API and the saga-recovery worker build their gateway here. That matters
because the worker is a gateway *client* in its own right ( it refunds
charges it never took, in a process that never saw the request), so a provider
swap that missed it would "refund" at the stub — the payment row would land
`refunded`, no money would move, and nothing would alert.

The resilience shell (bounded retry + circuit breaker) wraps whatever concrete
gateway is configured, so a real provider drops in behind the same protection.
"""

from __future__ import annotations

from typing import Any

from src.payments.adapters.resilient_gateway import ResilientPaymentGateway
from src.payments.adapters.stub_gateway import stub_gateway_from_settings
from src.payments.ports.gateway import PaymentGatewayPort
from src.shared.config.setting import AppSettings
from src.shared.resilience import ValkeyConcurrencyBudget


def make_payment_gateway(settings: AppSettings, valkey: Any | None) -> PaymentGatewayPort:
    """Build the configured gateway behind the resilience shell.

    ``valkey`` backs the stub's deferred-charge window when the dev/demo pending
    trigger is enabled (``stub_gateway_from_settings`` refuses a pending trigger
    without one); a real provider ignores it. It also backs the fleet-wide
    concurrency budget — ``None`` (bare test app) leaves the
    per-process breaker as the only guard.
    """
    budget = (
        ValkeyConcurrencyBudget(
            valkey,
            max_concurrent=settings.resilience_dependency_max_concurrent,
            permit_ttl_seconds=settings.resilience_permit_ttl_seconds,
        )
        if valkey is not None
        else None
    )
    return ResilientPaymentGateway(
        stub_gateway_from_settings(settings, valkey),
        budget=budget,
        max_attempts=settings.resilience_max_attempts,
        base_delay_seconds=settings.resilience_retry_base_delay_seconds,
        max_delay_seconds=settings.resilience_retry_max_delay_seconds,
        failure_threshold=settings.resilience_breaker_failure_threshold,
        reset_timeout_seconds=settings.resilience_breaker_reset_seconds,
    )
