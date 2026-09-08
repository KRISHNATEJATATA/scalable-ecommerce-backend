"""Resilient wrapper around the :class:`PaymentGatewayPort` (decorator pattern).

Wraps any gateway (today the stub; tomorrow a real provider adapter) with the
shared bounded retry + circuit breaker. The port stays untouched — the saga
service keeps speaking :class:`PaymentGatewayPort`, unaware that its calls now
retry transient faults and fail fast (503) while the breaker is open.

Retry safety: ``charge`` carries the **idempotency key** to the gateway, which
de-duplicates on it — a retried charge cannot double-charge (the stub's map and
a real provider's idempotency window both guarantee this). ``lookup`` is a
read. Only transient faults (connection errors, timeouts, 5xx) retry; a
business decline (a definitive ``failed`` outcome) is an answer, not a fault,
and is never retried.

One logical call = ``resilience_max_attempts`` bounded tries inside the
breaker's allow/record brackets, so the breaker trips on repeated logical
failures — not on every in-retry blip.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from decimal import Decimal
from typing import TypeVar

from src.payments.ports.gateway import GatewayCharge, PaymentGatewayPort
from src.shared.errors.exceptions import DependencyUnavailableError
from src.shared.resilience import CircuitBreaker, CircuitOpenError, is_transient_exception, retry_transient

logger = logging.getLogger(__name__)

_T = TypeVar("_T")

_DEPENDENCY = "payment_gateway"


class ResilientPaymentGateway:
    """Implements :class:`PaymentGatewayPort` over an inner gateway + resilience."""

    def __init__(
        self,
        inner: PaymentGatewayPort,
        *,
        breaker: CircuitBreaker | None = None,
        max_attempts: int = 3,
        base_delay_seconds: float = 0.2,
        max_delay_seconds: float = 2.0,
        failure_threshold: int = 5,
        reset_timeout_seconds: float = 30.0,
    ) -> None:
        self._inner = inner
        self._breaker = breaker or CircuitBreaker(
            _DEPENDENCY,
            failure_threshold=failure_threshold,
            reset_timeout_seconds=reset_timeout_seconds,
        )
        self._attempts = max_attempts
        self._base_delay = base_delay_seconds
        self._max_delay = max_delay_seconds

    async def charge(self, *, amount: Decimal, idempotency_key: str, payment_method_token: str) -> GatewayCharge:
        """Charge under resilience: the idempotency key makes bounded retries
        safe (the gateway de-duplicates); an open breaker fails fast (503)."""
        return await self._resilient(
            lambda: self._inner.charge(
                amount=amount, idempotency_key=idempotency_key, payment_method_token=payment_method_token
            ),
            context=f"charge {idempotency_key}",
        )

    async def lookup(self, idempotency_key: str) -> GatewayCharge | None:
        """Look up one charge's outcome under the same resilience shell (read-only)."""
        return await self._resilient(lambda: self._inner.lookup(idempotency_key), context=f"lookup {idempotency_key}")

    async def _resilient(self, operation: Callable[[], Awaitable[_T]], *, context: str) -> _T:
        """One logical gateway call: breaker gate → bounded retry → outcome record.

        Exceptions classify three ways: transient faults record a breaker
        failure and surface as 503; definitive answers (business outcomes,
        programming errors) pass through untouched — the breaker tracks
        dependency *health*, not business results; and a cancellation records
        an **abandoned** call (no health signal, but the half-open probe slot
        must be released or the breaker wedges). Success resets the failure
        streak.
        """
        if not self._breaker.allow():
            raise CircuitOpenError("payment gateway is unavailable (circuit open)")
        try:
            result = await retry_transient(
                operation,
                attempts=self._attempts,
                base_delay_seconds=self._base_delay,
                max_delay_seconds=self._max_delay,
                on_retry=lambda attempt, exc, delay: logger.warning(
                    "payment gateway %s failed (attempt %d/%d): %s; retrying in %.2fs",
                    context,
                    attempt,
                    self._attempts,
                    type(exc).__name__,
                    delay,
                ),
            )
        except Exception as exc:
            if is_transient_exception(exc):
                self._breaker.record_failure()
                raise DependencyUnavailableError("payment gateway is unavailable") from exc
            raise
        except BaseException:
            # CancelledError et al.: no outcome, but release any half-open probe slot.
            self._breaker.record_abandoned()
            raise
        self._breaker.record_success()
        return result
