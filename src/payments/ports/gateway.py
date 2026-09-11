"""Port (Protocol) for the payment gateway — the Strategy edge.

Implemented today by ``adapters/stub_gateway.StubPaymentGateway``; a real
provider (Stripe/Adyen/…) drops in behind the same Protocol without touching the
service, routes, or worker. The port speaks the *gateway's* language:

- :meth:`PaymentGatewayPort.charge` takes an **idempotency key** and a payment-
  method **token** — never card data. The provider de-duplicates on that key, so
  a retried charge cannot double-charge even if our own row was lost.
- :meth:`PaymentGatewayPort.lookup` answers "what actually happened to this
  charge?" for the reconciliation poller, so a missed webhook still resolves.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol


@dataclass(frozen=True, slots=True)
class GatewayCharge:
    """One gateway-side charge outcome. ``ref`` is the provider's own reference."""

    ref: str
    outcome: str  # ``GatewayOutcome.succeeded`` | ``GatewayOutcome.failed`` | ``GatewayOutcome.pending``
    reason: str | None = None


class GatewayOutcome:
    """The outcome vocabulary of :class:`GatewayCharge` (plain constants: wire-shaped).

    ``pending`` is a real provider's "processing" answer: the charge is
    accepted but not yet decided, so the caller must not treat it as either
    terminal outcome — the payment row stays ``pending`` and the reconciliation
    poller asks :meth:`PaymentGatewayPort.lookup` until the provider resolves.
    """

    SUCCEEDED = "succeeded"
    FAILED = "failed"
    PENDING = "pending"


class PaymentGatewayPort(Protocol):
    async def charge(self, *, amount: Decimal, idempotency_key: str, payment_method_token: str) -> GatewayCharge:
        """Charge ``amount`` against ``payment_method_token``, de-duplicated by
        ``idempotency_key``: charging the same key twice returns the same outcome
        and reference as the first attempt."""
        ...

    async def lookup(self, idempotency_key: str) -> GatewayCharge | None:
        """The recorded outcome for ``idempotency_key``, or ``None`` if the gateway
        has never seen it (the reconciliation poller's question)."""
        ...
