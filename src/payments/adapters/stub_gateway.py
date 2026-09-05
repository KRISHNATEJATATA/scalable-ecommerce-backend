"""The stub payment gateway — the concrete :class:`PaymentGatewayPort` (Strategy).

Deterministic, in-process, and **provider-idempotent**: the first charge under an
idempotency key decides the outcome and mints the reference, and every replay of
that key returns exactly the same answer — so a retried charge can't double-charge
even before our own DB guards are consulted. The map is process-local on purpose:
it *simulates* a provider's idempotency window, while the durable guarantees live
in ``payments.payments`` (``UNIQUE(idempotency_key)`` + guarded terminal
transitions).

Cross-process caveat: with more than one app replica, two concurrent charges
under the same key can each land in a *different* process-local map. The durable
no-double-charge guarantees are our own ``UNIQUE(idempotency_key)`` (exactly one
row per key) plus the real provider's idempotency window in production — never
this dict. Do not cite the stub as the production defense.

Failure injection: a token containing ``PAYMENT_STUB_FAIL_TOKEN_SUBSTRING``
(default ``decline``) declines; everything else succeeds. That is all a stub must
do — enough to drive both event paths end to end.

PCI SAQ-A posture: this adapter sees only an opaque ``payment_method_token``. No
PAN/CVV ever reaches it (the application layer rejects anything card-shaped
before the port is called).
"""

from __future__ import annotations

import uuid
from decimal import Decimal

from src.payments.ports.gateway import GatewayCharge, GatewayOutcome


class StubPaymentGateway:
    """In-memory stub implementing :class:`src.payments.ports.gateway.PaymentGatewayPort`."""

    def __init__(self, fail_token_substring: str = "decline") -> None:
        self._fail_substring = fail_token_substring.lower()
        # idempotency_key -> the one outcome that key will ever produce.
        self._charges: dict[str, GatewayCharge] = {}

    async def charge(self, *, amount: Decimal, idempotency_key: str, payment_method_token: str) -> GatewayCharge:
        existing = self._charges.get(idempotency_key)
        if existing is not None:
            return existing  # provider-side dedup: same key → same answer, no second charge
        declined = self._fail_substring in payment_method_token.lower()
        charge = GatewayCharge(
            ref=f"stub_{uuid.uuid4().hex}",
            outcome=GatewayOutcome.FAILED if declined else GatewayOutcome.SUCCEEDED,
            reason="declined_by_stub" if declined else None,
        )
        self._charges[idempotency_key] = charge
        return charge

    async def lookup(self, idempotency_key: str) -> GatewayCharge | None:
        return self._charges.get(idempotency_key)
