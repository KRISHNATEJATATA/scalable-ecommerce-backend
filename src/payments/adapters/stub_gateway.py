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
this dict. Do not cite the stub as the production defense. The one exception is
:class:`DeferredChargeWindow`: a deferred charge must be resolvable by the
payment reconciler, which runs in its own process, so it lives in Valkey —
the stand-in for the shared window a real provider exposes to every caller.

Failure injection: a token containing ``PAYMENT_STUB_FAIL_TOKEN_SUBSTRING``
(default ``decline``) declines; everything else succeeds. That is all a stub must
do — enough to drive both event paths end to end.

Deferred-settlement injection (dev/demo only): a token containing
``PAYMENT_STUB_PENDING_TOKEN_SUBSTRING`` (empty = disabled, the default) makes
the stub answer :attr:`GatewayOutcome.PENDING` — a real provider's "processing"
answer — and resolve to ``succeeded`` on the gateway side after
``PAYMENT_STUB_PENDING_SETTLE_SECONDS``. The checkout therefore ends in the
charge-timeout 409 with the order left ``pending`` for the recovery poller,
which is the only way to demonstrate crash recovery end to end. Settings refuse
to enable it outside local/dev.

PCI SAQ-A posture: this adapter sees only an opaque ``payment_method_token``. No
PAN/CVV ever reaches it (the application layer rejects anything card-shaped
before the port is called).
"""

from __future__ import annotations

import json
import time
import uuid
from decimal import Decimal
from typing import Any

from valkey.asyncio import Valkey

from src.payments.ports.gateway import GatewayCharge, GatewayOutcome


def _decode(value: Any) -> str:
    """Valkey replies are ``bytes`` unless the client decodes — accept both."""
    if isinstance(value, bytes | bytearray):
        return bytes(value).decode("utf-8")
    return str(value)


class DeferredChargeWindow:
    """The deferred charges' shared "provider-side" record, over Valkey.

    A charge the stub answered ``pending`` is not decided yet, so it cannot
    live in the process-local map (the payment reconciler — a separate
    process — must be able to look it up). This window is that shared record:
    :meth:`defer` writes the reference and its settle deadline, and
    :meth:`state` answers the provider-idempotent current state — ``pending``
    until the deadline, ``succeeded`` after. Entries expire like a provider's
    idempotency window does; a lookup after expiry answers "never saw it".
    """

    _PREFIX = "payments:stub:deferred:"

    def __init__(self, valkey: Valkey, *, settle_seconds: int) -> None:
        self._valkey = valkey
        self._settle_seconds = settle_seconds
        self._ttl_seconds = settle_seconds + 3600  # the window outlives the settle deadline

    async def defer(self, idempotency_key: str, ref: str) -> None:
        """Record an accepted-but-undecided charge under its idempotency key."""
        payload = json.dumps({"ref": ref, "settle_at": time.time() + self._settle_seconds})
        await self._valkey.set(self._PREFIX + idempotency_key, payload, ex=self._ttl_seconds)

    async def state(self, idempotency_key: str) -> GatewayCharge | None:
        """The current gateway-side state of a deferred charge: ``pending``
        until ``settle_at``, ``succeeded`` after, ``None`` if never deferred
        (or the entry expired — the provider forgot, same as production)."""
        raw = await self._valkey.get(self._PREFIX + idempotency_key)
        if raw is None:
            return None
        try:
            entry = json.loads(_decode(raw))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None  # defensive: a corrupt entry reads as "unknown charge"
        if not isinstance(entry, dict) or not isinstance(entry.get("ref"), str):
            return None
        settle_at = entry.get("settle_at")
        if not isinstance(settle_at, int | float):
            return None
        if time.time() < settle_at:
            return GatewayCharge(ref=entry["ref"], outcome=GatewayOutcome.PENDING)
        return GatewayCharge(ref=entry["ref"], outcome=GatewayOutcome.SUCCEEDED)


class StubPaymentGateway:
    """In-memory stub implementing :class:`src.payments.ports.gateway.PaymentGatewayPort`."""

    def __init__(
        self,
        fail_token_substring: str = "decline",
        *,
        pending_token_substring: str = "",
        pending_settle_seconds: int = 5,
        deferred_window: DeferredChargeWindow | None = None,
    ) -> None:
        self._fail_substring = fail_token_substring.lower()
        self._pending_substring = pending_token_substring.lower()
        self._deferred = deferred_window
        # idempotency_key -> the one outcome that key will ever produce.
        self._charges: dict[str, GatewayCharge] = {}

    async def charge(self, *, amount: Decimal, idempotency_key: str, payment_method_token: str) -> GatewayCharge:
        existing = self._charges.get(idempotency_key)
        if existing is not None:
            return existing  # provider-side dedup: same key → same answer, no second charge
        if self._deferred is not None:
            deferred = await self._deferred.state(idempotency_key)
            if deferred is not None:
                # Deferred in this or another process: replay the one answer
                # (same ref) this key will ever produce — provider dedup again.
                return deferred
        token = payment_method_token.lower()
        declined = self._fail_substring in token
        ref = f"stub_{uuid.uuid4().hex}"
        if declined:
            charge = GatewayCharge(ref=ref, outcome=GatewayOutcome.FAILED, reason="declined_by_stub")
            self._charges[idempotency_key] = charge
            return charge
        if self._pending_substring and self._pending_substring in token:
            # Accepted but undecided: the checkout's charge step lands in the
            # documented "outcome unknown" 409 and the order stays pending for
            # the recovery poller; the reconciler resolves this charge via
            # ``lookup`` once the settle deadline passes. Decline wins if a
            # token somehow carries both substrings.
            if self._deferred is None:  # defensive: the settings factory never builds this shape
                raise RuntimeError("pending trigger requires a deferred window")
            await self._deferred.defer(idempotency_key, ref)
            return GatewayCharge(ref=ref, outcome=GatewayOutcome.PENDING)
        charge = GatewayCharge(ref=ref, outcome=GatewayOutcome.SUCCEEDED, reason=None)
        self._charges[idempotency_key] = charge
        return charge

    async def lookup(self, idempotency_key: str) -> GatewayCharge | None:
        charge = self._charges.get(idempotency_key)
        if charge is not None:
            return charge
        if self._deferred is not None:
            return await self._deferred.state(idempotency_key)  # pending → succeeded at settle_at
        return None


def stub_gateway_from_settings(settings: Any, valkey: Valkey | None = None) -> StubPaymentGateway:
    """Build the stub from ``AppSettings``, wiring the deferred-settlement demo
    trigger when it is configured.

    The trigger needs the shared window (the reconciler runs in its own
    process), so configuring it without a Valkey client is a broken setup and
    is refused loudly instead of silently stranding orders.
    """
    if not settings.payment_stub_pending_token_substring:
        return StubPaymentGateway(settings.payment_stub_fail_token_substring)
    if valkey is None:
        raise RuntimeError(
            "PAYMENT_STUB_PENDING_TOKEN_SUBSTRING requires a Valkey client: "
            "a deferred charge must be resolvable by the payment reconciler's lookup"
        )
    return StubPaymentGateway(
        settings.payment_stub_fail_token_substring,
        pending_token_substring=settings.payment_stub_pending_token_substring,
        pending_settle_seconds=settings.payment_stub_pending_settle_seconds,
        deferred_window=DeferredChargeWindow(valkey, settle_seconds=settings.payment_stub_pending_settle_seconds),
    )
