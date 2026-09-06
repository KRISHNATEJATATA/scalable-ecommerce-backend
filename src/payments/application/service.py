"""Payments use-cases: the saga's Payment step, webhooks, and reconciliation.

Three entry points over the same guarded state machine:

- :meth:`PaymentsService.charge` — create (or resume) the attempt under an
  idempotency key and drive it to a terminal state through the gateway. The key
  is propagated to the gateway itself, so a retried charge cannot double-charge
  even before our DB dedup is consulted.
- :meth:`PaymentsService.handle_webhook` — async provider confirmation.
  Duplicate **and out-of-order** notifications are no-ops: outcomes are applied by
  a single guarded ``pending → terminal`` UPDATE, so whatever arrives second
  updates zero rows. The body is HMAC-verified; card data never appears anywhere
  in this flow (tokens only — PCI SAQ-A).
- :meth:`PaymentsService.reconcile` — the poller that asks the gateway what
  happened to still-pending charges, so a *missed* webhook still resolves. Rows
  past ``max_age`` whose lookup affirmatively returns "never saw it" are
  *abandoned* (guarded flip to ``failed``) instead of asked about forever.

Every applied outcome emits its event via the transactional outbox, written from
the transition's own RETURNING row inside the same transaction.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import re
import uuid
from decimal import Decimal
from typing import Any

from src.payments.application.dto import PaymentResponse
from src.payments.application.mappers import to_domain
from src.payments.application.outbox import payment_failed_outbox, payment_succeeded_outbox
from src.payments.domain.payment import PaymentStatus
from src.payments.ports.gateway import GatewayCharge, GatewayOutcome, PaymentGatewayPort
from src.payments.ports.repository import PaymentsRepositoryPort
from src.shared.config.setting import AppSettings
from src.shared.db.pagination import PageParams, PageResponse
from src.shared.errors.exceptions import (
    AuthenticationError,
    DependencyUnavailableError,
    InvalidPaymentMethodError,
    PaymentIdempotencyConflictError,
    UnknownPaymentRefError,
)

log = logging.getLogger(__name__)

# A raw PAN is 12–21 digits, often spaced or dashed. Hosted-checkout tokens are
# opaque alphanumerics with separators/prefixes — never *only* digits of that
# length. This is a shape guard at the boundary, not full Luhn validation: its
# job is to make "card data reached the server" loudly reject, not to score PANs.
_PAN_SHAPED = re.compile(r"^\d{12,21}$")

# Failure reason stamped when reconciliation abandons a charge the gateway
# affirmatively never saw (past max_age). Machine-readable on purpose: the
# RUNBOOK alerts on it, and the saga compensates a `PaymentFailed` either way.
_ABANDONED_REASON = "abandoned_by_reconciler"

_WEBHOOK_TYPE_TO_OUTCOME = {
    "payment.succeeded": GatewayOutcome.SUCCEEDED,
    "payment.failed": GatewayOutcome.FAILED,
}


class PaymentsService:
    """Read + write-side use-cases for payments."""

    def __init__(
        self,
        repo: PaymentsRepositoryPort,
        gateway: PaymentGatewayPort | None = None,
        *,
        webhook_secret: str | None = None,
        reconciliation_grace_seconds: int | None = None,
        reconciliation_max_age_seconds: int | None = None,
    ) -> None:
        self._repo = repo
        self._gateway = gateway
        self._webhook_secret = webhook_secret
        # The fields' declared defaults, not ``get_settings()``: every real call
        # site injects the configured values, so building a service must not
        # require a fully-configured environment (same pattern as CatalogService).
        self._reconciliation_grace_seconds = (
            reconciliation_grace_seconds
            if reconciliation_grace_seconds is not None
            else AppSettings.model_fields["payment_reconciliation_grace_seconds"].default
        )
        self._reconciliation_max_age_seconds = (
            reconciliation_max_age_seconds
            if reconciliation_max_age_seconds is not None
            else AppSettings.model_fields["payment_reconciliation_max_age_seconds"].default
        )

    # --- reads ------------------------------------------------------

    async def list_by_order_id(self, order_id: uuid.UUID, params: PageParams) -> PageResponse[PaymentResponse]:
        """Return a keyset page of payment attempts for an order (newest first by default)."""
        page = await self._repo.list_by_order_id(order_id, params)
        items = [PaymentResponse.model_validate(to_domain(row)) for row in page.items]
        return PageResponse(items=items, next_cursor=page.next_cursor)

    async def get_by_idempotency_key(self, idempotency_key: str) -> PaymentResponse | None:
        """The payment attempt under ``idempotency_key``, or ``None`` if never charged.

        The saga recovery poller's question: it settles a crashed checkout from
        the payment row's terminal state without re-presenting the payment
        token (which is never stored).
        """
        row = await self._repo.get_by_idempotency_key(idempotency_key)
        if row is None:
            return None
        return _response(row)

    # --- the saga's Payment step --------------------------------------------------

    async def charge(
        self, *, order_id: uuid.UUID, idempotency_key: str, amount: Decimal, payment_method_token: str
    ) -> PaymentResponse:
        """Charge ``amount`` for ``order_id``, idempotent on ``idempotency_key``.

        A replay short-circuits on our row (terminal states return as-is, pending
        resumes), and the same key rides along to the gateway so *its* dedup backs
        ours up — two layers, one guarantee: no double charge. Replaying the key
        with a different order/amount is rejected rather than silently resumed."""
        self._reject_raw_pan(payment_method_token)
        gateway = self._require_gateway()
        row, created = await self._repo.create_pending(
            order_id=order_id, idempotency_key=idempotency_key, amount=amount
        )
        if not created:
            if row.order_id != order_id or _amount(row) != amount:
                raise PaymentIdempotencyConflictError()
            if row.status != PaymentStatus.PENDING.value:
                log.info("charge retry under %s hits an already-%s payment", idempotency_key, row.status)
                return _response(row)  # decided once; replays never re-charge

        result = await gateway.charge(
            amount=amount, idempotency_key=idempotency_key, payment_method_token=payment_method_token
        )
        return await self._apply(row.id, result)

    async def handle_webhook(self, body: bytes, signature: str | None) -> bool:
        """Verify + apply one provider notification. Returns ``True`` when it changed state.

        ``False`` means a duplicate/out-of-order notification hit an already-final
        payment — an idempotent no-op the sender sees as success. Raises a 401 on
        a bad/missing signature and a 404 for an unknown reference."""
        self._verify_signature(body, signature)
        try:
            payload: Any = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise UnknownPaymentRefError(f"webhook body is not a valid event: {exc}") from exc

        outcome = _WEBHOOK_TYPE_TO_OUTCOME.get(payload.get("type"))
        idempotency_key = payload.get("idempotency_key")
        if outcome is None or not isinstance(idempotency_key, str):
            raise UnknownPaymentRefError("webhook 'type'/'idempotency_key' missing or unrecognised")
        row = await self._repo.get_by_idempotency_key(idempotency_key)
        if row is None:
            raise UnknownPaymentRefError(f"no payment exists for idempotency key {idempotency_key!r}")

        ref = payload.get("gateway_ref")
        reason = payload.get("reason")
        updated = await self._apply_outcome(
            row.id,
            outcome=outcome,
            gateway_ref=ref if isinstance(ref, str) else None,
            failure_reason=reason if isinstance(reason, str) else None,
        )
        if updated is not None:
            log.info("webhook applied: payment %s → %s", row.id, outcome)
        else:  # already final: duplicate or out-of-order delivery — nothing to do
            log.info("webhook for %s ignored: payment already %s", row.id, row.status)
        return updated is not None

    # --- reconciliation -------------------------------------------------------------

    async def reconcile(self, *, batch_size: int = 50) -> int:
        """Ask the gateway about stuck ``pending`` charges; returns how many reached a terminal state.

        One missed webhook must not strand a payment in ``pending`` forever (and
        its order with it). Each candidate inside the ``[grace, max_age]`` window
        is looked up by its idempotency key; only the gateway's answer moves
        state, through the same guarded transition a webhook uses — so a late
        webhook racing the poller is still safe. A gateway fault on one row is
        logged and skipped: the next pass retries it, and one bad row must not
        stall the batch.

        Rows past ``max_age`` are *abandoned* instead of asked about forever — but
        only on the gateway's affirmative "never saw it" (``lookup`` → ``None``):
        a *failed* lookup still postpones, so a merely unreachable gateway
        abandons nothing. The abandonment rides the same guarded flip (a webhook
        that landed first wins) and emits ``PaymentFailed`` for the saga to
        compensate."""
        gateway = self._require_gateway()
        resolved = 0
        candidates = await self._repo.due_for_reconciliation(
            grace_seconds=self._reconciliation_grace_seconds,
            max_age_seconds=self._reconciliation_max_age_seconds,
            batch_size=batch_size,
        )
        for row in candidates:
            try:
                result = await gateway.lookup(row.idempotency_key)
            except Exception:  # boundary: one flaky lookup postpones, never blocks
                log.warning("gateway lookup failed for %s; will retry next pass", row.idempotency_key, exc_info=True)
                continue
            if result is None:
                continue  # the gateway never saw this charge: leave it for a later pass
            if await self._apply_outcome(
                row.id, outcome=result.outcome, gateway_ref=result.ref, failure_reason=result.reason
            ):
                resolved += 1
        stale = await self._repo.abandonable(
            max_age_seconds=self._reconciliation_max_age_seconds, batch_size=batch_size
        )
        for row in stale:
            try:
                result = await gateway.lookup(row.idempotency_key)
            except Exception:  # boundary: a down gateway abandons nothing
                log.warning("gateway lookup failed for %s; will retry next pass", row.idempotency_key, exc_info=True)
                continue
            if result is not None:
                # The gateway *did* see it after all — resolve it like an
                # in-window row (money may have moved) instead of abandoning.
                if await self._apply_outcome(
                    row.id, outcome=result.outcome, gateway_ref=result.ref, failure_reason=result.reason
                ):
                    resolved += 1
                continue
            if await self._apply_outcome(
                row.id, outcome=GatewayOutcome.FAILED, gateway_ref=None, failure_reason=_ABANDONED_REASON
            ):
                log.warning(
                    "payment %s abandoned: gateway never saw the charge after %ds",
                    row.id,
                    self._reconciliation_max_age_seconds,
                )
                resolved += 1
        if resolved:
            log.info("reconciliation resolved %d stuck payment(s)", resolved)
        return resolved

    # --- internals --------------------------------------------------------------------

    def _require_gateway(self) -> PaymentGatewayPort:
        if self._gateway is None:  # pragma: no cover - misconfiguration guard
            raise RuntimeError("payments service requires a configured gateway")
        return self._gateway

    @staticmethod
    def _reject_raw_pan(token: str) -> None:
        normalized = token.replace(" ", "").replace("-", "")
        if _PAN_SHAPED.fullmatch(normalized):
            raise InvalidPaymentMethodError(
                "payment_method_token looks like raw card data; use a hosted-checkout token"
            )

    def _verify_signature(self, body: bytes, signature: str | None) -> None:
        """HMAC-SHA256 over the raw body, constant-time compared.

        Replay protection rides on the payment-level idempotency instead of a
        timestamp window: a replayed *valid* notification is a no-op by
        construction, so there is nothing left to replay into."""
        if not self._webhook_secret:
            raise DependencyUnavailableError(
                "PAYMENT_WEBHOOK_SECRET is not configured; refusing webhooks (fail closed)"
            )
        expected = hmac.new(self._webhook_secret.encode(), body, hashlib.sha256).hexdigest()
        received = signature.removeprefix("sha256=") if signature else ""
        if not received or not hmac.compare_digest(expected, received):
            raise AuthenticationError("invalid webhook signature")

    async def _apply(self, payment_id: uuid.UUID, result: GatewayCharge) -> PaymentResponse:
        """Land the synchronous charge outcome; a webhook that beat us wins."""
        updated = await self._apply_outcome(
            payment_id, outcome=result.outcome, gateway_ref=result.ref, failure_reason=result.reason
        )
        if updated is not None:
            return _response(updated)
        row = await self._repo.get(payment_id)
        if row is None:  # pragma: no cover - the row was created moments ago by us
            raise RuntimeError(f"payment {payment_id} vanished mid-charge")
        return _response(row)

    async def _apply_outcome(
        self, payment_id: uuid.UUID, *, outcome: str, gateway_ref: str | None, failure_reason: str | None
    ) -> Any | None:
        """One guarded flip + its outbox row; ``None`` when the payment was already final."""
        if outcome == GatewayOutcome.SUCCEEDED:
            return await self._repo.transition(
                payment_id,
                to_status=PaymentStatus.SUCCEEDED.value,
                gateway_ref=gateway_ref,
                outbox_factory=payment_succeeded_outbox,
            )
        return await self._repo.transition(
            payment_id,
            to_status=PaymentStatus.FAILED.value,
            gateway_ref=gateway_ref,
            failure_reason=failure_reason or "unknown",
            outbox_factory=payment_failed_outbox,
        )


def _response(row: Any) -> PaymentResponse:
    """Map an ORM/domain payment row to the response schema (never leak the ORM)."""
    return PaymentResponse.model_validate(to_domain(row))


def _amount(row: Any) -> Decimal:
    amount = row.amount
    return amount if isinstance(amount, Decimal) else Decimal(str(amount))
