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
import time
import uuid
from decimal import Decimal
from typing import Any

from src.payments.application.dto import PaymentResponse
from src.payments.application.outbox import payment_failed_outbox, payment_refunded_outbox, payment_succeeded_outbox
from src.payments.domain.payment import Payment, PaymentStatus
from src.payments.ports.gateway import GatewayCharge, GatewayOutcome, PaymentGatewayPort
from src.payments.ports.repository import PaymentsRepositoryPort, PaymentSucceededHook
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

#: Unix-seconds header binding a webhook delivery to its moment of signing.
WEBHOOK_TIMESTAMP_HEADER = "X-Webhook-Timestamp"


def sign_webhook(body: bytes, secret: str, *, timestamp: int | None = None) -> str:
    """Mint ``X-Payment-Signature`` for one delivery — the sender's side of the scheme.

    HMAC-SHA256 over ``f"{timestamp}.".encode() + body``: the timestamp sits
    *inside* the signed payload, so re-dating a captured delivery breaks the
    signature rather than re-validating it. The verifier (:meth:`PaymentsService._verify_signature`)
    is this function's mirror image; a real gateway integration (or the tests)
    must sign through here so the two sides cannot drift.
    """
    ts = int(time.time()) if timestamp is None else timestamp
    return "sha256=" + hmac.new(secret.encode(), f"{ts}.".encode() + body, hashlib.sha256).hexdigest()


class PaymentsService:
    """Read + write-side use-cases for payments."""

    def __init__(
        self,
        repo: PaymentsRepositoryPort,
        gateway: PaymentGatewayPort | None = None,
        *,
        webhook_secret: str | None = None,
        webhook_tolerance_seconds: int | None = None,
        reconciliation_grace_seconds: int | None = None,
        reconciliation_max_age_seconds: int | None = None,
        on_payment_succeeded: PaymentSucceededHook | None = None,
    ) -> None:
        self._repo = repo
        self._gateway = gateway
        self._on_payment_succeeded = on_payment_succeeded
        self._webhook_secret = webhook_secret
        # The fields' declared defaults, not ``get_settings()``: every real call
        # site injects the configured values, so building a service must not
        # require a fully-configured environment (same pattern as CatalogService).
        self._webhook_tolerance_seconds = (
            webhook_tolerance_seconds
            if webhook_tolerance_seconds is not None
            else AppSettings.model_fields["payment_webhook_tolerance_seconds"].default
        )
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
        items = [PaymentResponse.model_validate(row) for row in page.items]
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

    async def refund(self, *, idempotency_key: str, reason: str) -> bool:
        """Return the money of the succeeded charge under ``idempotency_key``.

        The saga's reverse leg for an orphaned paid payment (a cancel or a
        stock-shortfall compensation won while the charge was landing): the provider
        call raises or answers (idempotent on the same key discipline as the
        charge — a retry cannot double-refund), then the guarded
        ``succeeded → refunded`` flip, which preserves the original ``gateway_ref``
        and writes the ``PaymentRefunded`` outbox row in the same transaction.

        Returns ``True`` when the money is confirmed returned (the flip landed
        this call or a previous one). A payment that is not ``succeeded``
        refunds nothing: still-``pending`` belongs to the reconciler,
        ``refunded`` is already done — and the row stays ``succeeded`` on any
        failure, the truthful state (money taken, not yet returned), which keeps
        the RUNBOOK §9 orphan query finding it. Raises (a dead provider behind
        the resilient shell) escape to the saga, whose arms own the pair: the
        drive arm contains the raise, counts it and answers "reconciled" (its
        order is already terminal); the poller arm lets the pass roll back so
        the next pass retries instead of counting a transient."""
        gateway = self._require_gateway()
        row = await self._repo.get_by_idempotency_key(idempotency_key)
        if row is None:
            # No charge row under this key: nothing we recorded to refund. The
            # caller (the saga) can only have raced an unseen state — surface
            # it as the not-refundable answer, never as success.
            log.error("refund (%s) requested for unknown charge key %s", reason, idempotency_key)
            return False
        if row.status != PaymentStatus.SUCCEEDED.value:
            log.info("refund (%s) skipped for a %s payment under %s", reason, row.status, idempotency_key)
            return row.status == PaymentStatus.REFUNDED.value
        payment_id, order_id, gateway_ref = row.id, row.order_id, row.gateway_ref
        result = await gateway.refund(amount=row.amount, idempotency_key=idempotency_key, gateway_ref=gateway_ref)
        if result.outcome != GatewayOutcome.SUCCEEDED:
            # The row deliberately stays `succeeded`: the charge landed and the
            # money is still out — falsifying it to `failed` would break the
            # §9 orphan query and misstate the ledger. The saga's orphan
            # counter + this line are the alertable signal.
            log.error(
                "refund (%s) failed for payment %s (order %s): %s — payment stays succeeded; "
                "manual reconciliation required",
                reason,
                payment_id,
                order_id,
                result.reason or "unknown",
            )
            return False
        # The flip + ``PaymentRefunded`` outbox row commit together: the
        # service owns this boundary (the repository only flushes), so the
        # money-side undo and its announcement are one atomic fact.
        # NOTE: the gateway call above stays OUTSIDE any transaction — a unit
        # of work must never span an external call.
        async with self._repo.uow.transaction():
            updated = await self._repo.transition(
                payment_id,
                to_status=PaymentStatus.REFUNDED.value,
                gateway_ref=gateway_ref,
                failure_reason=None,
                outbox_factory=payment_refunded_outbox,
                expect=PaymentStatus.SUCCEEDED.value,
            )
        if updated is None:
            # Already final in a way this read did not see. The only way a
            # ``succeeded`` read loses the ``succeeded → refunded`` flip is a
            # concurrent refund winning — which is the success answer. Re-read
            # to confirm rather than assuming.
            current = await self._repo.get(payment_id)
            if current is None or current.status != PaymentStatus.REFUNDED.value:
                log.error(
                    "refund (%s) flip for payment %s (order %s) lost unexpectedly — manual reconciliation required",
                    reason,
                    payment_id,
                    order_id,
                )
                return False
        # The row keeps the *charge's* provider reference — the refund is a leg,
        # not a new row, so the ledger still reads "this charge, now returned".
        # The provider's own refund reference is informational: it goes to the log
        # (that is what ties our record to theirs) rather than into the row.
        log.info(
            "payment %s (order %s) refunded (%s); provider refund %s",
            payment_id,
            order_id,
            reason,
            result.ref,
        )
        return True

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
        # Service-owned boundary: the idempotent create commits here. The
        # gateway call below stays OUTSIDE any transaction — a unit of work
        # must never hold row locks across an external call.
        async with self._repo.uow.transaction():
            row, created = await self._repo.create_pending(
                order_id=order_id, idempotency_key=idempotency_key, amount=amount
            )
        if not created:
            if row.order_id != order_id or row.amount != amount:
                raise PaymentIdempotencyConflictError()
            if row.status != PaymentStatus.PENDING.value:
                log.info("charge retry under %s hits an already-%s payment", idempotency_key, row.status)
                return _response(row)  # decided once; replays never re-charge

        result = await gateway.charge(
            amount=amount, idempotency_key=idempotency_key, payment_method_token=payment_method_token
        )
        return await self._apply(row.id, result)

    async def handle_webhook(self, body: bytes, signature: str | None, timestamp: str | None) -> bool:
        """Verify + apply one provider notification. Returns ``True`` when it changed state.

        ``False`` means a duplicate/out-of-order notification hit an already-final
        payment — an idempotent no-op the sender sees as success. Raises a 401 on
        a bad/missing signature or a timestamp outside the skew window, and a 404
        for an unknown reference."""
        self._verify_signature(body, signature, timestamp)
        try:
            payload: Any = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise UnknownPaymentRefError(f"webhook body is not a valid event: {exc}") from exc
        if not isinstance(payload, dict):  # a signed-but-non-object body must 404, not AttributeError→500
            raise UnknownPaymentRefError("webhook body is not a valid event: expected a JSON object")

        # A non-string `type` (list/dict — a signed body the gateway would never
        # send) must fall into the 404 arm too, not TypeError (unhashable) → 500.
        event_type = payload.get("type")
        outcome = _WEBHOOK_TYPE_TO_OUTCOME.get(event_type) if isinstance(event_type, str) else None
        idempotency_key = payload.get("idempotency_key")
        if outcome is None or not isinstance(idempotency_key, str):
            raise UnknownPaymentRefError("webhook 'type'/'idempotency_key' missing or unrecognised")
        row = await self._repo.get_by_idempotency_key(idempotency_key)
        if row is None:
            raise UnknownPaymentRefError(f"no payment exists for idempotency key {idempotency_key!r}")

        ref = payload.get("gateway_ref")
        reason = payload.get("reason")
        payment_id = row.id
        prior_status = row.status
        # The read above stays outside the unit of work; the guarded flip
        # below opens the service-owned boundary (see ``_apply_outcome``), so
        # a duplicate delivery never commits a half-applied outcome.
        updated = await self._apply_outcome(
            payment_id,
            outcome=outcome,
            gateway_ref=ref if isinstance(ref, str) else None,
            failure_reason=reason if isinstance(reason, str) else None,
        )
        if updated is not None:
            log.info("webhook applied: payment %s → %s", payment_id, outcome)
        else:  # already final: duplicate or out-of-order delivery — nothing to do
            log.info("webhook for %s ignored: payment already %s", payment_id, prior_status)
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
        candidates = [
            (row.id, row.idempotency_key)
            for row in await self._repo.due_for_reconciliation(
                grace_seconds=self._reconciliation_grace_seconds,
                max_age_seconds=self._reconciliation_max_age_seconds,
                batch_size=batch_size,
            )
        ]
        for payment_id, idempotency_key in candidates:
            try:
                result = await gateway.lookup(idempotency_key)
            except Exception:  # boundary: one flaky lookup postpones, never blocks
                log.warning("gateway lookup failed for %s; will retry next pass", idempotency_key, exc_info=True)
                continue
            if result is None:
                continue  # the gateway never saw this charge: leave it for a later pass
            if await self._apply_outcome(
                payment_id, outcome=result.outcome, gateway_ref=result.ref, failure_reason=result.reason
            ):
                resolved += 1
        stale = [
            (row.id, row.idempotency_key)
            for row in await self._repo.abandonable(
                max_age_seconds=self._reconciliation_max_age_seconds, batch_size=batch_size
            )
        ]
        for payment_id, idempotency_key in stale:
            try:
                result = await gateway.lookup(idempotency_key)
            except Exception:  # boundary: a down gateway abandons nothing
                log.warning("gateway lookup failed for %s; will retry next pass", idempotency_key, exc_info=True)
                continue
            if result is not None:
                # The gateway *did* see it after all — resolve it like an
                # in-window row (money may have moved) instead of abandoning.
                if await self._apply_outcome(
                    payment_id, outcome=result.outcome, gateway_ref=result.ref, failure_reason=result.reason
                ):
                    resolved += 1
                continue
            if await self._apply_outcome(
                payment_id, outcome=GatewayOutcome.FAILED, gateway_ref=None, failure_reason=_ABANDONED_REASON
            ):
                log.warning(
                    "payment %s abandoned: gateway never saw the charge after %ds",
                    payment_id,
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

    def _verify_signature(self, body: bytes, signature: str | None, timestamp: str | None) -> None:
        """HMAC-SHA256 over ``{timestamp}.{body}`` with a skew window, constant-time compared.

        Two deliberate replay layers: the signed timestamp (bounded by
        ``payment_webhook_tolerance_seconds``, future-skew included) expires a
        captured delivery outside the window, and the payment-level idempotency
        still no-ops a replay landing inside it — there is no nonce/dedupe
        store, bounded skew + idempotent flip is the accepted posture
        (``ponytail:`` revisit only if a payment could be flipped by a replay,
        which the guarded transition already prevents). Every failure — bad
        timestamp shape, expired, forged — is the same 401, so the endpoint
        gives no oracle distinguishing which half failed."""
        if not self._webhook_secret:
            raise DependencyUnavailableError(
                "PAYMENT_WEBHOOK_SECRET is not configured; refusing webhooks (fail closed)"
            )
        # The timestamp is canonicalized (parsed → int) before it feeds the HMAC,
        # so no header spelling (" 12", "+12", "012") can diverge from the form
        # the signature was computed over.
        if timestamp is None:
            raise AuthenticationError("invalid webhook signature")
        try:
            ts = int(timestamp)
        except ValueError:
            raise AuthenticationError("invalid webhook signature") from None
        if abs(time.time() - ts) > self._webhook_tolerance_seconds:
            raise AuthenticationError("invalid webhook signature")
        expected = hmac.new(self._webhook_secret.encode(), f"{ts}.".encode() + body, hashlib.sha256).hexdigest()
        received = signature.removeprefix("sha256=") if signature else ""
        # compare_digest raises on non-ASCII str; encode both sides so a garbage
        # header answers 401 instead of surfacing a TypeError.
        if not received or not hmac.compare_digest(expected.encode(), received.encode()):
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
    ) -> Payment | None:
        """One guarded flip + its outbox row; ``None`` when no transition landed.

        ``None`` covers both "already final" and a ``pending`` (processing)
        answer: the provider accepted the charge but hasn't decided it, so
        there is nothing to flip and no event to ship — the row stays
        ``pending`` for the reconciliation poller."""
        if outcome == GatewayOutcome.PENDING:
            return None
        # Service-owned boundary: the guarded flip + its outbox row commit
        # here. ``on_succeeded`` (the cancelled-order refund-intent journal)
        # runs inside the same unit of work via the transition below.
        async with self._repo.uow.transaction():
            if outcome == GatewayOutcome.SUCCEEDED:
                return await self._repo.transition(
                    payment_id,
                    to_status=PaymentStatus.SUCCEEDED.value,
                    gateway_ref=gateway_ref,
                    outbox_factory=payment_succeeded_outbox,
                    on_succeeded=self._on_payment_succeeded,
                )
            return await self._repo.transition(
                payment_id,
                to_status=PaymentStatus.FAILED.value,
                gateway_ref=gateway_ref,
                failure_reason=failure_reason or "unknown",
                outbox_factory=payment_failed_outbox,
            )


def _response(row: Payment) -> PaymentResponse:
    """Map a payment snapshot to the response schema."""
    return PaymentResponse.model_validate(row)
