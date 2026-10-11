"""Notification use-cases: outbound email from bus events.

``NotificationService`` consumes the validated events the SqsConsumer hands it
(contract-validated before the handler runs). Recipients are resolved WITHOUT
any cross-module identity read:

* ``OrderPlaced`` carries the buyer's checkout-time ``user_email`` (an order-row
  snapshot); the ``recipients`` table (materialized from ``UserCreated``) is the
  fallback for events written before the field existed, and the event's own
  address wins. Every handled ``OrderPlaced`` also upserts the **Order
  Recipient** projection (``order_id → email``) BEFORE any send decision — the
  address source for the refund notice, present even when the confirmation is
  suppressed or its send never completes.
* ``PaymentRefunded`` carries no address at all: it resolves the Order
  Recipient. An event whose address cannot be resolved at all raises — left
  for redrive → DLQ, never silently dropped.

Every send path is explicitly AT-LEAST-ONCE, never described as duplicate-proof,
and shares one sequence:

1. resolve the recipient (per type, above); an unresolvable one ⇒ raise (the
   message is left for SQS redrive → DLQ after ``maxReceiveCount``);
2. suppression list ⇒ never send (counted, ack);
3. CLAIM the durable send state: insert the ``sent_emails`` row ``pending``
   (generating its uuid4 message identifier) and commit BEFORE any send — a
   claimed ``sent`` row ⇒ ack (the dedupe-TTL-expiry backstop); a claimed
   ``pending`` row ⇒ a prior attempt crashed mid-window ⇒ TAKE OVER: resend
   with the same persisted identifier and count it
   (``notification_send_recovered_total``);
4. render + send via the sender port, then mark the row ``sent``.

The crash window between the send and the mark can still double-send once —
email APIs are not transactional — but every send now has durable state, every
duplicate carries the SAME persisted identifier (the
``X-Notification-Message-Id`` MIME header, reconcilable downstream — SES
overwrites the RFC ``Message-ID``), and stuck ``pending`` rows are the
reconciliation query (RUNBOOK §15).
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Callable
from typing import Any

from src.notifications.application.metrics import (
    notification_send_recovered_total,
    notification_sent_total,
    notification_suppressed_total,
)
from src.notifications.domain.email import EmailType
from src.notifications.ports.repository import NotificationRepositoryPort
from src.notifications.ports.sender import NotificationSenderPort
from src.notifications.themes import EmailTheme

log = logging.getLogger(__name__)


class UnknownRecipientError(RuntimeError):
    """No email is known for an event's order/user — neither carried on the
    event nor materialized in this module's projections. Left for redrive →
    DLQ, never silent."""


class NotificationService:
    """Notification use-cases over the repository + sender ports.

    The copy of every email type comes from the loaded :class:`EmailTheme`
    (the startup-decided, file-based theme — packaged minimal default or a
    frontend-mounted one); item lines stay data-shaped here and are rendered
    per format for the confirmation theme's ``$items`` placeholder.
    """

    def __init__(self, repo: NotificationRepositoryPort, sender: NotificationSenderPort, theme: EmailTheme) -> None:
        self._repo = repo
        self._sender = sender
        self._theme = theme

    async def handle_user_created(self, event: dict[str, Any]) -> None:
        """Materialize the ``UserCreated`` payload into the recipients table."""
        data = event["data"]
        await self._repo.upsert_recipient(uuid.UUID(str(data["user_id"])), str(data["email"]))

    async def handle_order_placed(self, event: dict[str, Any]) -> None:
        """Send the order confirmation for one ``OrderPlaced`` (at-least-once, suppression-aware).

        The durable claim happens BEFORE the send : a redelivery after
        a recorded send acks; a redelivery over a crashed attempt takes the claim
        over and resends with the same persisted message identifier.
        """
        data = event["data"]
        order_id = uuid.UUID(str(data["order_id"]))
        # The event's own checkout-time address wins (it rides the same message,
        # so no cross-topic ordering with UserCreated can strand the send); the
        # recipients table is the fallback for events written before OrderPlaced
        # carried it. Only NEITHER raises — redrive → DLQ, never silent.
        email = data.get("user_email") or await self._repo.get_recipient_email(uuid.UUID(str(data["user_id"])))
        if email is None:
            raise UnknownRecipientError(f"no address known for order {order_id}'s user; leaving for redrive → DLQ")
        # The Order Recipient projection is fed from the event the worker already
        # consumes, BEFORE any send decision — the refund notice's address source,
        # present even when this confirmation is suppressed or never completes.
        await self._repo.upsert_order_email(order_id, email)
        await self._send_claimed(
            order_id, email, EmailType.ORDER_CONFIRMATION.value, lambda: self._render_confirmation(data)
        )

    async def handle_payment_refunded(self, event: dict[str, Any]) -> None:
        """Send the refund-issued email for one ``PaymentRefunded`` (at-least-once,
        the same claim discipline as the confirmation).

        The event carries NO address: the recipient resolves from the Order
        Recipient projection (this order's own ``OrderPlaced``, handled above).
        A miss raises — redrive retries it after the visibility timeout (the
        refund event can outride its ``OrderPlaced`` across topics), and a
        permanent miss lands in the DLQ visibly rather than skipping the one
        email that tells the customer their money came back.
        """
        data = event["data"]
        order_id = uuid.UUID(str(data["order_id"]))
        email = await self._repo.get_order_email(order_id)
        if email is None:
            raise UnknownRecipientError(f"no address known for refunded order {order_id}; leaving for redrive → DLQ")
        amount = str(data["amount"])
        await self._send_claimed(
            order_id,
            email,
            EmailType.REFUND_ISSUED.value,
            lambda: self._theme.render_refund_issued(str(data["order_id"]), amount),
        )

    async def _send_claimed(
        self, order_id: uuid.UUID, email: str, email_type: str, render: Callable[[], tuple[str, str, str]]
    ) -> None:
        """The shared suppression → claim → render → send → mark sequence.

        ``render`` runs after the claim so a takeover resend renders fresh (and
        a suppressed/already-sent event never renders at all). A send failure
        raises BEFORE the mark — the row stays ``pending`` and the redrive takes
        it over, so the retry sends cleanly.
        """
        if await self._repo.is_suppressed(email):
            notification_suppressed_total.labels(reason="suppressed").inc()
            return
        claim = await self._repo.claim_send(order_id, email_type, email)
        if claim.outcome == "already_sent":
            notification_suppressed_total.labels(reason="already_sent").inc()
            return
        # The ONE id this send ever carries: generated (uuid4) at the first
        # claim, persisted on the row, reused by every takeover resend.
        message_id = claim.message_id
        if claim.outcome == "takeover":
            # A prior attempt claimed but never marked — its send outcome is
            # unknown. Resend with the SAME persisted identifier (the
            # at-least-once residual, now detectable) and count the recovery.
            notification_send_recovered_total.labels(email_type=email_type).inc()
            log.warning("taking over a claimed-but-unmarked send for order %s (message_id=%s)", order_id, message_id)
        subject, body, body_html = render()
        await self._sender.send(to=email, subject=subject, body=body, body_html=body_html, message_id=str(message_id))
        await self._repo.mark_sent(order_id, email_type, email)
        # Counted HERE (not at the callers): only a send that reached the mark —
        # suppressed and already-sent paths returned above without sending.
        notification_sent_total.labels(email_type=email_type).inc()

    def _render_confirmation(self, data: dict[str, Any]) -> tuple[str, str, str]:
        items = [
            # The label is the checkout-time product snapshot's name — the mail
            # shows what the user bought, not the opaque id. Events written
            # before the field existed carry None → fall back to the id.
            (str(line.get("product_name") or line["product_id"]), int(line["quantity"]), str(line["unit_price"]))
            for line in data["items"]
        ]
        items_text = "\n".join(f"  - {label} x{quantity} @ {unit_price}" for label, quantity, unit_price in items)
        items_html = "\n".join(f"<li>{label} x{quantity} @ {unit_price}</li>" for label, quantity, unit_price in items)
        return self._theme.render_order_confirmation(str(data["order_id"]), str(data["total"]), items_text, items_html)
