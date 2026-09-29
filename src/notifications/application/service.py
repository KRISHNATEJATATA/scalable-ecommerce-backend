"""Notification use-cases: order-confirmation emails from bus events.

``NotificationService`` consumes the validated ``OrderPlaced`` / ``UserCreated``
events the SqsConsumer hands it (contract-validated before the handler runs).
The recipient email is resolved WITHOUT any cross-module identity read and —
since ``OrderPlaced`` carries the buyer's checkout-time ``user_email`` (an
order-row snapshot) — without depending on cross-topic event ordering either:
the event's own address wins, this module's ``recipients`` table (materialized
from ``UserCreated``) is the fallback for events written before the field
existed, and only an event with NEITHER is left for redrive → DLQ.

``OrderPlaced`` send path (explicitly AT-LEAST-ONCE, never
described as duplicate-proof):
1. resolve the recipient — the event's ``user_email`` first, else the
   ``recipients`` table; an event carrying neither (a pre-snapshot event whose
   user the consumer has never seen) ⇒ raise (the message is left for SQS
   redrive → DLQ after ``maxReceiveCount``: never silently dropped);
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
    """No email is known for an ``OrderPlaced``'s user — neither carried on the
    event nor materialized in ``recipients``. Left for redrive → DLQ, never silent."""


class NotificationService:
    """Order-confirmation use-case over the repository + sender ports.

    The confirmation copy comes from the loaded :class:`EmailTheme` (the
    startup-decided, file-based theme — packaged minimal default or a
    frontend-mounted one); the item lines stay data-shaped here and are
    rendered per format for the theme's ``$items`` placeholder.
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

        The durable claim happens BEFORE the send (ADR 0024): a redelivery after
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
        if await self._repo.is_suppressed(email):
            notification_suppressed_total.labels(reason="suppressed").inc()
            return
        email_type = EmailType.ORDER_CONFIRMATION.value
        claim = await self._repo.claim_send(order_id, email_type, email)
        if claim.outcome == "already_sent":
            notification_suppressed_total.labels(reason="already_sent").inc()
            return
        # The ONE id this confirmation ever carries: generated (uuid4) at the
        # first claim, persisted on the row, reused by every takeover resend.
        message_id = claim.message_id
        if claim.outcome == "takeover":
            # A prior attempt claimed but never marked — its send outcome is
            # unknown. Resend with the SAME persisted identifier (the
            # at-least-once residual, now detectable) and count the recovery.
            notification_send_recovered_total.labels(email_type=email_type).inc()
            log.warning("taking over a claimed-but-unmarked send for order %s (message_id=%s)", order_id, message_id)
        items = [
            # The label is the checkout-time product snapshot's name — the mail
            # shows what the user bought, not the opaque id. Events written
            # before the field existed carry None → fall back to the id.
            (str(line.get("product_name") or line["product_id"]), int(line["quantity"]), str(line["unit_price"]))
            for line in data["items"]
        ]
        items_text = "\n".join(f"  - {label} x{quantity} @ {unit_price}" for label, quantity, unit_price in items)
        items_html = "\n".join(f"<li>{label} x{quantity} @ {unit_price}</li>" for label, quantity, unit_price in items)
        subject, body, body_html = self._theme.render_order_confirmation(
            str(data["order_id"]), str(data["total"]), items_text, items_html
        )
        await self._sender.send(to=email, subject=subject, body=body, body_html=body_html, message_id=str(message_id))
        # A send failure raises BEFORE the mark — the row stays ``pending`` and
        # the redrive takes it over above, so the retry sends cleanly.
        await self._repo.mark_sent(order_id, email_type, email)
        notification_sent_total.labels(email_type=email_type).inc()
