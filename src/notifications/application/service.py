"""Notification use-cases: order-confirmation emails from bus events.

``NotificationService`` consumes the validated ``OrderPlaced`` / ``UserCreated``
events the SqsConsumer hands it (contract-validated before the handler runs).
The recipient email is resolved from THIS module's own ``recipients`` table —
materialized from ``UserCreated`` events, which carry ``user_id`` + ``email`` —
so the send path never reads identity/Keycloak directly (no cross-module
import; the bus is the delivery mechanism).

``OrderPlaced`` send path:
1. resolve the recipient — unknown ⇒ raise (the message is left for SQS
   redrive → DLQ after ``maxReceiveCount``: never silently dropped; once the
   user's first authenticated request JITs them, ``UserCreated`` materializes
   the recipient and the redelivery sends);
2. suppression list ⇒ never send (counted, ack);
3. the ``sent_emails`` backstop ⇒ already sent ⇒ ack (the check that keeps a
   dedupe-TTL expiry from ever double-sending);
4. render + send via the sender port, then record the sent row — a transient
   send failure raises BEFORE anything is recorded, so the redrive retries
   cleanly; the crash window between the send and the record can double-send
   once (at-least-once email delivery — email APIs are not transactional), the
   accepted residual.
"""

from __future__ import annotations

import logging
import uuid
from typing import Any

from src.notifications.application.metrics import notification_sent_total, notification_suppressed_total
from src.notifications.domain.email import EmailType
from src.notifications.ports.repository import NotificationRepositoryPort
from src.notifications.ports.sender import NotificationSenderPort
from src.notifications.themes import EmailTheme

log = logging.getLogger(__name__)


class UnknownRecipientError(RuntimeError):
    """No email is known for an ``OrderPlaced``'s user — left for redrive → DLQ, never silent."""


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
        """Send the order confirmation for one ``OrderPlaced`` (idempotent, suppression-aware)."""
        data = event["data"]
        order_id = uuid.UUID(str(data["order_id"]))
        email = await self._repo.get_recipient_email(uuid.UUID(str(data["user_id"])))
        if email is None:
            raise UnknownRecipientError(f"no email known for order {order_id}'s user; leaving for redrive → DLQ")
        if await self._repo.is_suppressed(email):
            notification_suppressed_total.labels(reason="suppressed").inc()
            return
        if await self._repo.has_sent(order_id, EmailType.ORDER_CONFIRMATION.value):
            notification_suppressed_total.labels(reason="already_sent").inc()
            return
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
        await self._sender.send(to=email, subject=subject, body=body, body_html=body_html)
        if not await self._repo.record_sent(order_id, EmailType.ORDER_CONFIRMATION.value, email):
            # A concurrent worker recorded it first (the crash window) — sent either way.
            log.debug("sent_emails backstop hit for order %s (concurrent send); tolerated", order_id)
        notification_sent_total.labels(email_type=EmailType.ORDER_CONFIRMATION.value).inc()
