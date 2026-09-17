"""Notification domain — email types + the order-confirmation rendering.

The confirmation's content is domain language, rendered from the validated
``OrderPlaced`` payload. The event is the data: it carries product ids (not
names) and prices as the wire's strings, so the body lists exactly what the
event has — no catalog lookup, no cross-module read.
"""

from __future__ import annotations

from enum import StrEnum


class EmailType(StrEnum):
    """The kind of notification email (the ``sent_emails.email_type`` vocabulary)."""

    ORDER_CONFIRMATION = "order_confirmation"


def render_order_confirmation(order_id: str, total: str, items: list[tuple[str, int, str]]) -> tuple[str, str]:
    """Render the confirmation email's subject + plain-text body from a paid order.

    Values arrive as the validated event's wire shapes (ids and prices as
    strings); they are interpolated verbatim so the email mirrors the order.
    """
    lines = "\n".join(f"  - {product_id} x{quantity} @ {unit_price}" for product_id, quantity, unit_price in items)
    subject = f"Order confirmation — {order_id}"
    body = f"Thanks for your order!\n\nOrder: {order_id}\nTotal: {total}\n\nItems:\n{lines}\n"
    return subject, body
