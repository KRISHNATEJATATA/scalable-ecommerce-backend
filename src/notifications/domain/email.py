"""Notification domain — the email-type vocabulary.

The COPY (subject + text + html) of every email type is carried by the packaged
email themes (:mod:`src.notifications.themes`) — file-based, so a frontend can
brand them by mounting its own theme dir (the same startup-decided switch the
Keycloak realm's ``emailTheme`` uses). The event is the data: it carries product
ids (not names) and prices as the wire's strings, so the body lists exactly what
the event has — no catalog lookup, no cross-module read.
"""

from __future__ import annotations

from enum import StrEnum


class EmailType(StrEnum):
    """The kind of notification email (the ``sent_emails.email_type`` vocabulary).

    Additive by design: the column is an unconstrained ``String(64)``, so a new
    member needs no migration — only a theme triple (the loader fails fast until
    the theme dir ships it) and a handler.
    """

    ORDER_CONFIRMATION = "order_confirmation"
    REFUND_ISSUED = "refund_issued"


class EmailStatus(StrEnum):
    """The durable send state of one ``sent_emails`` row.

    ``PENDING`` = the intent is claimed but the send outcome is unknown (the
    crash window lives here); ``SENT`` = the sender accepted the message.
    """

    PENDING = "pending"
    SENT = "sent"
