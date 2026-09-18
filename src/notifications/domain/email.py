"""Notification domain — the email-type vocabulary.

The confirmation's COPY (subject + text + html) is carried by the packaged
email themes (:mod:`src.notifications.themes`) — file-based, so a frontend can
brand it by mounting its own theme dir (the same startup-decided switch the
Keycloak realm's ``emailTheme`` uses). The event is the data: it carries
product ids (not names) and prices as the wire's strings, so the body lists
exactly what the event has — no catalog lookup, no cross-module read.
"""

from __future__ import annotations

from enum import StrEnum


class EmailType(StrEnum):
    """The kind of notification email (the ``sent_emails.email_type`` vocabulary)."""

    ORDER_CONFIRMATION = "order_confirmation"
