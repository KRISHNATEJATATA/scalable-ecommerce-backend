"""The sender port — how a notification email leaves the system."""

from __future__ import annotations

from typing import Protocol


class NotificationSenderPort(Protocol):
    """Sends one email. Local = SMTP (Mailpit); prod = AWS SES (ECS task role)."""

    async def send(
        self, *, to: str, subject: str, body: str, body_html: str | None = None, message_id: str | None = None
    ) -> None:
        """Send one email to ``to``: plain text plus an optional HTML alternative
        (the theme-driven confirmation carries both; older callers stay text-only).
        ``message_id`` (a bare persisted id, no brackets) rides in the MIME
        headers (``Message-ID`` + the SES-proof ``X-Notification-Message-Id``),
        so a crash-window resend carries the SAME identifier.
        Raises on any transport failure."""
        ...
