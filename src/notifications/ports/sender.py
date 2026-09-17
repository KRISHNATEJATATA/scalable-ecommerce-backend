"""The sender port — how a notification email leaves the system."""

from __future__ import annotations

from typing import Protocol


class NotificationSenderPort(Protocol):
    """Sends one email. Local = SMTP (Mailpit); prod = AWS SES (ECS task role)."""

    async def send(self, *, to: str, subject: str, body: str) -> None:
        """Send one plain-text email to ``to``. Raises on any transport failure."""
        ...
