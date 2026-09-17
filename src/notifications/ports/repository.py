"""The notification repository port — recipients, suppressions, the sent backstop."""

from __future__ import annotations

import uuid
from typing import Protocol


class NotificationRepositoryPort(Protocol):
    """The state the notification consumer needs, in this module's own schema."""

    async def upsert_recipient(self, user_id: uuid.UUID, email: str) -> None:
        """Materialize ``UserCreated`` (user_id → email); a re-delivery refreshes the email."""
        ...

    async def get_recipient_email(self, user_id: uuid.UUID) -> str | None:
        """The email known for ``user_id`` (``None`` = never seen — the caller raises for redrive)."""
        ...

    async def is_suppressed(self, email: str) -> bool:
        """Whether ``email`` is on the suppression list (hard bounce / spam complaint)."""
        ...

    async def has_sent(self, order_id: uuid.UUID, email_type: str) -> bool:
        """Whether a confirmation was already recorded for the order (the dedupe-TTL backstop)."""
        ...

    async def record_sent(self, order_id: uuid.UUID, email_type: str, recipient: str) -> bool:
        """Record one sent confirmation; ``False`` when the UNIQUE backstop says it exists."""
        ...
