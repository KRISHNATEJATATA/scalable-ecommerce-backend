"""The notification repository port — recipients, the order→email projection,
suppressions, the durable send state."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Literal, Protocol

# The outcome of claiming the durable send state for one (order_id, email_type):
# ``claimed`` — the row is ours, status ``pending``; go send.
# ``already_sent`` — a prior attempt recorded ``sent``; ack, never resend.
# ``takeover`` — a prior attempt claimed but never marked (the crash window);
# resend with the SAME persisted message_id and count the recovery.
ClaimOutcome = Literal["claimed", "already_sent", "takeover"]


@dataclass(frozen=True)
class ClaimResult:
    """The claim outcome plus the ONE message_id this send ever carries:
    generated (uuid4) at claim time, persisted on the row, and returned on
    every later claim so a takeover resend reuses it."""

    outcome: ClaimOutcome
    message_id: uuid.UUID


class NotificationRepositoryPort(Protocol):
    """The state the notification consumer needs, in this module's own schema."""

    async def upsert_recipient(self, user_id: uuid.UUID, email: str) -> None:
        """Materialize ``UserCreated`` (user_id → email); a re-delivery refreshes the email."""
        ...

    async def upsert_order_email(self, order_id: uuid.UUID, email: str) -> None:
        """Materialize ``OrderPlaced`` (order_id → email) — the Order Recipient
        projection, the refund notice's address source. Written from the event
        the worker already consumes, before any send decision; a re-delivery
        refreshes the email."""
        ...

    async def get_order_email(self, order_id: uuid.UUID) -> str | None:
        """The email known for ``order_id`` (the Order Recipient) — how a
        ``PaymentRefunded`` (which carries no address) resolves its recipient.
        ``None`` = never seen; the caller raises for redrive → DLQ."""
        ...

    async def get_recipient_email(self, user_id: uuid.UUID) -> str | None:
        """The email known for ``user_id`` — the FALLBACK address source for an
        ``OrderPlaced`` that doesn't carry one (pre-snapshot events). ``None``
        = never seen; the caller raises for redrive only when the event carried
        no address either."""
        ...

    async def is_suppressed(self, email: str) -> bool:
        """Whether ``email`` is on the suppression list (hard bounce / spam complaint)."""
        ...

    async def has_sent(self, order_id: uuid.UUID, email_type: str) -> bool:
        """Whether an email of this type is recorded ``sent`` for the order."""
        ...

    async def claim_send(self, order_id: uuid.UUID, email_type: str, recipient: str) -> ClaimResult:
        """Claim the durable send state BEFORE the send: insert the ``pending``
        row (generating its uuid4 message_id) and commit, so a crash mid-send
        leaves a visible, reconcilable record rather than an invisible
        duplicate. On the UNIQUE conflict, returns the row's persisted id."""
        ...

    async def mark_sent(self, order_id: uuid.UUID, email_type: str, recipient: str) -> None:
        """Flip a claimed row to ``sent`` after the sender accepted the message."""
        ...
