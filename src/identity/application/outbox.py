"""Shared builder for identity outbox rows.

Mirrors ``catalog``/``inventory``: the application layer owns the event envelope,
the repository only persists it — in the same transaction as the state change.

Handed to :meth:`IdentityRepository.get_or_create` as a factory because only the
repository knows whether the JIT upsert actually *inserted* a row; a concurrent
retry that lost the ``ON CONFLICT`` race must not re-announce a creation.
"""

from __future__ import annotations

import uuid

from src.events.models import UserCreated, UserCreatedData
from src.shared.config.logging import request_id_ctx
from src.shared.db.outbox import OutboxMessage


def user_created_outbox(user_id: uuid.UUID, email: str) -> OutboxMessage:
    """Build the ``UserCreated`` outbox message for a freshly JIT-provisioned user."""
    event = UserCreated(
        trace_id=request_id_ctx.get() or str(uuid.uuid4()),
        data=UserCreatedData(user_id=user_id, email=email),
    )
    return OutboxMessage(event.type, event.model_dump_json())
