"""Valkey fast path for checkout idempotency — the concrete :class:`IdempotencyPort`.

``idempotency:{user_id}:{key}`` holds ``{body_hash, status, response}`` under a
TTL. A hit with the same body hash replays the stored response without
touching Postgres; a hit with a different hash is a caller bug (409). Eviction
or a Valkey outage only loses the fast path: the composite
``UNIQUE(user_id, idempotency_key)`` plus the stored body hash still prevent a
duplicate order, degrading to re-reading the stored row. Valkey faults degrade
to a miss (get) or a warning (put) — this adapter is a cache, never the truth.
"""

from __future__ import annotations

import json
import logging
import uuid
from typing import Any

from src.orders.ports.checkout import IdempotencyRecord

log = logging.getLogger(__name__)


class ValkeyIdempotencyStore:
    """Implements the saga's ``IdempotencyPort`` over Valkey (imported by the composition root)."""

    def __init__(self, valkey: Any, *, ttl_seconds: int) -> None:
        self._valkey = valkey
        self._ttl = ttl_seconds

    @staticmethod
    def _key(user_id: uuid.UUID, idempotency_key: str) -> str:
        return f"idempotency:{user_id}:{idempotency_key}"

    async def get(self, user_id: uuid.UUID, idempotency_key: str) -> IdempotencyRecord | None:
        """The stored replay answer, or ``None`` on miss, eviction, or Valkey failure."""
        try:
            raw = await self._valkey.get(self._key(user_id, idempotency_key))
        except Exception:  # boundary: Valkey down degrades to the DB backstop
            log.warning("idempotency fast-path read failed; falling back to DB", exc_info=True)
            return None
        if raw is None:
            return None
        try:
            data = json.loads(raw)
            return IdempotencyRecord(body_hash=data["body_hash"], status=int(data["status"]), response=data["response"])
        except (ValueError, KeyError, TypeError):
            log.warning("idempotency fast-path record unreadable; falling back to DB")
            return None

    async def put(
        self,
        user_id: uuid.UUID,
        idempotency_key: str,
        *,
        body_hash: str,
        status: int,
        response: Any,
    ) -> None:
        """Store the replay answer; a Valkey failure is logged, never raised."""
        payload = json.dumps(
            {
                "body_hash": body_hash,
                "status": status,
                "response": response.model_dump(mode="json") if hasattr(response, "model_dump") else response,
            }
        )
        try:
            await self._valkey.set(self._key(user_id, idempotency_key), payload, ex=self._ttl)
        except Exception:  # boundary: losing the fast path must not fail checkout
            log.warning("idempotency fast-path write failed; DB backstop still guards", exc_info=True)
