"""Async Valkey client (redis-py-compatible).

Holds ephemeral shared state only: rate-limit counters, idempotency keys, and
the JWT ``jti`` denylist (TTL = token remaining life). Not for sessions/caching.
Wired in a later phase.
"""

from valkey.asyncio import Valkey

from src.shared.config.setting import AppSettings


def create_client(settings: AppSettings) -> Valkey:
    """Create an async Valkey client from settings (connects lazily)."""
    return Valkey.from_url(settings.valkey_url)


async def ping(client: Valkey) -> bool:
    """Return True if Valkey answers PING."""
    return bool(await client.ping())
