"""Async Valkey client (redis-py-compatible).

Holds ephemeral shared state: rate-limit counters, idempotency keys, event-dedup
keys, and the **product read-cache** (cache-aside entries + fill-locks). Not in
the auth path — revocation is Keycloak's short token TTL, not
an app-side ``jti`` denylist. Wired into the app lifespan (``src/app.py``).
"""

from valkey.asyncio import Valkey

from src.shared.config.setting import AppSettings


def create_client(settings: AppSettings) -> Valkey:
    """Create an async Valkey client from settings (connects lazily)."""
    return Valkey.from_url(settings.valkey_url)


async def ping(client: Valkey) -> bool:
    """Return True if Valkey answers PING."""
    return bool(await client.ping())
