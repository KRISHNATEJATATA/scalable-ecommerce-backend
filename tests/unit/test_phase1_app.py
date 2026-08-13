"""Phase 1 smoke test: app factory boots and the probe/metric routes behave.

Uses ``httpx.AsyncClient`` over the ASGI app directly (no network, no lifespan),
so ``/v1/ready`` sees no wired pools and must report 503.
"""

import asyncio
import time

import httpx

from src.app import create_app
from src.shared.config.setting import AppSettings

SETTINGS = AppSettings(
    _env_file=None,
    database_url="postgresql+asyncpg://u:p@localhost:5432/db",
)


def _client() -> httpx.AsyncClient:
    app = create_app(SETTINGS)
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")


async def test_health_is_200():
    async with _client() as client:
        resp = await client.get("/v1/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}
    assert resp.headers["X-Request-ID"]  # request-id middleware ran
    assert resp.headers["X-Content-Type-Options"] == "nosniff"


async def test_ready_is_503_without_pools():
    async with _client() as client:
        resp = await client.get("/v1/ready")
    assert resp.status_code == 503
    body = resp.json()
    assert body["status"] == "not ready"
    assert body["checks"] == {"postgres": False, "valkey": False}


async def test_valkey_outage_is_degraded_not_unready():
    """Valkey must never gate readiness — the app falls through to the DB without it."""

    class _FakeEngine:
        async def connect(self):
            raise AssertionError("patched out")

    app = create_app(SETTINGS)
    app.state.db_engine = _FakeEngine()
    app.state.valkey = None  # unreachable cache
    import src.shared.clients.postgres_client as pg

    original = pg.ping
    pg.ping = lambda engine: _ok()

    async def _ok():
        return True

    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.get("/v1/ready")
    finally:
        pg.ping = original

    assert resp.status_code == 200  # ALB keeps the task in service
    assert resp.json() == {"status": "degraded", "checks": {"postgres": True, "valkey": False}}


async def test_ready_probes_are_deadline_bounded():
    """A blackholed dependency must not hang the probe past the ALB's own timeout."""
    import src.shared.api.health as health
    import src.shared.clients.postgres_client as pg

    class _Blackhole:
        async def ping(self):
            await asyncio.sleep(30)  # accepts, never answers

    settings = SETTINGS.model_copy(update={"readiness_probe_timeout_seconds": 0.05})
    app = create_app(settings)
    app.state.db_engine = object()
    app.state.valkey = _Blackhole()

    async def _hang(_engine):
        await asyncio.sleep(30)

    original_pg, original_vk = pg.ping, health.valkey_client.ping
    pg.ping = _hang
    health.valkey_client.ping = lambda client: client.ping()
    started = time.monotonic()
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.get("/v1/ready")
    finally:
        pg.ping, health.valkey_client.ping = original_pg, original_vk

    assert time.monotonic() - started < 5  # bounded, not hung on either dependency
    assert resp.status_code == 503
    assert resp.json()["checks"] == {"postgres": False, "valkey": False}


async def test_metrics_exposed():
    async with _client() as client:
        resp = await client.get("/metrics")
    assert resp.status_code == 200
    assert "text/plain" in resp.headers["content-type"]
