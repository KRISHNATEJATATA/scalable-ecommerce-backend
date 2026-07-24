"""Phase 1 smoke test: app factory boots and the probe/metric routes behave.

Uses ``httpx.AsyncClient`` over the ASGI app directly (no network, no lifespan),
so ``/v1/ready`` sees no wired pools and must report 503.
"""

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


async def test_metrics_exposed():
    async with _client() as client:
        resp = await client.get("/metrics")
    assert resp.status_code == 200
    assert "text/plain" in resp.headers["content-type"]
