"""Per-endpoint RED metrics: rate/errors under the route template, duration.

Verified over the ASGI app directly (no lifespan, no network) — the middleware
is pure ASGI, so the guarantees under test (path template label, unmatched
404s, /metrics exclusion) are observable from the registry. A dedicated test
route is registered per test (the same seam ``test_phase1_app`` uses) because
every real data route requires the lifespan-owned DB pool.
"""

import uuid

import httpx
from prometheus_client import REGISTRY

from src.app import create_app
from src.shared.config.setting import AppSettings

SETTINGS = AppSettings(
    _env_file=None,
    database_url="postgresql+asyncpg://u:p@localhost:5432/db",
)


def _app():
    app = create_app(SETTINGS)

    async def _detail(order_id: uuid.UUID) -> dict:
        return {"id": str(order_id)}

    app.add_api_route("/_test/items/{order_id}", _detail, methods=["GET"])
    return app


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=_app()), base_url="http://test")


def _counter_value(name: str, **labels: str) -> float:
    """The current value of one labeled series, 0 if never incremented."""
    value = REGISTRY.get_sample_value(name, labels)
    return 0.0 if value is None else value


def _histogram_count(**labels: str) -> float:
    value = REGISTRY.get_sample_value("http_request_duration_seconds_count", labels)
    return 0.0 if value is None else value


async def test_requests_counted_under_route_template():
    """Two different order ids must land under one path label — the template,
    never the raw URL (URL ids are unbounded cardinality)."""
    before = _counter_value("http_requests_total", method="GET", path="/_test/items/{order_id}", status="200")
    async with _client() as client:
        await client.get(f"/_test/items/{uuid.uuid4()}")
        await client.get(f"/_test/items/{uuid.uuid4()}")
    after = _counter_value("http_requests_total", method="GET", path="/_test/items/{order_id}", status="200")
    assert after - before == 2


async def test_unmatched_path_is_labeled_not_the_raw_url():
    before = _counter_value("http_requests_total", method="GET", path="unmatched", status="404")
    async with _client() as client:
        await client.get("/v1/definitely-not-a-route-xyz")
    assert _counter_value("http_requests_total", method="GET", path="unmatched", status="404") == before + 1


async def test_metrics_path_is_not_self_instrumented():
    before = _counter_value("http_requests_total", method="GET", path="/metrics", status="200")
    async with _client() as client:
        resp = await client.get("/metrics")
    assert resp.status_code == 200
    assert _counter_value("http_requests_total", method="GET", path="/metrics", status="200") == before


async def test_duration_is_observed_per_template():
    before = _histogram_count(method="GET", path="/_test/items/{order_id}")
    async with _client() as client:
        await client.get(f"/_test/items/{uuid.uuid4()}")
    assert _histogram_count(method="GET", path="/_test/items/{order_id}") == before + 1


async def test_scrape_output_contains_red_series():
    async with _client() as client:
        await client.get(f"/_test/items/{uuid.uuid4()}")
        text = (await client.get("/metrics")).text
    assert "http_requests_total{method=" in text
    assert "http_request_duration_seconds_count" in text
    assert 'path="/_test/items/{order_id}"' in text
