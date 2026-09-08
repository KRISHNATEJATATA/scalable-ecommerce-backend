"""Phase 1 smoke test: app factory boots and the probe/metric routes behave.

Uses ``httpx.AsyncClient`` over the ASGI app directly (no network, no lifespan),
so ``/v1/ready`` sees no wired pools and must report 503.
"""

import asyncio
import logging
import time

import httpx
from sqlalchemy.orm.exc import StaleDataError

from src.app import create_app
from src.shared.config.setting import AppSettings
from src.shared.errors.exceptions import ConcurrentUpdateError

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


async def test_ready_is_503_problem_without_pools():
    async with _client() as client:
        resp = await client.get("/v1/ready")
    assert resp.status_code == 503
    assert resp.headers["content-type"].startswith("application/problem+json")  # the documented contract
    body = resp.json()
    assert body["title"] == "Service Unavailable"
    assert {"dependency": "postgres", "reachable": False} in body["details"]
    assert {"dependency": "valkey", "reachable": False} in body["details"]


async def test_valkey_outage_gates_readiness():
    """Valkey is functional state now (carts, idempotency fast path, dedup): it gates.
    A live-but-failing client (not just an absent one) must 503 the probe."""

    class _DeadValkey:
        async def ping(self):
            raise OSError("valkey down")

    async def _ok(_engine=None):
        return True

    app = create_app(SETTINGS)
    app.state.db_engine = object()
    app.state.valkey = _DeadValkey()  # a real outage: client exists, ping fails
    import src.shared.api.health as health
    import src.shared.clients.postgres_client as pg

    original_pg, original_vk = pg.ping, health.valkey_client.ping
    pg.ping = _ok
    health.valkey_client.ping = lambda client: client.ping()  # surface the real raise
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.get("/v1/ready")
    finally:
        pg.ping, health.valkey_client.ping = original_pg, original_vk

    assert resp.status_code == 503  # ALB deregisters the task
    body = resp.json()
    assert body["title"] == "Service Unavailable"
    assert {"dependency": "valkey", "reachable": False} in body["details"]


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
    assert resp.headers["content-type"].startswith("application/problem+json")
    failed = {d["dependency"] for d in resp.json()["details"]}
    assert failed == {"postgres", "valkey"}


async def test_metrics_exposed():
    async with _client() as client:
        resp = await client.get("/metrics")
    assert resp.status_code == 200
    assert "text/plain" in resp.headers["content-type"]


async def test_app_runs_with_no_prometheus_and_no_database():
    """Prometheus is optional, full stop.

    The metrics are a pull model: the app registers counters in-process and
    serves text on ``/metrics`` — no Prometheus server, client, or exporter
    dependency anywhere in the request path. The outbox-lag poller in the
    lifespan must likewise survive a database it cannot reach (telemetry
    boundary: a failed refresh keeps last values and retries), so a broken DB
    degrades the numbers, never the app.
    """
    import asyncio
    from contextlib import suppress

    import pytest
    from prometheus_client import generate_latest
    from starlette.testclient import TestClient

    from src.shared.bus.metrics import poll_outbox_lag, update_outbox_lag

    settings = SETTINGS.model_copy(
        update={"keycloak_jwks_url": "http://localhost:8080/realms/ecommerce/protocol/openid-connect/certs"}
    )
    app = create_app(settings)
    with TestClient(app) as client:  # exercises the lifespan incl. the poll task
        resp = client.get("/metrics")
        assert resp.status_code == 200
        assert b"http_requests_total" in resp.content
        assert b"outbox_lag_seconds" in resp.content
        assert b"checkout_attempts_total" in resp.content
    # (Task shutdown is exercised by TestClient's own lifespan exit: the context
    # would hang at teardown if the poll task ignored cancellation.)

    # And with the DB unreachable, the refresh raises but the poll loop — the
    # telemetry boundary — logs and retries instead of dying with the DB.
    class _BrokenMaker:
        def __call__(self):
            raise RuntimeError("db unreachable")

    with pytest.raises(RuntimeError):
        await update_outbox_lag(_BrokenMaker())  # the raw refresh propagates

    poll = asyncio.create_task(poll_outbox_lag(_BrokenMaker(), 0.01))
    await asyncio.sleep(0.05)  # several failed passes
    poll.cancel()
    with suppress(asyncio.CancelledError):
        await poll
    assert generate_latest()  # the registry still renders


async def test_generated_openapi_documents_errors_as_rfc9457_problems():
    """Every 4xx/5xx in ``/openapi.json`` must be ``application/problem+json``.

    FastAPI documents its own validation failure as ``application/json`` with an
    ``HTTPValidationError`` body, but ``register_exception_handlers`` answers every
    error with a flat Problem Details document. A generated client would parse the
    wrong content type and look for fields that are never sent.
    """
    schema = create_app(SETTINGS).openapi()

    checked = 0
    for operations in schema["paths"].values():
        for operation in operations.values():
            if not isinstance(operation, dict):
                continue
            for status, response in operation.get("responses", {}).items():
                if not (status.isdigit() and int(status) >= 400):
                    continue
                assert list(response["content"]) == ["application/problem+json"], status
                assert response["content"]["application/problem+json"]["schema"] == {
                    "$ref": "#/components/schemas/Problem"
                }
                checked += 1

    assert checked  # a spec with no documented errors would pass vacuously
    assert "Problem" in schema["components"]["schemas"]
    assert "HTTPValidationError" not in schema["components"]["schemas"]


async def test_openapi_override_keeps_app_metadata():
    """Rewriting the error responses must not cost the rest of the document.

    Re-implementing ``get_openapi(...)`` with a hand-copied argument list drops
    whatever isn't copied — ``servers``, ``openapi_tags``, ``webhooks``,
    ``summary``, ``separate_input_output_schemas``. None are set today, so the loss
    would be silent until someone sets one on ``create_app``. This test sets them
    after the fact and asserts they survive alongside the Problem rewrite.
    """
    app = create_app(SETTINGS)
    app.servers = [{"url": "https://api.example.test", "description": "prod"}]
    app.openapi_tags = [{"name": "catalog", "description": "products"}]
    app.summary = "E-commerce API"
    app.openapi_schema = None  # drop whatever the factory may have cached

    schema = app.openapi()

    assert schema["servers"] == app.servers
    assert schema["tags"] == app.openapi_tags
    assert schema["info"]["summary"] == "E-commerce API"
    assert schema["info"]["title"] == app.title
    assert schema["openapi"] == app.openapi_version
    assert "Problem" in schema["components"]["schemas"]  # ...and the rewrite still ran


async def test_optimistic_lock_conflicts_are_409_problems_not_500s():
    """Both the translated error *and* a raw SQLAlchemy ``StaleDataError`` map to 409.

    A lost update is the optimistic lock working, so the caller should be told to
    re-read and retry — not handed an opaque 500 from the boundary handler. The
    ``StaleDataError`` arm is the backstop for a module whose adapter forgets to
    translate; it logs at **WARNING** because reaching it is a defect either way —
    SQLAlchemy also raises ``StaleDataError`` for an ORM write that matched an
    unexpected row count, which is our bug, not contention.
    """
    app = create_app(SETTINGS)

    async def conflict() -> None:
        raise ConcurrentUpdateError("product")

    async def untranslated() -> None:
        raise StaleDataError("UPDATE matched 0 rows")

    app.add_api_route("/_test/conflict", conflict)
    app.add_api_route("/_test/untranslated", untranslated)

    # The app's logging config doesn't propagate to root, so capture at the source.
    records: list[logging.LogRecord] = []
    handler = logging.Handler()
    handler.emit = records.append
    logger = logging.getLogger("src.shared.errors.exception_handlers")
    logger.addHandler(handler)

    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            for path in ("/_test/conflict", "/_test/untranslated"):
                records.clear()
                resp = await client.get(path)
                assert resp.status_code == 409, (path, resp.text)
                assert resp.headers["content-type"].startswith("application/problem+json"), path
                assert resp.json()["title"] == "Conflict"
                assert "retry" in resp.json()["detail"]
                # The untranslated arm must be alertable; the translated one is routine.
                assert [r.levelno for r in records] == ([logging.WARNING] if path.endswith("untranslated") else []), (
                    path
                )
    finally:
        logger.removeHandler(handler)


async def test_generated_openapi_matches_what_a_product_patch_accepts():
    """The live ``/openapi.json`` must describe the patch rules the API enforces.

    Two ways the generated schema drifted from the hand-authored contract and the
    runtime: it allowed ``{}`` (no ``minProperties``), and it advertised ``null``
    for ``name``/``price`` — both of which the API answers with a 422. A client
    generated from the runtime spec would send them believing they were valid.
    """
    schema = create_app(SETTINGS).openapi()["components"]["schemas"]["ProductUpdate"]

    assert schema["minProperties"] == 1
    for field in ("name", "price"):
        prop = schema["properties"][field]
        variants = prop.get("anyOf", [prop])
        assert "null" not in [v.get("type") for v in variants], field
        assert "default" not in prop, field  # a null default reintroduces the claim
    # ...while genuinely nullable columns keep their null branch.
    assert "null" in [v.get("type") for v in schema["properties"]["description"]["anyOf"]]
