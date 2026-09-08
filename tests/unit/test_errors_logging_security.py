"""boundary tests: traceback redaction, sanitized 5xx, docs CSP.

Uses ``httpx.AsyncClient`` over the ASGI app directly (no network, no lifespan),
matching ``test_phase1_app.py``.
"""

import logging

import httpx
import pytest
from pydantic import ValidationError

from src.app import create_app
from src.shared.config.logging import RedactFilter
from src.shared.config.setting import AppSettings
from src.shared.errors.error_builder import build_problem
from src.shared.errors.exception_handlers import _status_reason

SETTINGS = AppSettings(_env_file=None, database_url="postgresql+asyncpg://u:p@localhost:5432/db")


def _records() -> tuple[logging.Logger, logging.Handler, list[logging.LogRecord]]:
    records: list[logging.LogRecord] = []
    handler = logging.Handler()
    handler.emit = records.append  # type: ignore[method-assign]
    logger = logging.getLogger("test.ticket")
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    return logger, handler, records


async def test_redactfilter_scrubs_traceback_not_just_message():
    """A poison event failing contract validation logs the offending payload (a
    customer email) inside the pydantic ValidationError text at ERROR level via
    ``log.exception`` — the RedactFilter must drop the whole rendered traceback,
    not only ``record.getMessage()``."""
    logger, handler, records = _records()
    try:
        # Simulate exactly the poison-event path: exception text carrying PII.
        from pydantic import BaseModel

        class _Event(BaseModel):
            email: str

        try:
            _Event.model_validate({"email": {"customer_email": "buyer@example.com"}, "password": "hunter2"})
        except ValidationError as exc:
            logger.exception("event failed contract validation: %s", exc)
        finally:
            logger.removeHandler(handler)

        assert records, "expected at least one record"
        record = records[-1]
        RedactFilter().filter(record)
        rendered = record.getMessage()
        assert "buyer@example.com" not in rendered
        assert "hunter2" not in rendered
        assert "REDACTED" in rendered
        # traceback must not survive via exc_info/exc_text either
        assert record.exc_info is None
        assert record.exc_text is None or "buyer@example.com" not in str(record.exc_text)
    finally:
        logger.removeHandler(handler)


async def test_redactfilter_scrubs_bare_email_and_cached_exc_text():
    """A bare address contains no sensitive KEY substring, and a pre-rendered
    ``exc_text`` (no exc_info — the stdlib formatter caches it) must not leak."""
    flt = RedactFilter()

    bare = logging.LogRecord("t", logging.ERROR, __file__, 1, "receipt to buyer@example.com bounced", (), None)
    flt.filter(bare)
    assert "buyer@example.com" not in bare.getMessage()
    assert "REDACTED" in bare.getMessage()

    cached = logging.LogRecord("t", logging.ERROR, __file__, 1, "boom", (), None)
    cached.exc_text = "boom email=buyer@example.com"  # as a stdlib formatter would cache it
    flt.filter(cached)
    assert cached.exc_info is None
    assert "buyer@example.com" not in str(cached.exc_text)
    assert "REDACTED" in str(cached.exc_text)


async def test_redactfilter_leaves_clean_records_alone():
    logger, handler, records = _records()
    try:
        logger.info("order %s created", "ord_123")
        record = records[-1]
        RedactFilter().filter(record)
        assert record.getMessage() == "order ord_123 created"
    finally:
        logger.removeHandler(handler)


def _client(app, *, raise_app_exceptions: bool = True) -> httpx.AsyncClient:
    # 500-path tests need raise_app_exceptions=False: ServerErrorMiddleware
    # re-raises after sending the sanitized response (real servers log it).
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app, raise_app_exceptions=raise_app_exceptions),
        base_url="http://test",
    )


async def test_5xx_bodies_are_sanitized_by_default():
    """A 500 must never carry raw exception text; internals stay in the logs."""
    app = create_app(SETTINGS)

    async def boom() -> None:
        raise RuntimeError("SELECT * FROM secret_table; driver error 42")

    app.add_api_route("/_test/boom", boom)
    async with _client(app, raise_app_exceptions=False) as client:
        resp = await client.get("/_test/boom")
    assert resp.status_code == 500
    body = resp.json()
    assert body["title"] == "Internal Server Error"
    assert "detail" not in body  # sanitized: generic message only
    assert "secret_table" not in resp.text
    # correlation holds even on the 500 boundary: the handler re-establishes the
    # context the request-id middleware had to reset, and the response header
    # quotes the same id the body stamps.
    assert body["trace_id"]
    assert resp.headers["X-Request-ID"] == body["trace_id"]


async def test_5xx_trace_id_matches_client_supplied_request_id():
    """A client quoting X-Request-ID gets that exact id back on the 500, so the
    support request can be joined to the log record."""
    app = create_app(SETTINGS)

    async def boom() -> None:
        raise RuntimeError("raw internals")

    app.add_api_route("/_test/boom", boom)
    async with _client(app, raise_app_exceptions=False) as client:
        resp = await client.get("/_test/boom", headers={"X-Request-ID": "client-trace-42"})
    assert resp.headers["X-Request-ID"] == "client-trace-42"
    assert resp.json()["trace_id"] == "client-trace-42"


async def test_verbose_5xx_details_are_dev_optin_and_refused_outside_dev():
    """Dev may opt into raw 5xx detail; staging/prod configs are rejected."""
    dev = SETTINGS.model_copy(update={"verbose_error_details": True})
    app = create_app(dev)

    async def boom() -> None:
        raise RuntimeError("raw internals")

    app.add_api_route("/_test/boom", boom)
    async with _client(app, raise_app_exceptions=False) as client:
        resp = await client.get("/_test/boom")
    assert "raw internals" in resp.json()["detail"]

    for env in ("staging", "prod"):
        with pytest.raises(ValueError, match="verbose_error_details"):
            AppSettings(
                _env_file=None,
                database_url="postgresql+asyncpg://u:p@localhost:5432/db",
                environment=env,
                verbose_error_details=True,
            )


async def test_http_exception_maps_to_rfc9457_reason_title_and_detail():
    """`title` is the HTTP reason phrase; the route's text rides in `detail`."""
    app = create_app(SETTINGS)

    async def missing() -> None:
        from fastapi import HTTPException, status

        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="product not found")

    app.add_api_route("/_test/missing", missing)
    async with _client(app) as client:
        resp = await client.get("/_test/missing")
    assert resp.status_code == 404
    body = resp.json()
    assert body["title"] == "Not Found"
    assert body["detail"] == "product not found"
    assert resp.headers["content-type"].startswith("application/problem+json")


async def test_unknown_status_code_gets_generic_title():
    assert _status_reason(599) == "Error"
    assert _status_reason(404) == "Not Found"


async def test_docs_csp_is_relaxed_only_on_docs_routes_outside_prod():
    """Swagger/ReDoc CDNs must load on /docs in non-prod, while the API keeps
    the strict `default-src 'none'` policy — and prod never relaxes."""
    app = create_app(SETTINGS)
    async with _client(app) as client:
        docs = await client.get("/docs")
        api = await client.get("/v1/health")
    strict = "default-src 'none'; frame-ancestors 'none'"
    assert "cdn.jsdelivr.net" in docs.headers["Content-Security-Policy"]  # relaxed on docs
    assert api.headers["Content-Security-Policy"] == strict  # strict on the API

    prod = SETTINGS.model_copy(update={"environment": "prod"})
    prod_app = create_app(prod)
    async with _client(prod_app) as client:
        missing_docs = await client.get("/docs")
        api = await client.get("/v1/health")
    assert missing_docs.status_code == 404  # docs disabled in prod
    assert api.headers["Content-Security-Policy"] == strict  # and never relaxed


def test_build_problem_flat_shape():
    problem = build_problem(400, "Bad Request", detail="x", details=[{"loc": ["q"]}])
    assert problem["type"] == "about:blank"
    assert problem["status"] == 400
    assert problem["title"] == "Bad Request"
    assert problem["detail"] == "x"
    assert problem["details"] == [{"loc": ["q"]}]
    assert "trace_id" in problem
