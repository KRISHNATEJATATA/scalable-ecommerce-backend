"""Metrics endpoint + per-endpoint RED metrics.

``/metrics`` serves the default ``prometheus_client`` registry (text
exposition). The :class:`MetricsMiddleware` middleware records **RED** per
endpoint — Rate (``http_requests_total``), Errors (the ``status`` label on the
same counter) and Duration (``http_request_duration_seconds``) — for every
request the API process handles.

The ``path`` label is the **route template** (e.g. ``/v1/orders/{order_id}``),
never the raw path: URL ids must not become label values, or every order in
history would be its own time series. FastAPI's ``APIRoute`` stashes itself on
``scope["route"]`` during routing, so the middleware reads the template after
the call; unmatched requests (404s) are labeled ``unmatched``. ``/metrics``
itself is excluded — the scrape is instrumentation feedback, not traffic.

Pure ASGI (not ``BaseHTTPMiddleware``) on purpose: counting needs only the
status from the wrapped ``send`` and the elapsed time, no response object.
"""

from __future__ import annotations

import time

from fastapi import APIRouter, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from starlette.types import ASGIApp, Message, Receive, Scope, Send

router = APIRouter(tags=["metrics"])

#: RED "Rate" + "Errors": one increment per request; alerts split on ``status``.
REQUESTS_TOTAL = Counter(
    "http_requests_total",
    "HTTP requests by method, route template and status code.",
    ["method", "path", "status"],
)

#: RED "Duration": per-endpoint latency distribution (default buckets cover
#: DB- and gateway-bound calls, 5ms..10s).
REQUEST_DURATION = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration by method and route template.",
    ["method", "path"],
)


@router.get("/metrics")
async def metrics() -> Response:
    """Expose the default Prometheus registry in text exposition format."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


class MetricsMiddleware:
    """Count every HTTP request (rate/status/duration) under its route template."""

    def __init__(self, app: ASGIApp) -> None:
        self._app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or scope["path"] == "/metrics":
            await self._app(scope, receive, send)
            return
        method = scope["method"]
        started = time.perf_counter()
        status: int | None = None

        async def observe(message: Message) -> None:
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
            await send(message)

        try:
            await self._app(scope, receive, observe)
        finally:
            # `route` is set in the shared scope dict by FastAPI's APIRoute when
            # one matched — read it after the call, not before routing.
            route = scope.get("route")
            path = getattr(route, "path", "unmatched")
            REQUESTS_TOTAL.labels(method, path, status if status is not None else 500).inc()
            REQUEST_DURATION.labels(method, path).observe(time.perf_counter() - started)
