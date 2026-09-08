"""Health check endpoints.

Two probes with distinct responsibilities:
  - GET /v1/health  → liveness:   is the process alive?  (always 200, no deps)
  - GET /v1/ready   → readiness:  are Postgres, Valkey and the bus reachable?

Every dependency **gates** readiness — the 503 body is the one RFC 9457 Problem
shape (matching the hand-authored contract, which already declared
``application/problem+json``). The API is not "truly ready" without its deps:
carts, the checkout idempotency fast path and event dedup live in Valkey, and
outbox shipping needs the bus. (This supersedes the earlier degraded-not-gating
stance: with cart state in Valkey, a Valkey outage is a functional outage for
shoppers, not a graceful degradation.)

All checks run **concurrently** and **deadline-bounded**: a blackholed
dependency accepts the TCP connection and never answers, so an unbounded ping
would hang past the ALB's own health-check timeout and be scored as a timeout
anyway — only with a request worker pinned for the duration. The Postgres probe
runs on a dedicated pool-free engine so a saturated request pool is never
reported as a dead database. The bus probe is skipped (key omitted) when no bus
client exists — a bare test app or a local dev run without LocalStack.
"""

import asyncio
import logging
from collections.abc import Coroutine
from typing import Any

from fastapi import APIRouter, Request, Response, status
from fastapi.responses import JSONResponse

from src.shared.clients import postgres_client, valkey_client
from src.shared.errors.error_builder import PROBLEM_CONTENT_TYPE, build_problem

logger = logging.getLogger(__name__)

router = APIRouter(tags=["health"])


@router.get("/health")
async def health() -> dict[str, str]:
    """Liveness: the process is up. No dependency checks (kills zombies, not outages)."""
    return {"status": "ok"}


@router.get(
    "/ready",
    responses={status.HTTP_503_SERVICE_UNAVAILABLE: {"description": "A critical dependency is unreachable."}},
)
async def ready(request: Request) -> Response:
    """Readiness: Postgres, Valkey and (when configured) the bus gate traffic.

    Returns the ``{status, checks}`` body on 200, and the flat RFC 9457 Problem
    on 503 with the failed dependencies named in ``details``.
    """
    timeout = request.app.state.settings.readiness_probe_timeout_seconds
    state = request.app.state

    engine = getattr(state, "db_probe_engine", None) or getattr(state, "db_engine", None)
    valkey = getattr(state, "valkey", None)
    bus = getattr(state, "bus_sqs", None)

    # All probes at once: the readiness deadline is per-check, but the request
    # worker is pinned for the *sum* — serial checks would double the worst-case
    # probe latency toward the ALB's own health-check timeout.
    probes: list[Coroutine[Any, Any, bool]] = [
        _postgres_ready(engine, timeout),
        _valkey_ready(valkey, timeout),
    ]
    if bus is not None:  # only probed where a bus client exists (omitted → not checked)
        probes.append(_bus_ready(bus, timeout))
    results = await asyncio.gather(*probes)

    checks: dict[str, bool] = {"postgres": results[0], "valkey": results[1]}
    if bus is not None:
        checks["bus"] = results[2]

    if not all(checks.values()):
        problem = build_problem(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            title="Service Unavailable",
            detail="one or more critical dependencies are unreachable",
            details=[{"dependency": name, "reachable": False} for name, ok in checks.items() if not ok],
        )
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE, content=problem, media_type=PROBLEM_CONTENT_TYPE
        )
    return JSONResponse(content={"status": "ready", "checks": checks})


async def _postgres_ready(engine: Any, timeout: float) -> bool:
    """Postgres is the one dependency with no in-process fallback."""
    try:
        async with asyncio.timeout(timeout):
            ok = bool(engine) and await postgres_client.ping(engine)
        return bool(ok)
    except Exception:  # includes TimeoutError from the deadline above
        logger.warning("Readiness: Postgres ping failed or timed out (>%ss)", timeout)
        return False


async def _valkey_ready(valkey: Any, timeout: float) -> bool:
    try:
        async with asyncio.timeout(timeout):
            ok = bool(valkey) and await valkey_client.ping(valkey)
        return bool(ok)
    except Exception:
        logger.warning("Readiness: Valkey ping failed or timed out (>%ss)", timeout)
        return False


async def _bus_ready(client: Any, timeout: float) -> bool:
    """Any successful SQS API call proves the bus endpoint is reachable."""
    try:
        async with asyncio.timeout(timeout):
            await client.list_queues(MaxResults=1)
        return True
    except Exception:
        logger.warning("Readiness: bus (SQS) ping failed or timed out (>%ss)", timeout)
        return False
