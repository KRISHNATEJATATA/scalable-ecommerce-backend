"""
Health check endpoints.

Two probes with distinct responsibilities:
  - GET /v1/health  → liveness:   is the process alive?  (always 200)
  - GET /v1/ready   → readiness:  is Postgres reachable? (200 / 503)

Only Postgres gates readiness. Valkey is reported in the body (``degraded``) but
never 503s: the app is built to fall through to the DB without it, so gating on
it would let a cache outage deregister every task.
"""

import asyncio
import logging

from fastapi import APIRouter, Request, Response, status

from src.shared.clients import postgres_client, valkey_client

logger = logging.getLogger(__name__)

router = APIRouter(tags=["health"])


@router.get("/health")
async def health() -> dict[str, str]:
    """Liveness: the process is up. No dependency checks."""
    return {"status": "ok"}


@router.get("/ready")
async def ready(request: Request, response: Response) -> dict[str, object]:
    """Readiness: Postgres is the only hard gate; Valkey is reported, not gating.

    The app degrades cleanly without Valkey — ``container.get_product_cache``
    returns ``None`` and the catalog service falls through to Postgres — so
    failing readiness on a Valkey blip would have the ALB deregister every task
    and turn a cache outage into a full outage. Postgres has no such fallback.

    Both probes are **deadline-bounded**: a blackholed dependency accepts the TCP
    connection and never answers, so an unbounded ping would hang past the ALB's
    own health-check timeout and the probe would be scored as a timeout anyway —
    only with a request worker pinned for the duration. The Postgres probe runs on
    a dedicated pool-free engine so a saturated request pool is never reported as
    a dead database.
    """
    timeout = request.app.state.settings.readiness_probe_timeout_seconds
    state = request.app.state

    engine = getattr(state, "db_probe_engine", None) or getattr(state, "db_engine", None)
    try:
        async with asyncio.timeout(timeout):
            postgres_ok = bool(engine) and await postgres_client.ping(engine)
    except Exception:  # includes TimeoutError from the deadline above
        logger.warning("Readiness: Postgres ping failed or timed out (>%ss)", timeout)
        postgres_ok = False

    valkey = getattr(state, "valkey", None)
    try:
        async with asyncio.timeout(timeout):
            valkey_ok = bool(valkey) and await valkey_client.ping(valkey)
    except Exception:
        logger.warning("Readiness: Valkey ping failed or timed out (degraded, not gating)")
        valkey_ok = False

    checks = {"postgres": postgres_ok, "valkey": valkey_ok}
    response.status_code = status.HTTP_200_OK if postgres_ok else status.HTTP_503_SERVICE_UNAVAILABLE
    if not postgres_ok:
        status_text = "not ready"
    else:
        status_text = "ready" if valkey_ok else "degraded"
    return {"status": status_text, "checks": checks}
