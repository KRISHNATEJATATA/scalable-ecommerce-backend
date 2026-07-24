"""
Health check endpoints.

Two probes with distinct responsibilities:
  - GET /v1/health  → liveness:   is the process alive?  (always 200)
  - GET /v1/ready   → readiness:  are critical deps reachable?  (200 / 503)
"""

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
    """Readiness: ping Postgres + Valkey; 503 if either is unreachable."""
    checks: dict[str, bool] = {}

    engine = getattr(request.app.state, "db_engine", None)
    try:
        checks["postgres"] = bool(engine) and await postgres_client.ping(engine)
    except Exception:
        logger.warning("Readiness: Postgres ping failed")
        checks["postgres"] = False

    valkey = getattr(request.app.state, "valkey", None)
    try:
        checks["valkey"] = bool(valkey) and await valkey_client.ping(valkey)
    except Exception:
        logger.warning("Readiness: Valkey ping failed")
        checks["valkey"] = False

    ok = all(checks.values())
    response.status_code = status.HTTP_200_OK if ok else status.HTTP_503_SERVICE_UNAVAILABLE
    return {"status": "ready" if ok else "not ready", "checks": checks}
