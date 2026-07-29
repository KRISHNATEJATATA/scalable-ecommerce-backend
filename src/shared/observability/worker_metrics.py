"""Prometheus export for **worker** processes.

The API serves `/metrics` from its own process (`src/shared/api/metrics.py`), so
anything incremented in a worker — relay, cache worker, image worker, reservation
reaper — is invisible to Prometheus by default. Workers come in two shapes and
need different answers:

* **Long-running** (compose services, always-on ECS services). Give them a scrape
  endpoint: ``WORKER_METRICS_PORT`` starts ``prometheus_client``'s tiny WSGI
  server on that port and Prometheus pulls it like any other target.
* **Short-lived** (``--once`` runs on an EventBridge schedule). A pull endpoint is
  useless — the process is gone before the next scrape. These push to a
  **Pushgateway** at exit (``METRICS_PUSHGATEWAY_URL``), which is exactly the
  batch-job case the Pushgateway exists for.

Both are stock ``prometheus_client``, no new dependency. Both are **opt-in**:
unset means off, so tests and local runs neither bind a port nor make a network
call.

A pushed counter still can't tell you the reaper is *dead* — a process that never
runs pushes nothing. Liveness stays an alert on the expired-hold backlog in
Postgres (`docs/RUNBOOK.md` §8); these metrics report what the worker *did*.
"""

from __future__ import annotations

import logging

from prometheus_client import REGISTRY, push_to_gateway, start_http_server

from src.shared.config.setting import AppSettings

log = logging.getLogger(__name__)


def serve_worker_metrics(settings: AppSettings, job: str) -> None:
    """Expose this worker's metrics for scraping, if a port is configured.

    Call once at worker startup. ``job`` names the worker in logs and matches the
    Pushgateway job used by :func:`push_worker_metrics`.
    """
    port = settings.worker_metrics_port
    if port is None:
        return
    start_http_server(port)
    log.info("worker metrics listening on :%d (job=%s)", port, job)


def push_worker_metrics(settings: AppSettings, job: str) -> None:
    """Push this run's metrics to the Pushgateway, if one is configured.

    For ``--once`` batch runs, called at exit. A failed push must never fail the
    run: the work already committed, and losing a sample is not worth re-running a
    reaper sweep over — hence the broad catch at this process boundary.
    """
    url = settings.metrics_pushgateway_url
    if url is None:
        return
    try:
        push_to_gateway(url, job=job, registry=REGISTRY)
    except Exception:  # boundary: telemetry must not break the job
        log.warning("could not push metrics to %s (job=%s)", url, job, exc_info=True)
