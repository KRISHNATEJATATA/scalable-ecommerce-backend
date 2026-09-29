"""Pool-status gauges for every SQLAlchemy engine in the process.

Every ECS process builds its own pool, and they all draw on the same RDS
``max_connections`` (the budget formula in ``docs/DEPLOYMENT.md``). Until these
gauges existed the only signal for connection pressure was the outage itself —
you cannot pin autoscaling to a budget you cannot see.

One process-wide collector reads every registered pool **at scrape time**
(``collect()`` runs when Prometheus pulls ``/metrics``, a worker's
``WORKER_METRICS_PORT`` server, or a ``--once`` run pushes to the Pushgateway),
so there is no background thread and no staleness window. A single collector
rather than one per pool because the registry rejects duplicate timeseries
*names* — two collectors both yielding ``db_pool_connections_capacity`` would
collide even under different labels. Registering a label twice (tests and app
startups re-create engines constantly) just swaps the engine it points at.
"""

from __future__ import annotations

import logging
from typing import cast

from prometheus_client.core import REGISTRY, GaugeMetricFamily
from prometheus_client.registry import Collector
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.pool import QueuePool

log = logging.getLogger(__name__)


class _PoolCollector(Collector):
    """Scrape-time snapshot of every pool registered in this process."""

    def __init__(self) -> None:
        self._pools: dict[str, tuple[AsyncEngine, int]] = {}

    def add(self, label: str, engine: AsyncEngine, capacity: int) -> None:
        self._pools[label] = (engine, capacity)

    def collect(self):  # noqa: D102 — prometheus_client calls this per scrape
        capacity = GaugeMetricFamily(
            "db_pool_connections_capacity",
            "Configured max connections for this pool (pool_size + max_overflow) — "
            "the per-process term in the RDS connection budget.",
            labels=["pool"],
        )
        checked_out = GaugeMetricFamily(
            "db_pool_connections_checked_out",
            "Connections currently in use by this pool.",
            labels=["pool"],
        )
        open_connections = GaugeMetricFamily(
            "db_pool_connections_open",
            "Connections this pool currently holds open (checked out + checked in).",
            labels=["pool"],
        )
        overflow = GaugeMetricFamily(
            "db_pool_connections_overflow",
            "Connections open beyond pool_size (never negative) — sustained non-zero means pool_size is undersized.",
            labels=["pool"],
        )
        for label, (engine, pool_capacity) in self._pools.items():
            try:
                # Every engine this app creates is a QueuePool (see postgres_client);
                # size()/overflow() aren't on the Pool base type.
                pool = cast(QueuePool, engine.sync_engine.pool)
                checked_out_count = pool.checkedout()
                # size() is the *configured* pool_size, not a live count; and raw
                # overflow() reads negative below full base occupancy — clamped so
                # "non-zero = living in overflow" is literally true.
                open_count = pool.checkedout() + pool.checkedin()
                overflow_count = max(0, pool.overflow())
            except Exception:  # boundary: a disposed engine must never break a scrape
                log.debug("pool %s not collectable (disposed?)", label, exc_info=True)
                continue
            capacity.add_metric([label], pool_capacity)
            checked_out.add_metric([label], checked_out_count)
            open_connections.add_metric([label], open_count)
            overflow.add_metric([label], overflow_count)
        return [capacity, checked_out, open_connections, overflow]


_collector = _PoolCollector()
_registered = False


def register_pool_metrics(label: str, engine: AsyncEngine, *, capacity: int) -> None:
    """Expose ``engine``'s pool status under the ``pool=<label>`` label.

    ``capacity`` is the pool's configured maximum (``pool_size + max_overflow``)
    — the number the connection-budget formula counts, which SQLAlchemy does not
    expose back out of a live pool. Re-registering a label replaces the engine
    it points at, so engine re-creation (tests, lifespan restarts) never goes
    stale. One process holds at most one pool per label (``api``/``worker``/
    ``probe``).
    """
    global _registered
    _collector.add(label, engine, capacity)
    if not _registered:
        REGISTRY.register(_collector)
        _registered = True
