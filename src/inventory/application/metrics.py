"""Inventory Prometheus counters.

Registered on the default ``prometheus_client`` registry, so the API's
``/metrics`` (``src/shared/api/metrics.py``) exposes the ones incremented in the
API process, and ``src/shared/observability/worker_metrics.py`` exports the ones
incremented in the reaper process (scrape port when looping, Pushgateway when
running ``--once``).

* ``inventory_oversell_blocked_total`` — every rejected reservation, i.e. the
  atomic conditional decrement doing its job. A spike means real contention on a
  hot SKU (or a saga retry storm), not a bug. Deliberately *excludes* duplicate
  order lines, which raise ``ReservationConflictError`` instead — counting a
  caller's own bookkeeping mistake as an oversell block would hide the signal.
* ``inventory_reservation_conflict_total`` — retries of an order line with a
  changed quantity. A caller bug, not contention.
* ``inventory_reaper_released_total`` — holds reclaimed from stalled sagas. A
  persistently non-zero rate means checkouts are dying mid-flight upstream.

**This counter is not the reaper's liveness alert.** A dead reaper increments
nothing and pushes nothing, so silence here is ambiguous. Liveness is the
expired-hold backlog query in `docs/RUNBOOK.md` §8; this counter tells you the
volume of stalled checkouts once you know the reaper is running.
"""

from __future__ import annotations

from prometheus_client import Counter

oversell_blocked_total = Counter(
    "inventory_oversell_blocked_total",
    # No `sku` label on purpose: SKU is unbounded cardinality and would blow up the
    # time series. The rejected SKU is on the log line; the counter is the alertable signal.
    "Reservation attempts rejected because free stock did not cover the requested quantity.",
)

reservation_conflict_total = Counter(
    "inventory_reservation_conflict_total",
    "Reservation retries rejected because the order line already holds a different quantity.",
)

reaper_released_total = Counter(
    "inventory_reaper_released_total",
    "Expired reservations released back to available stock by the reaper.",
)
