"""Checkout-saga Prometheus counters.

Registered on the default ``prometheus_client`` registry, so the API's
``/metrics`` (``src/shared/api/metrics.py``) exposes the ones incremented in the
API process, and ``src/shared/observability/worker_metrics.py`` exports the ones
incremented in the recovery-poller process — same pattern as
``src/inventory/application/metrics.py``.

* ``checkout_attempts_total`` — one increment per ``CheckoutSaga.checkout()``
  call, labeled with how the call ended (derived from the exception type, so
  one checkout is never counted twice):

  - ``paid`` — the caller's order reached ``paid`` (fresh drive);
  - ``replayed`` — an idempotent replay served the stored ``paid`` order (also
    a success: the client got its order);
  - ``out_of_stock`` — the shelves refused (``InsufficientStockError``): the
    oversell guard working, counted here per checkout and separately per
    reservation by ``inventory_oversell_blocked_total``;
  - ``idempotency_conflict`` — same Idempotency-Key, different body (caller
    bug, never a second order);
  - ``conflict`` — any other controlled 409: empty cart, declined payment,
    cancelled replay, step timeout, reservation line conflicts — **and** the
    post-payment failures converts to "it will be settled
    automatically" (the order stays pending; unwinding is forbidden once money
    moved);
  - ``error`` — anything unhandled (the 500-shaped residue).

  Checkout success rate = ``rate(paid + replayed) / rate(sum)``.
* ``checkout_compensation_total`` — every ``_compensate`` run (release holds +
  cancel), labeled by the ``step`` that failed: ``reserve``/``charge`` on the
  live path, ``crashed`` from the recovery poller. A user-facing cancel is the
  mirror image of compensation (glossary: Orders/Compensation), never counted
  here. A sustained rate means checkouts are failing mid-flight with real
  state to unwind.
* ``checkout_recovery_total`` — the recovery poller's settlements per
  ``outcome``: ``completed``, ``compensated``, ``deferred``. A sustained
  ``compensated`` rate means checkouts are crashing mid-flight upstream; a
  ``deferred`` rate means the payment reconciler owns those orders, not this
  poller.
"""

from __future__ import annotations

from prometheus_client import Counter

checkout_attempts_total = Counter(
    "checkout_attempts_total",
    "Checkout calls by how they ended (paid, replayed, out_of_stock, idempotency_conflict, conflict, error).",
    ["outcome"],
)

checkout_compensation_total = Counter(
    "checkout_compensation_total",
    "Sagas that ran release-holds + cancel compensation, by the step that failed (reserve, charge, crashed).",
    ["step"],
)

checkout_recovery_total = Counter(
    "checkout_recovery_total",
    "Recovery-poller settlements of crashed checkouts by outcome (completed, compensated, deferred).",
    ["outcome"],
)
