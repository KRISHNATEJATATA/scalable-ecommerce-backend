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
  - ``cart_changed`` — the catalog's current price no longer matched the
    cart's snapshot (or a product was gone) at checkout
    (``CartChangedError``): the async price projection losing its race, and
    the revalidation guard catching it before an order exists;
  - ``idempotency_conflict`` — same Idempotency-Key, different body (caller
    bug, never a second order);
  - ``conflict`` — any other controlled 409: empty cart, declined payment,
    cancelled replay, step timeout, reservation line conflicts — **and** the
    post-payment failures convert to "it will be settled automatically" (the
    order stays pending; unwinding is forbidden once money moved) and both
    orphan-arm "it will be reconciled" answers (the order is already
    cancelled, so the stranded pair is reconciled by hand);
  - ``error`` — anything unhandled (the 500-shaped residue).

  Checkout success rate = ``rate(paid + replayed) / rate(sum)``.
* ``checkout_compensation_total`` — every ``_compensate`` run (release holds +
  cancel), labeled by the ``step`` that failed: ``reserve``/``charge`` on the
  live path, ``crashed`` from the recovery poller. A user-facing cancel is the
  mirror image of compensation (glossary: Orders/Compensation), never counted
  here. A sustained rate means checkouts are failing mid-flight with real
  state to unwind.
* ``checkout_recovery_total`` — the recovery poller's settlements per
  ``outcome``: ``completed``, ``compensated``, ``deferred``, plus the
  refund-retry claim's ``refunded`` (a journaled refund intent the retry
  closed — money back) and ``refund_failed`` (the retry met a definitive
  refusal — terminal, counted as an orphan). A sustained ``compensated``
  rate means checkouts are crashing mid-flight upstream; a ``deferred``
  rate means the payment reconciler owns those orders (or the refund
  provider is down and retries are backing up), not this poller.
* ``checkout_orphaned_paid_payments_total`` — an orphaned paid payment the
  saga could **not** automatically refund: the charge succeeded but the order
  died (a concurrent cancel won the guarded flip, or the recovery poller
  compensated a paid-without-consume shortfall) and the refund leg was
  **refused** by the provider — or raised when not even the refund intent
  could be journaled, leaving nothing to retry it. Successful refunds emit
  ``PaymentRefunded`` and are the routine race resolution; a raised refund
  with the intent journaled is retried by the recovery poller's refund claim
  , never counted here. Any increment therefore means money is
  still taken on a cancelled order with no automatic owner left, and a human
  must reconcile it (the payment reconciler only scans ``pending`` charges).
  The log line and the 409 the caller sees are transient, so this counter is
  the alertable signal — increments are refund-provider incidents, not race
  bookkeeping.
* ``checkout_paid_without_consume_total`` — a succeeded payment whose order
  could not consume its full stock because the reservation reaper released the
  holds before the payment confirmed (the paid-without-consume window). The
  saga compensates the order instead of paying it — the stock was already back
  in the pool — and refunds the charge through the payments service's refund
  leg (the payment must not stand: the goods were never delivered). If the
  automatic refund is refused, the succeeded payment lands on the cancelled
  order and the orphan counter above counts it; the RUNBOOK §9 query finds
  the pair.
"""

from __future__ import annotations

from prometheus_client import Counter

checkout_attempts_total = Counter(
    "checkout_attempts_total",
    "Checkout calls by outcome (paid, replayed, out_of_stock, cart_changed, idempotency_conflict, conflict, error).",
    ["outcome"],
)

checkout_compensation_total = Counter(
    "checkout_compensation_total",
    "Sagas that ran release-holds + cancel compensation, by the step that failed (reserve, charge, crashed).",
    ["step"],
)

checkout_recovery_total = Counter(
    "checkout_recovery_total",
    "Recovery-poller settlements by outcome (completed, compensated, deferred, refunded, refund_failed).",
    ["outcome"],
)

checkout_orphaned_paid_payments_total = Counter(
    "checkout_orphaned_paid_payments_total",
    "Succeeded payments on a cancelled order whose automatic refund was refused (or lost its journaled "
    "intent) — no automatic owner remains; manual reconciliation required.",
)

checkout_paid_without_consume_total = Counter(
    "checkout_paid_without_consume_total",
    "Succeeded payments whose order could not consume its full stock (holds reaped before the payment "
    "confirmed) — the order is compensated and the charge refunded; only a failed refund needs manual "
    "reconciliation.",
)
