"""Notification Prometheus counters.

Registered on the default ``prometheus_client`` registry; incremented only in
the notification-worker process, so they reach Prometheus via that worker's
``WORKER_METRICS_PORT`` (same pattern as ``src/orders/application/metrics.py``).

* ``notification_sent_total`` — one increment per confirmation actually sent,
  labeled by ``email_type``.
* ``notification_suppressed_total`` — sends deliberately NOT made, by
  ``reason``: ``suppressed`` (recipient on the suppression list — hard bounce
  or spam complaint ⇒ never send again) and ``already_sent`` (a ``sent``
  ``sent_emails`` row caught a delivery the Valkey dedupe missed — expected
  after the dedupe-TTL expiry, a redelivery signal otherwise).
* ``notification_send_recovered_total`` — takeovers of a claimed-but-unmarked
  send (ADR 0024's crash window). Each one is a resend whose predecessor MAY
  have landed: the observable of the at-least-once residual, alertable when
  it moves without a worker crash.
"""

from __future__ import annotations

from prometheus_client import Counter

notification_sent_total = Counter(
    "notification_sent_total",
    "Notification emails actually sent, by email type.",
    ["email_type"],
)

notification_suppressed_total = Counter(
    "notification_suppressed_total",
    "Sends deliberately not made (suppressed list, already_sent backstop), by reason.",
    ["reason"],
)

notification_send_recovered_total = Counter(
    "notification_send_recovered_total",
    "Takeovers of a claimed-but-unmarked send (the crash window); each a possible duplicate, same Message-ID.",
    ["email_type"],
)
