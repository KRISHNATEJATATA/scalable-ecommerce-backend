"""Notification Prometheus counters.

Registered on the default ``prometheus_client`` registry; incremented only in
the notification-worker process, so they reach Prometheus via that worker's
``WORKER_METRICS_PORT`` (same pattern as ``src/orders/application/metrics.py``).

* ``notification_sent_total`` — one increment per confirmation actually sent,
  labeled by ``email_type``.
* ``notification_suppressed_total`` — sends deliberately NOT made, by
  ``reason``: ``suppressed`` (recipient on the suppression list — hard bounce
  or spam complaint ⇒ never send again) and ``already_sent`` (the
  ``sent_emails`` backstop caught a delivery the Valkey dedupe missed —
  expected after the dedupe-TTL expiry, a redelivery signal otherwise).
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
