"""Notification worker — the `service`-role consumer that sends order-confirmation
emails.

Thin SQS transport shell over the generic idempotent :class:`SqsConsumer`: it
drains the ``notifications`` queue (subscribed to ``OrderPlaced`` and
``UserCreated`` via SNS — see ``CONSUMERS`` in ``scripts/bus_bootstrap.py``)
and hands each validated event to the application service:

* ``UserCreated`` upserts the recipient (user_id → email) into this module's
  own ``recipients`` table — the bus-delivered materialization that keeps the
  send path free of any cross-module identity read.
* ``OrderPlaced`` renders + sends the confirmation via the sender port, with
  the suppression-list and ``sent_emails`` backstops (see
  :class:`~src.notifications.application.service.NotificationService`).

Idempotent twice over: ``SqsConsumer`` dedupes on ``event_id`` **within this
subscription** (``event:notifications:{event_id}``), and the service's DB
backstop (``UNIQUE(order_id, email_type)``) means a redelivery after the
dedupe-TTL expiry can never double-send. A handler that raises leaves the
message for SQS redrive → DLQ (replay per ``docs/RUNBOOK.md``).

Run: ``python -m src.notifications.adapters.notification_worker``.
"""

from __future__ import annotations

import asyncio
import logging
import signal
from typing import Any

from src.notifications.adapters.db.repository import NotificationRepository
from src.notifications.adapters.senders import make_sender_cm
from src.notifications.application.service import NotificationService
from src.notifications.themes import EmailTheme
from src.shared.bus.client import sqs_client
from src.shared.bus.consumer import Handler, SqsConsumer
from src.shared.config.setting import AppSettings, get_settings

log = logging.getLogger(__name__)


def make_notification_handler(sessionmaker: Any, sender: Any, theme: Any) -> Handler:
    """Build the SqsConsumer handler routing validated events into the service.

    A per-message session (the worker pool's shape: one session at a time per
    coroutine) wraps the repository + service for each event. The loaded
    :class:`~src.notifications.themes.EmailTheme` carries the confirmation copy.
    """

    async def _handle(event: dict[str, Any]) -> None:
        event_type = event.get("type")
        async with sessionmaker() as session:
            service = NotificationService(NotificationRepository(session), sender, theme)
            if event_type == "UserCreated":
                await service.handle_user_created(event)
            elif event_type == "OrderPlaced":
                await service.handle_order_placed(event)
            else:  # pragma: no cover - the subscription only carries these two events
                log.warning("notification worker ignoring unexpected event type %r", event_type)

    return _handle


async def run_worker(settings: AppSettings, sessionmaker: Any, valkey: Any, stop: asyncio.Event) -> None:
    """Build a real SQS-backed notification consumer and run its loop."""
    if not settings.notifications_queue_url:
        raise RuntimeError("NOTIFICATIONS_QUEUE_URL must be configured for the notification worker")
    # The theme loads first (fail-fast at boot: a missing/misrouted theme dir is
    # a boot failure, before any queue or SMTP contact — same contract as the
    # sender selection below).
    theme = EmailTheme.from_settings(settings.notification_email_theme_dir)
    # "theme", not "email theme" — the RedactFilter scrubs any message containing
    # a _REDACT_KEYS substring ("email" is one), and this line carries no secret.
    log.info("notification worker theme: %s", theme.name)
    async with make_sender_cm(settings) as sender, sqs_client(settings) as sqs:
        consumer = SqsConsumer(
            sqs,
            valkey,
            settings.notifications_queue_url,
            make_notification_handler(sessionmaker, sender, theme),
            consumer_name="notifications",
            dedup_ttl_seconds=settings.consumer_dedup_ttl_seconds,
            lease_ttl_seconds=settings.consumer_lease_ttl_seconds,
            max_messages=settings.consumer_max_messages,
            wait_time_seconds=settings.consumer_wait_time_seconds,
        )
        await consumer.run(stop)


def main() -> None:  # pragma: no cover - process entrypoint
    """`python -m src.notifications.adapters.notification_worker` — the `service`-role notification worker."""
    from src.shared.clients import valkey_client
    from src.shared.clients.postgres_client import create_engine, create_sessionmaker
    from src.shared.config.logging import setup_logging
    from src.shared.observability.worker_metrics import serve_worker_metrics

    settings = get_settings()
    setup_logging(settings.log_level)
    serve_worker_metrics(settings, job="notification-worker")
    engine = create_engine(settings, worker=True)
    sessionmaker = create_sessionmaker(engine)
    valkey = valkey_client.create_client(settings)
    log.info("notification worker starting (queue=%s)", settings.notifications_queue_url)

    async def _run() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, stop.set)
            except NotImplementedError:  # Windows Proactor loop has no add_signal_handler
                signal.signal(sig, lambda *_: stop.set())
        try:
            await run_worker(settings, sessionmaker, valkey, stop)
        finally:
            await valkey.aclose()
            await engine.dispose()

    asyncio.run(_run())


if __name__ == "__main__":  # pragma: no cover
    main()
