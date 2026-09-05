"""Payment reconciliation poller — the `service`-role backstop for missed webhooks.

Webhooks are at-least-once *eventually*; a provider outage, a bad endpoint
deployment, or a dropped delivery can leave a paid charge stuck ``pending``
forever — and with it, the order. The poller is the backstop: every pass takes the
oldest still-pending payments past their confirmation grace window, asks the
gateway what actually happened (``lookup`` by idempotency key), and applies the
answer through the **same guarded transition a webhook uses** — so a late webhook
racing the poller is still safe, whichever arrives second updates zero rows.

Same shape as the reservation reaper: a TTL plus a sweep, not a hope. Run
continuously (``python -m src.payments.adapters.reconciler``, as in
docker-compose) or as a scheduled one-shot in prod (EventBridge → ECS task with
``--once``).

This module is a **scheduling shell**: it owns the loop, signal handling, and
session, and delegates the actual reconciliation to
:meth:`~src.payments.application.service.PaymentsService.reconcile`.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal

from sqlalchemy.ext.asyncio import async_sessionmaker

from src.payments.adapters.db.repository import PaymentsRepository
from src.payments.adapters.stub_gateway import StubPaymentGateway
from src.payments.application.service import PaymentsService
from src.shared.config.setting import AppSettings, get_settings

log = logging.getLogger(__name__)


class PaymentReconciler:
    """Reconciles stuck pending payments in batches until stopped."""

    def __init__(
        self,
        sessionmaker: async_sessionmaker,
        gateway: StubPaymentGateway,
        *,
        batch_size: int,
        grace_seconds: int,
        max_age_seconds: int,
        webhook_secret: str | None = None,
    ) -> None:
        self._sessionmaker = sessionmaker
        self._gateway = gateway
        self._batch = batch_size
        self._grace_seconds = grace_seconds
        self._max_age_seconds = max_age_seconds
        self._webhook_secret = webhook_secret

    async def sweep_once(self) -> int:
        """One batch: resolve stuck pendings against the gateway; return how many."""
        async with self._sessionmaker() as session:
            service = PaymentsService(
                PaymentsRepository(session),
                self._gateway,
                webhook_secret=self._webhook_secret,
                reconciliation_grace_seconds=self._grace_seconds,
                reconciliation_max_age_seconds=self._max_age_seconds,
            )
            return await service.reconcile(batch_size=self._batch)

    async def run(self, poll_interval: float, stop: asyncio.Event | None = None) -> None:
        """Loop until ``stop`` is set; sleep ``poll_interval`` only when idle.

        A full batch means there may be more waiting, so the next pass runs
        immediately — the same drain-then-sleep shape as the outbox relay."""
        while stop is None or not stop.is_set():
            try:
                resolved = await self.sweep_once()
            except Exception:  # boundary: one bad pass must not kill the poller
                log.exception("reconciliation pass failed; retrying after interval")
                resolved = 0
            if resolved < self._batch:
                await asyncio.sleep(poll_interval)


async def run_reconciler(
    settings: AppSettings,
    sessionmaker: async_sessionmaker,
    *,
    gateway: StubPaymentGateway | None = None,
    stop: asyncio.Event | None = None,
    once: bool = False,
) -> int:
    """Build a reconciler from settings and run it (one sweep with ``once=True``)."""
    reconciler = PaymentReconciler(
        sessionmaker,
        gateway or StubPaymentGateway(settings.payment_stub_fail_token_substring),
        batch_size=settings.payment_reconciliation_batch_size,
        grace_seconds=settings.payment_reconciliation_grace_seconds,
        max_age_seconds=settings.payment_reconciliation_max_age_seconds,
        webhook_secret=settings.payment_webhook_secret,
    )
    if once:
        return await reconciler.sweep_once()
    await reconciler.run(settings.payment_reconciliation_poll_interval_seconds, stop=stop)
    return 0


def main() -> None:  # pragma: no cover - process entrypoint
    """`python -m src.payments.adapters.reconciler [--once]` — the `service`-role poller."""
    from src.shared.clients.postgres_client import create_engine, create_sessionmaker
    from src.shared.config.logging import setup_logging
    from src.shared.observability.worker_metrics import push_worker_metrics, serve_worker_metrics

    parser = argparse.ArgumentParser(description="Resolve payments whose webhook never arrived.")
    parser.add_argument("--once", action="store_true", help="run a single sweep and exit (scheduled-task mode)")
    args = parser.parse_args()

    settings = get_settings()
    setup_logging(settings.log_level)
    # Looping: Prometheus scrapes us. `--once`: we exit before any scrape, so push
    # at the end instead. Both no-ops unless configured.
    if not args.once:
        serve_worker_metrics(settings, job="payment-reconciler")
    engine = create_engine(settings, worker=True)
    sessionmaker = create_sessionmaker(engine)
    log.info(
        "payment reconciler starting (once=%s, grace=%ss)",
        args.once,
        settings.payment_reconciliation_grace_seconds,
    )

    async def _run() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, stop.set)
        try:
            await run_reconciler(settings, sessionmaker, stop=stop, once=args.once)
        finally:
            await engine.dispose()

    asyncio.run(_run())
    if args.once:
        push_worker_metrics(settings, job="payment-reconciler")


if __name__ == "__main__":  # pragma: no cover
    main()
