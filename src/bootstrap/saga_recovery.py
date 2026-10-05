"""Saga recovery poller — the `service`-role worker that settles crashed checkouts.
A checkout that dies between steps (process crash, deploy restart) leaves a
`pending` order with holds against it: stock nobody can buy and an order nobody
owns the outcome of. The reaper would eventually release the holds, but the
order itself would sit `pending` forever. This poller closes that window: every
pass claims `pending` orders older than the saga step timeout (``FOR UPDATE
SKIP LOCKED``, so N replicas split the batch) and settles each from its
payment row — commit + mark paid when the charge succeeded, release + cancel
otherwise. Still-`pending` payments are left for the payment reconciler. The
same pass also retries journaled refund intents on already-`cancelled` orders
(a refund that raised on the drive leaves `refund: requested` in the journal
, so a transient refund-provider outage never becomes manual
reconciliation.

Safe to run continuously (as in docker-compose, mirroring the reaper) or as a
scheduled one-shot in prod (EventBridge → ECS task with ``--once``).

This module is a **scheduling shell**: it owns the loop, the signal handling
and the sessions, and delegates settling to
:meth:`~src.orders.application.checkout_saga.CheckoutSaga.recover_stuck`. Like
every other worker it builds repositories only to hand them to services and
ports — Route/Worker → Service → Repository is never short-circuited.

Lives in ``bootstrap`` (not ``orders``) deliberately: settling composes four
modules' services, and the module-independence contract lets only the
composition root do that. The saga's decision logic stays in
``orders.application``; this is transport.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import signal
from datetime import UTC, datetime, timedelta

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.bootstrap.saga_factory import build_checkout_saga
from src.orders.application.checkout_saga import CheckoutSaga
from src.shared.config.setting import AppSettings, get_settings

log = logging.getLogger(__name__)


class SagaRecovery:
    """Claims and settles crashed checkouts in batches until stopped."""

    def __init__(
        self,
        sessionmaker: async_sessionmaker,
        valkey: object,
        settings: AppSettings,
    ) -> None:
        self._sessionmaker = sessionmaker
        self._valkey = valkey
        self._settings = settings

    def _saga(self, session: AsyncSession) -> CheckoutSaga:
        """Build the saga over the shared factory (identical wiring to the request path)."""
        return build_checkout_saga(session, self._valkey, self._settings)

    async def sweep_once(self) -> dict[str, int]:
        """One pass: settle every stuck checkout, return the outcome counts."""
        async with self._sessionmaker() as session:
            cutoff = datetime.now(UTC) - timedelta(seconds=self._settings.checkout_saga_step_timeout_seconds)
            return await self._saga(session).recover_stuck(
                cutoff=cutoff, batch_size=self._settings.checkout_saga_recovery_batch_size
            )

    async def run(self, poll_interval: float, stop: asyncio.Event | None = None) -> None:
        """Loop until ``stop`` is set; sleep ``poll_interval`` only when idle.

        The idle sleep wakes the moment ``stop`` is set, so SIGTERM never waits
        out a full interval (bounded shutdown: finish the current sweep, exit).
        """
        while stop is None or not stop.is_set():
            try:
                settled = await self.sweep_once()
            except Exception:  # boundary: one bad pass must not kill recovery
                log.exception("saga recovery pass failed; retrying after interval")
                settled = {"completed": 0, "compensated": 0, "deferred": 0, "refunded": 0, "refund_failed": 0}
            if sum(settled.values()) == 0:
                if stop is None:
                    await asyncio.sleep(poll_interval)
                    continue
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=poll_interval)


async def run_recovery(
    settings: AppSettings,
    sessionmaker: async_sessionmaker,
    valkey: object,
    *,
    stop: asyncio.Event | None = None,
    once: bool = False,
) -> dict[str, int]:
    """Build recovery from settings and run it (one sweep with ``once=True``)."""
    recovery = SagaRecovery(sessionmaker, valkey, settings)
    if once:
        return await recovery.sweep_once()
    await recovery.run(settings.checkout_saga_recovery_poll_interval_seconds, stop=stop)
    return {"completed": 0, "compensated": 0, "deferred": 0, "refunded": 0, "refund_failed": 0}


def main() -> None:  # pragma: no cover - process entrypoint
    """`python -m src.bootstrap.saga_recovery [--once]` — the `service`-role saga recovery."""
    from src.shared.clients import valkey_client
    from src.shared.clients.postgres_client import create_engine, create_sessionmaker
    from src.shared.config.logging import setup_logging
    from src.shared.observability.worker_metrics import push_worker_metrics, serve_worker_metrics

    parser = argparse.ArgumentParser(description="Settle crashed checkout sagas.")
    parser.add_argument("--once", action="store_true", help="run a single sweep and exit (scheduled-task mode)")
    args = parser.parse_args()

    settings = get_settings()
    setup_logging(settings.log_level)
    if not args.once:
        serve_worker_metrics(settings, job="saga-recovery")
    engine = create_engine(settings, worker=True)
    sessionmaker = create_sessionmaker(engine)
    valkey = valkey_client.create_client(settings)
    log.info("saga recovery starting (once=%s)", args.once)

    async def _run() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, stop.set)
        try:
            await run_recovery(settings, sessionmaker, valkey, stop=stop, once=args.once)
        finally:
            await valkey.aclose()
            await engine.dispose()

    asyncio.run(_run())
    if args.once:
        push_worker_metrics(settings, job="saga-recovery")


if __name__ == "__main__":  # pragma: no cover
    main()
