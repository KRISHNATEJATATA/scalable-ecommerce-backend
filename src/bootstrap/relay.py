"""Outbox relay process entrypoint: ``python -m src.bootstrap.relay``.

The relay itself is kernel code (:mod:`src.shared.bus.relay`); the entrypoint
lives here because choosing *which* schemas to drain is composition knowledge.
"""

from __future__ import annotations

import asyncio
import logging
import signal

from src.bootstrap.outbox import OUTBOX_SCHEMAS
from src.shared.bus.relay import run_relay
from src.shared.clients.postgres_client import create_engine, create_sessionmaker
from src.shared.config.logging import setup_logging
from src.shared.config.setting import get_settings
from src.shared.observability.worker_metrics import serve_worker_metrics

log = logging.getLogger(__name__)


def main() -> None:  # pragma: no cover - process entrypoint
    """`python -m src.bootstrap.relay` — the `service`-role relay worker."""
    settings = get_settings()
    setup_logging(settings.log_level)
    serve_worker_metrics(settings, job="outbox-relay")
    engine = create_engine(settings, worker=True)
    sessionmaker = create_sessionmaker(engine)
    log.info("outbox relay starting (schemas=%s)", ",".join(OUTBOX_SCHEMAS))

    async def _run() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, stop.set)
        try:
            await run_relay(settings, sessionmaker, schemas=OUTBOX_SCHEMAS, stop=stop)
        finally:
            await engine.dispose()

    asyncio.run(_run())


if __name__ == "__main__":  # pragma: no cover
    main()
