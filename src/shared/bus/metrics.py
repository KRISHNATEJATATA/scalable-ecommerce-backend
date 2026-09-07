"""Outbox-bus Prometheus metrics.

``outbox_lag_seconds{schema}`` — the age of the oldest unpublished outbox row
per publishing schema (0 when nothing is unpublished). This is the relay's
alert signal, and it is deliberately sourced **from the database**, not from
whoever observed it last: a relay that died increments nothing, so a relay-side
gauge would go silent exactly when the alarm matters (the same trap the reaper
counter documents). The database is the ground truth — unpublished rows are
rows, whoever reads them.

Two writers share the one gauge:

* the API's lifespan polls it every ``OUTBOX_LAG_POLL_SECONDS`` (async session,
  one ``UNION ALL`` round trip over the per-schema partial indexes) so the
  API's ``/metrics`` keeps reporting through a relay outage;
* the relay stamps it inline at claim time (its claim query already returns
  the rows ordered by ``occurred_at``), free, for its own scrape port.

Schema names are interpolated into identifier positions (validated against the
``OUTBOX_SCHEMAS`` allow-list inside :func:`update_outbox_lag`), never taken
from user input.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence

from prometheus_client import Gauge
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker

from src.shared.bus.constants import OUTBOX_SCHEMAS

log = logging.getLogger(__name__)

outbox_lag_seconds = Gauge(
    "outbox_lag_seconds",
    "Age of the oldest unpublished outbox row per schema; 0 when none is unpublished.",
    ["schema"],
)


async def update_outbox_lag(sessionmaker: async_sessionmaker, *, schemas: Sequence[str] = OUTBOX_SCHEMAS) -> None:
    """Measure every schema's lag straight from the DB and set the gauge.

    One session, one ``UNION ALL`` query; ``MIN`` over ``published_at IS NULL``
    rides each schema's partial index. An empty (fully drained) schema has a
    ``NULL`` age, which means **0** — no lag is not an unknown lag.
    """
    # Schema names are interpolated into identifier positions, so anything
    # outside the trusted constant is refused here, not trusted at the f-string.
    if any(schema not in OUTBOX_SCHEMAS for schema in schemas):
        raise ValueError(f"schemas must be a subset of OUTBOX_SCHEMAS, got {tuple(schemas)}")
    unions = " UNION ALL ".join(
        f"SELECT '{schema}' AS schema_name, "  # noqa: S608
        f"EXTRACT(EPOCH FROM (now() - MIN(occurred_at))) AS lag_seconds "
        f"FROM {schema}.outbox WHERE published_at IS NULL"
        for schema in schemas
    )
    async with sessionmaker() as session:
        rows = (await session.execute(text(unions))).all()
    for schema_name, lag_seconds in rows:
        outbox_lag_seconds.labels(schema_name).set(float(lag_seconds or 0.0))


async def poll_outbox_lag(
    sessionmaker: async_sessionmaker, poll_seconds: float, *, schemas: Sequence[str] = OUTBOX_SCHEMAS
) -> None:
    """Refresh the gauge every ``poll_seconds`` until the task is cancelled.

    A failed pass keeps the last values and retries: losing one sample must
    not break the exporter (telemetry boundary), and a stale-but-climbing
    gauge still alerts — the runbook's lag alarm is on sustained growth.
    """
    while True:
        try:
            await update_outbox_lag(sessionmaker, schemas=schemas)
        except asyncio.CancelledError:
            raise
        except Exception:  # boundary: telemetry must not die with the DB
            log.exception("outbox lag refresh failed; keeping last values")
        await asyncio.sleep(poll_seconds)
