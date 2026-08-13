"""PostgreSQL client for ecommerce backend.

Owns the async SQLAlchemy engine + session factory. The engine is created at
startup (lazy — no connection until first use) and disposed at shutdown.
"""

from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine
from sqlalchemy.sql import text

from src.shared.config.setting import AppSettings


def create_engine(settings: AppSettings, *, worker: bool = False) -> AsyncEngine:
    """Create the async engine from settings (does not open a connection).

    ``worker=True`` uses the much smaller worker pool. Workers are single-task
    loops that hold at most one session at a time, so the API's pool sizing
    (``5 + 10`` per process) would reserve ~15 connections each for nothing —
    and every process's pool counts against the same RDS ``max_connections``.
    See the connection-budget formula in ``docs/DEPLOYMENT.md``.
    """
    return create_async_engine(
        str(settings.database_url),
        pool_size=settings.db_worker_pool_size if worker else settings.db_pool_size,
        max_overflow=settings.db_worker_max_overflow if worker else settings.db_max_overflow,
        pool_pre_ping=settings.db_pool_pre_ping,
    )


def create_probe_engine(settings: AppSettings) -> AsyncEngine:
    """Engine for readiness probes only — its own **one-connection** pool.

    Probing through the request pool makes a *saturated* pool look like a *dead
    database*: the probe blocks on checkout, readiness 503s, the ALB deregisters
    the task, its traffic shifts to the remaining tasks, and they saturate too.
    A dedicated pool keeps that isolation, but it must still be **bounded** —
    ``NullPool`` opens a fresh connection per probe, so a probe storm (or a slow
    Postgres holding each one open) is unbounded connection growth against the
    RDS budget that the sizing formula in ``docs/DEPLOYMENT.md`` never counted.

    One connection is enough: the probe is a single ``SELECT 1``. Concurrent
    probes queue for it, bounded by ``pool_timeout`` (and by the probe's own
    deadline), and ``pool_pre_ping`` keeps a long-idle probe connection from
    reporting a stale socket as a dead database.
    """
    return create_async_engine(
        str(settings.database_url),
        pool_size=1,
        max_overflow=0,
        pool_timeout=settings.readiness_probe_timeout_seconds,
        pool_pre_ping=True,
    )


def create_sessionmaker(engine: AsyncEngine) -> async_sessionmaker:
    return async_sessionmaker(engine, expire_on_commit=False)


async def ping(engine: AsyncEngine) -> bool:
    """Return True if a trivial round-trip to Postgres succeeds."""
    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))
    return True
