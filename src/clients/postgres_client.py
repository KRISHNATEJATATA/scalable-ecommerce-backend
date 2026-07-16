"""PostgreSQL client for ecommerce backend.

Owns the async SQLAlchemy engine + session factory. The engine is created at
startup (lazy — no connection until first use) and disposed at shutdown.
"""

from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine
from sqlalchemy.sql import text

from src.config.setting import AppSettings


def create_engine(settings: AppSettings) -> AsyncEngine:
    """Create the async engine from settings (does not open a connection)."""
    return create_async_engine(
        str(settings.database_url),
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        pool_pre_ping=settings.db_pool_pre_ping,
    )


def create_sessionmaker(engine: AsyncEngine) -> async_sessionmaker:
    return async_sessionmaker(engine, expire_on_commit=False)


async def ping(engine: AsyncEngine) -> bool:
    """Return True if a trivial round-trip to Postgres succeeds."""
    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))
    return True
