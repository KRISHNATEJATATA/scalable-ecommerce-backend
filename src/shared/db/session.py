"""Request-scoped async DB session dependency.

``get_session`` opens one session per request from the app's sessionmaker
(owned by the lifespan in ``src/app.py``), rolls back on any error, and closes
on exit. No auto-commit: reads don't need it and auto-commit-on-exit is a
footgun for later writes — feature tickets that write commit explicitly.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

from fastapi import Request
from sqlalchemy.ext.asyncio import AsyncSession


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    """Yield one request-scoped ``AsyncSession``, rolling back on error, no auto-commit."""
    sm = request.app.state.db_sessionmaker
    async with sm() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
    # no auto-commit — reads don't need it and auto-commit-on-exit is a
    # footgun for later writes; feature tickets commit explicitly.
