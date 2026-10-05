"""Service-owned unit of work — the transaction boundary belongs to the use-case.

Every DB-backed repository used to call ``commit()`` (or ``rollback()``) at the
end of each of its own methods, so a transaction was scoped to one **repository
method**, not to one business operation. A multi-step operation — a saga step, a
cancel that flips + releases + journals, a checkout that reserves + consumes —
could therefore never be one atomic unit, and every caller had to reason about
which prefix of its work had already been committed.

``UnitOfWork`` inverts that. The application service owns the boundary::

    async with self._repo.uow.transaction():
        ...                       # several repository calls, one transaction

and the repositories bound to the same session only ``flush()``. The **physical**
unit of work is still the request-scoped ``AsyncSession`` (``src.shared.db.session``);
a ``UnitOfWork`` is a thin, depth-aware handle over it, so two handles created
around the same session are equivalent — a repository and a service may each hold
their own without coordinating.

Nesting is the reason this is a real object and not just a ``commit()`` call site.
An inner service invoked from an outer one (a saga's step calling the inventory
service; a cancel calling the stock-holds port) can only be atomic *with* its
caller if the inner transaction composes instead of committing. So the outermost
``transaction()`` owns the real commit and every deeper block is a **SAVEPOINT**:

* depth 0 — commit on clean exit, roll back on any exception (the use-case);
* depth > 0 — ``begin_nested()`` savepoint, released into the outer transaction,
  so an inner block can never commit work its caller did not ask for.

The outbox invariant is preserved for free: a state change and its ``outbox`` row
are written through the same session inside the same block, so they commit
together or not at all — never dual-write.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager, contextmanager
from typing import Protocol

from sqlalchemy.ext.asyncio import AsyncSession

#: Session-attached nesting depth (``session.info``), shared by every handle over
#: one session so the outermost block is always the one that commits.
_DEPTH_KEY = "uow_depth"


class UnitOfWorkPort(Protocol):
    """The transaction handle an application service drives.

    Repositories and services depend on this abstraction; ``UnitOfWork`` is the
    adapter over a request-scoped ``AsyncSession``. Only ``transaction()`` is
    part of the contract — committing is a policy decision the service owns,
    so there is deliberately no ``commit()``/``rollback()`` to call around it.
    """

    @property
    def session(self) -> AsyncSession:
        """The underlying session — repositories query through it, never commit it."""
        ...

    @property
    def depth(self) -> int:
        """Current nesting depth on the session (0 = no open unit of work)."""
        ...

    def transaction(self) -> AbstractAsyncContextManager[None]:
        """A block whose clean exit commits the outermost transaction and no-op-nests inside one."""
        ...


class UnitOfWork:
    """A thin, depth-aware transaction handle over one ``AsyncSession``.

    Safe to construct more than once per request: every handle shares the same
    session and the same nesting depth, so the outermost ``transaction()`` is the
    single commit point regardless of how the handles were threaded.
    """

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    @property
    def depth(self) -> int:
        """Current nesting depth on the session (0 = no open unit of work)."""
        return int(self.session.info.get(_DEPTH_KEY, 0))

    async def rollback(self) -> None:
        """Drop the session's current transaction (recovery's per-order error boundary).

        Adapter-only affordance — not part of :class:`UnitOfWorkPort`, so no
        caller can bypass depth tracking and unwind (or commit) a half-finished
        operation. There is deliberately no ``commit()`` here either: the
        outermost ``transaction()`` exit is the single commit point.
        """
        await self.session.rollback()

    @contextmanager
    def _depth(self) -> Iterator[bool]:
        """Track nesting on the session; yield ``True`` for the outermost block only."""
        info = self.session.info
        depth = int(info.get(_DEPTH_KEY, 0))
        info[_DEPTH_KEY] = depth + 1
        try:
            yield depth == 0
        finally:
            info[_DEPTH_KEY] = depth

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[None]:
        """Run a use-case body atomically; commit at the outermost level, SAVEPOINT when nested."""
        with self._depth() as outermost:
            if not outermost:
                # Nested: a SAVEPOINT composes into the caller's transaction, so
                # an inner service can never commit a half-finished outer operation.
                async with self.session.begin_nested():
                    yield
                return
            try:
                yield
            except BaseException:
                await self.session.rollback()
                raise
            else:
                # Commits any implicitly-begun transaction too, so a read that
                # autobegan before this block is flushed with the writes.
                await self.session.commit()
