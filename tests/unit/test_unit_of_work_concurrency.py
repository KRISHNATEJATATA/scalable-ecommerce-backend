"""The service-owned unit of work under real concurrency: two ``AsyncSession``s.

``UnitOfWork`` is a depth-aware handle over a request-scoped ``AsyncSession``,
and the whole migration rests on two claims that only a real database can
settle:

1. **The outermost block is the only commit point.** A nested ``transaction()``
   is a SAVEPOINT that composes into its caller, so an inner service can never
   commit work its caller did not ask for.
2. **Two handles over one session are equivalent.** Depth lives in
   ``session.info``, not on the handle, so a repository's handle and a service's
   handle agree about who is outermost — without coordinating.

Claim 1 is what makes a multi-repository use-case atomic. Its failure mode is a
*half-committed* operation, which is exactly what the rows written here test:
the writes are asserted on a **second** session, because only another
connection can observe whether the first one really committed. A savepoint that
wrongly committed would be visible there; a correct one is not.

Real Postgres (Testcontainers) via ``conftest.py`` — the nesting is
``begin_nested()`` + the session's implicit transaction, which SQLite does not
reproduce faithfully enough to be worth trusting.
"""

import asyncio

from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker

from src.shared.db.unit_of_work import UnitOfWork


async def _seed(session, sku: str, on_hand: int) -> None:
    await session.execute(
        text("INSERT INTO inventory.inventory (sku, on_hand, reserved, version) VALUES (:sku, :on_hand, 0, 1)"),
        {"sku": sku, "on_hand": on_hand},
    )
    await session.commit()


async def _on_hand(observer, sku: str) -> int | None:
    """Read ``on_hand`` through a *separate* connection.

    This is the crux of every assertion below: the test's own ``session`` sits in
    its own transaction with its own snapshot, so it cannot see the racer's
    uncommitted work. A fresh session reads the committed truth.
    """
    row = (
        await observer.execute(text("SELECT on_hand FROM inventory.inventory WHERE sku = :sku"), {"sku": sku})
    ).one_or_none()
    return None if row is None else row.on_hand


async def test_nested_transaction_release_does_not_commit_the_outer_work(async_engine, session):
    """A nested block that succeeds must NOT commit its caller's open work.

    The regression this guards: if ``transaction()`` committed on nested exit
    (or if depth were tracked per-handle instead of per-session), the inner
    block would flush the outer block's pending write to disk on its way out.
    The outer block then fails, and the caller's work is already irreversibly
    committed — a half-finished operation, the exact thing the UoW exists to
    prevent.

    Observed from a second connection, because only another connection can
    distinguish "written" from "flushed".
    """
    await _seed(session, "sku-nested", 5)
    maker = async_sessionmaker(async_engine, expire_on_commit=False)
    uow = UnitOfWork(session)

    async with maker() as observer:
        reached_failure_point = False
        try:
            async with uow.transaction():
                # The outer use-case's first write.
                await session.execute(
                    text("UPDATE inventory.inventory SET on_hand = on_hand - 1 WHERE sku = 'sku-nested'")
                )
                await session.flush()

                # An inner service invoked inside the outer one, holding its own
                # handle over the same session.
                async with UnitOfWork(session).transaction():
                    await session.execute(
                        text("UPDATE inventory.inventory SET reserved = reserved + 1 WHERE sku = 'sku-nested'")
                    )
                    await session.flush()

                    # The nested block is about to exit *cleanly*. Nothing from
                    # either level may be visible to another connection yet.
                    assert await _on_hand(observer, "sku-nested") == 5, (
                        "a nested savepoint released the caller's uncommitted work"
                    )

                # Still nothing, now that the savepoint has been released.
                assert await _on_hand(observer, "sku-nested") == 5, (
                    "the savepoint release committed the outer transaction"
                )

                reached_failure_point = True
                raise RuntimeError("outer use-case failed after its inner service succeeded")
        except RuntimeError:
            pass

        assert reached_failure_point, "the outer block never reached its failure point"

        # The outer failure rolled everything back — the inner service's work
        # included. A half-applied operation would leave on_hand at 4.
        assert await _on_hand(observer, "sku-nested") == 5, "the outer rollback did not undo the nested write"


async def test_two_handles_over_one_session_agree_on_the_commit_point(async_engine, session):
    """Two handles, one session: only the outermost commits, whoever created them.

    Mirrors production threading exactly — the service opens the boundary and
    the repository holds its *own* handle over the same session.
    """
    await _seed(session, "sku-handles", 5)
    maker = async_sessionmaker(async_engine, expire_on_commit=False)
    service_uow = UnitOfWork(session)
    repository_uow = UnitOfWork(session)  # a different object, the same session

    async with maker() as observer:
        async with service_uow.transaction():
            await session.execute(text("UPDATE inventory.inventory SET on_hand = 4 WHERE sku = 'sku-handles'"))
            await session.flush()

            # The repository's handle sees depth 1, so this is a savepoint, not
            # a commit. Per-handle depth would make it depth 0 and commit here.
            async with repository_uow.transaction():
                await session.execute(text("UPDATE inventory.inventory SET reserved = 1 WHERE sku = 'sku-handles'"))
                await session.flush()
                assert await _on_hand(observer, "sku-handles") == 5, (
                    "the repository's handle believed it was outermost and committed"
                )

            # The service's block is still the only commit point.
            assert await _on_hand(observer, "sku-handles") == 5, "work committed before the outermost block exited"

        # The outermost block committed both writes together.
        assert await _on_hand(observer, "sku-handles") == 4


async def test_two_sessions_racing_the_last_unit_yield_exactly_one_winner(async_engine, session):
    """The oversell race re-run through the unit of work: 2 sessions, 1 unit, 1 winner.

    Each racer owns its ``AsyncSession`` *and* drives the write through
    ``UnitOfWork``, so the commit is made by ``transaction()`` rather than by a
    repository. This is the shape the migration introduces, and it must not be
    able to oversell: the conditional decrement's guard and the commit point now
    live on opposite sides of the boundary, and a UoW that committed at the
    wrong depth (or not at all) would show up here as two winners or none.
    """
    await _seed(session, "sku-uow-race", 1)
    maker = async_sessionmaker(async_engine, expire_on_commit=False)
    racers = 2

    async def attempt() -> bool:
        async with maker() as own_session:
            async with UnitOfWork(own_session).transaction():
                # The guarded decrement: whoever matches the row wins the unit.
                result = await own_session.execute(
                    text(
                        "UPDATE inventory.inventory SET reserved = reserved + 1 "
                        "WHERE sku = 'sku-uow-race' AND on_hand - reserved >= 1"
                    )
                )
                await own_session.flush()
                return bool(result.rowcount)

    results = await asyncio.gather(*(attempt() for _ in range(racers)))

    assert sum(results) == 1, "exactly one racer may win the last unit through the unit of work"

    async with maker() as observer:
        row = (
            await observer.execute(text("SELECT on_hand, reserved FROM inventory.inventory WHERE sku = 'sku-uow-race'"))
        ).one()
    assert (row.on_hand, row.reserved) == (1, 1), "the winner's reservation must have committed exactly once"


async def test_failed_transaction_leaves_nothing_committed_for_a_racer(async_engine, session):
    """A block that raises must leave the row exactly as it found it.

    The complement of the nested test: here the *outermost* block fails, so the
    commit point never runs. Nothing may reach the second connection.
    """
    await _seed(session, "sku-rollback", 5)
    maker = async_sessionmaker(async_engine, expire_on_commit=False)
    uow = UnitOfWork(session)

    async with maker() as observer:
        try:
            async with uow.transaction():
                await session.execute(text("UPDATE inventory.inventory SET on_hand = 1 WHERE sku = 'sku-rollback'"))
                await session.flush()
                raise RuntimeError("use-case failed")
        except RuntimeError:
            pass

        assert await _on_hand(observer, "sku-rollback") == 5, "a failed outermost block still committed its work"

        # The session must be reusable afterwards: the rollback has to leave a
        # clean transaction, or every later statement dies with
        # PendingRollbackError and the recovery batch is poisoned.
        async with uow.transaction():
            await session.execute(text("UPDATE inventory.inventory SET on_hand = 3 WHERE sku = 'sku-rollback'"))

    assert await _on_hand(observer, "sku-rollback") == 3, "the session did not recover after the rollback"
