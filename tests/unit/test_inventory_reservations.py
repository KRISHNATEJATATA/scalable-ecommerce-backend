"""Inventory reservations, oversell defense and the reaper.

The crown-jewel invariant gets a real race, not a mocked one: N tasks, **each
with its own ``AsyncSession``**, fight over the last unit against a real Postgres.
Exactly one may win — if the conditional decrement or the transaction boundary
ever regresses, this is the check that fails.

Uses the shared Testcontainers-Postgres fixtures from ``conftest.py``.
"""

import asyncio
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from prometheus_client import REGISTRY
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker

from src.events.registry import validate_event
from src.inventory.adapters.db.repository import InventoryRepository
from src.inventory.adapters.reaper import ReservationReaper
from src.inventory.application.outbox import stock_released_outbox, stock_reserved_outbox
from src.inventory.application.service import InventoryService
from src.inventory.domain.reservation import ReservationStatus
from src.shared.errors.exceptions import (
    InsufficientStockError,
    InvalidReservationError,
    ReservationConflictError,
    ReservationContendedError,
    StockMutationError,
)


def _metric(name: str) -> float:
    return REGISTRY.get_sample_value(name) or 0.0


def _reaper(sessionmaker, batch_size: int = 10) -> ReservationReaper:
    return ReservationReaper(sessionmaker, batch_size=batch_size, reservation_ttl_seconds=900)


async def _seed(session, sku: str, on_hand: int) -> None:
    await session.execute(
        text("INSERT INTO inventory.inventory (sku, on_hand, reserved, version) VALUES (:sku, :on_hand, 0, 1)"),
        {"sku": sku, "on_hand": on_hand},
    )
    await session.commit()


async def _stock(session, sku: str) -> tuple[int, int]:
    row = (
        await session.execute(text("SELECT on_hand, reserved FROM inventory.inventory WHERE sku = :sku"), {"sku": sku})
    ).one()
    return row.on_hand, row.reserved


async def _outbox_types(session) -> list[str]:
    rows = (await session.execute(text("SELECT event_type FROM inventory.outbox ORDER BY occurred_at"))).all()
    return [row.event_type for row in rows]


async def _expired_backlog(session) -> int:
    """The reaper's alert signal (see RUNBOOK §8): holds still `held` past their TTL."""
    return (
        await session.execute(
            text("SELECT count(*) FROM inventory.reservations WHERE status = 'held' AND expires_at <= now()")
        )
    ).scalar_one()


def _service(session) -> InventoryService:
    return InventoryService(InventoryRepository(session), reservation_ttl_seconds=900)


async def test_reserve_holds_stock_and_emits_event_in_one_transaction(session):
    await _seed(session, "sku-hold", 5)

    reservation = await _service(session).reserve("sku-hold", 2, uuid.uuid4())

    assert reservation.status is ReservationStatus.HELD
    assert reservation.expires_at > datetime.now(UTC)
    # on_hand untouched (it's a hold, not a sale); reserved bumped.
    assert await _stock(session, "sku-hold") == (5, 2)
    # The reservation row and its outbox row landed together.
    assert await _outbox_types(session) == ["StockReserved"]


async def test_reserve_rejects_when_stock_does_not_cover_and_writes_no_event(session):
    await _seed(session, "sku-short", 1)
    before = _metric("inventory_oversell_blocked_total")

    with pytest.raises(InsufficientStockError):
        await _service(session).reserve("sku-short", 2, uuid.uuid4())

    assert await _stock(session, "sku-short") == (1, 0)  # rolled back, nothing held
    assert await _outbox_types(session) == []  # no event for a rejected hold
    assert _metric("inventory_oversell_blocked_total") == before + 1


async def test_n_parallel_reservations_for_one_unit_yield_exactly_one_winner(async_engine, session):
    """The oversell test: 20 concurrent checkouts, 1 unit, 1 winner."""
    await _seed(session, "sku-race", 1)
    maker = async_sessionmaker(async_engine, expire_on_commit=False)
    racers = 20

    async def attempt() -> bool:
        async with maker() as own_session:  # each racer owns its session/transaction
            try:
                await _service(own_session).reserve("sku-race", 1, uuid.uuid4())
            except InsufficientStockError:
                return False
            return True

    results = await asyncio.gather(*(attempt() for _ in range(racers)))

    assert sum(results) == 1, "exactly one racer may win the last unit"
    assert await _stock(session, "sku-race") == (1, 1)
    assert await _outbox_types(session) == ["StockReserved"]


async def test_reserve_retry_racing_a_release_does_not_deadlock(async_engine, session):
    """The lock-order regression: reserve and release must grab the pair in the same order.

    Both touch the reservation row *and* the inventory row. Release goes
    reservation → inventory. If reserve went inventory → reservation, this exact
    interleaving is a cycle: Postgres detects it after ``deadlock_timeout`` and
    kills one side with a 40P01 that surfaces to the caller as a 500.

    Staged deliberately: the releaser takes the reservation row lock and holds it
    across a pause, and the retry starts inside that window. The SKU keeps a spare
    unit free so the retry's decrement *succeeds* and actually takes the inventory
    lock — with a sold-out SKU it would reject first and never grab the second lock.
    """
    await _seed(session, "sku-deadlock", 2)
    order_id = uuid.uuid4()
    held = await _service(session).reserve("sku-deadlock", 1, order_id)
    maker = async_sessionmaker(async_engine, expire_on_commit=False)

    async def slow_release() -> None:
        async with maker() as own:
            await own.execute(
                text("UPDATE inventory.reservations SET status = 'released' WHERE id = :id"), {"id": held.id}
            )
            await asyncio.sleep(0.3)  # hold the reservation lock; let the retry get in
            await own.execute(text("UPDATE inventory.inventory SET reserved = reserved - 1 WHERE sku = 'sku-deadlock'"))
            await own.commit()

    async def retry_reserve() -> None:
        await asyncio.sleep(0.1)  # land inside the releaser's window
        async with maker() as own:
            await _service(own).reserve("sku-deadlock", 1, order_id)

    # No deadlock: the retry queues behind the releaser instead of racing it.
    await asyncio.wait_for(asyncio.gather(slow_release(), retry_reserve()), timeout=15)

    assert await _stock(session, "sku-deadlock") == (2, 1)  # released 1, re-reserved 1


async def test_retrying_the_same_order_line_is_idempotent(session):
    await _seed(session, "sku-retry", 5)
    order_id = uuid.uuid4()
    service = _service(session)

    first = await service.reserve("sku-retry", 2, order_id)
    second = await service.reserve("sku-retry", 2, order_id)

    assert second.id == first.id  # the existing hold, not a second one
    assert await _stock(session, "sku-retry") == (5, 2)  # the retry's decrement was rolled back


async def test_retry_is_idempotent_even_when_the_hold_consumed_all_stock(session):
    """The regression: the first hold takes everything, so the retry's CAS rejects.

    A rejected decrement doesn't mean "out of stock" here — the order's own hold is
    what's blocking it. The retry must still get its reservation back, not a 409.
    """
    await _seed(session, "sku-all", 1)
    order_id = uuid.uuid4()
    service = _service(session)

    first = await service.reserve("sku-all", 1, order_id)
    assert await _stock(session, "sku-all") == (1, 1)  # nothing free left

    second = await service.reserve("sku-all", 1, order_id)

    assert second.id == first.id
    assert await _stock(session, "sku-all") == (1, 1)
    assert await _outbox_types(session) == ["StockReserved"]  # no duplicate event


async def test_a_different_order_still_gets_rejected_when_stock_is_exhausted(session):
    """The retry lookup must not hand another order someone else's hold."""
    await _seed(session, "sku-taken", 1)
    service = _service(session)
    await service.reserve("sku-taken", 1, uuid.uuid4())

    with pytest.raises(InsufficientStockError):
        await service.reserve("sku-taken", 1, uuid.uuid4())


async def test_retry_with_a_changed_quantity_conflicts_instead_of_reading_as_no_stock(session):
    """A retry that changed the qty isn't the same request — and isn't an oversell either.

    It must not silently receive the old hold, and it must not inflate
    ``inventory_oversell_blocked_total``: the caller contradicted itself, stock
    pressure had nothing to do with it.
    """
    await _seed(session, "sku-qty", 10)  # plenty free, so this cannot be a stock problem
    order_id = uuid.uuid4()
    service = _service(session)
    await service.reserve("sku-qty", 1, order_id)
    oversells = _metric("inventory_oversell_blocked_total")
    conflicts = _metric("inventory_reservation_conflict_total")

    with pytest.raises(ReservationConflictError):
        await service.reserve("sku-qty", 5, order_id)

    assert _metric("inventory_oversell_blocked_total") == oversells  # not an oversell
    assert _metric("inventory_reservation_conflict_total") == conflicts + 1


# --- batch reserve (the checkout saga's one-transaction reserve step) -------


async def test_reserve_many_holds_every_line_and_emits_one_event_each(session):
    await _seed(session, "sku-batch-a", 5)
    await _seed(session, "sku-batch-b", 3)
    order_id = uuid.uuid4()

    reservations = await _service(session).reserve_many([("sku-batch-a", 2), ("sku-batch-b", 1)], order_id)

    assert {r.sku for r in reservations} == {"sku-batch-a", "sku-batch-b"}
    assert all(r.status is ReservationStatus.HELD for r in reservations)
    assert await _stock(session, "sku-batch-a") == (5, 2)
    assert await _stock(session, "sku-batch-b") == (3, 1)
    assert await _outbox_types(session) == ["StockReserved", "StockReserved"]


async def test_reserve_many_is_all_or_nothing_when_one_line_falls_short(session):
    """A rejected line rolls the whole batch back — nothing partial to compensate."""
    await _seed(session, "sku-plenty", 5)
    await _seed(session, "sku-short-batch", 1)
    before = _metric("inventory_oversell_blocked_total")

    with pytest.raises(InsufficientStockError) as excinfo:
        await _service(session).reserve_many([("sku-plenty", 2), ("sku-short-batch", 5)], uuid.uuid4())

    assert excinfo.value.sku == "sku-short-batch"
    # The cover-able line was rolled back with the rejected one: no holds, no
    # stock moved, no events — the saga's compensation finds nothing to release.
    assert await _stock(session, "sku-plenty") == (5, 0)
    assert await _stock(session, "sku-short-batch") == (1, 0)
    assert await _outbox_types(session) == []
    assert _metric("inventory_oversell_blocked_total") == before + 1


async def test_reserve_many_rejects_an_unstocked_sku_without_holding_the_rest(session):
    """A SKU with no inventory row is unavailability (409), not a 500 — and still all-or-nothing."""
    await _seed(session, "sku-stocked", 5)
    before = _metric("inventory_oversell_blocked_total")

    with pytest.raises(InsufficientStockError) as excinfo:
        await _service(session).reserve_many([("sku-stocked", 1), ("sku-never-seeded", 1)], uuid.uuid4())

    assert excinfo.value.sku == "sku-never-seeded"
    assert await _stock(session, "sku-stocked") == (5, 0)
    assert await _outbox_types(session) == []
    assert _metric("inventory_oversell_blocked_total") == before + 1


async def test_reserve_many_retry_is_idempotent_and_never_double_deducts(session):
    """A replay of a landed batch (the saga's timeout ambiguity) returns the same holds."""
    await _seed(session, "sku-replay-a", 2)
    await _seed(session, "sku-replay-b", 4)
    order_id = uuid.uuid4()
    service = _service(session)

    first = await service.reserve_many([("sku-replay-a", 2), ("sku-replay-b", 3)], order_id)
    # sku-replay-a is now fully held — the replay's CAS would reject it, and must
    # still get its reservation back rather than reading as out of stock.
    second = await service.reserve_many([("sku-replay-a", 2), ("sku-replay-b", 3)], order_id)

    assert {r.sku: r.id for r in second} == {r.sku: r.id for r in first}
    assert await _stock(session, "sku-replay-a") == (2, 2)
    assert await _stock(session, "sku-replay-b") == (4, 3)
    assert await _outbox_types(session) == ["StockReserved", "StockReserved"]  # no duplicate events


async def test_reserve_many_with_a_changed_quantity_conflicts_instead_of_double_deducting(session):
    await _seed(session, "sku-batch-qty", 10)
    order_id = uuid.uuid4()
    service = _service(session)
    await service.reserve_many([("sku-batch-qty", 1)], order_id)
    oversells = _metric("inventory_oversell_blocked_total")
    conflicts = _metric("inventory_reservation_conflict_total")

    with pytest.raises(ReservationConflictError):
        await service.reserve_many([("sku-batch-qty", 4)], order_id)

    assert _metric("inventory_oversell_blocked_total") == oversells  # not an oversell
    assert _metric("inventory_reservation_conflict_total") == conflicts + 1
    assert await _stock(session, "sku-batch-qty") == (10, 1)  # the original hold, untouched


async def test_reserve_many_conflict_rolls_back_the_lines_it_already_placed(session):
    """A mid-batch conflict must not leak the earlier lines' pending work.

    The fresh line sorts *before* the conflicting one, so the conflict surfaces
    with its insert/decrement/outbox row still pending in the transaction. The
    raise must roll that back: a dirty session would let the saga's compensation
    (same request-scoped session) commit phantom ``StockReserved`` events for a
    batch that never landed.
    """
    await _seed(session, "sku-aaa-fresh", 5)  # sorts first
    await _seed(session, "sku-zzz-held", 5)
    order_id = uuid.uuid4()
    service = _service(session)
    await service.reserve("sku-zzz-held", 1, order_id)  # the line the retry contradicts

    with pytest.raises(ReservationConflictError):
        await service.reserve_many([("sku-aaa-fresh", 2), ("sku-zzz-held", 4)], order_id)

    assert await _stock(session, "sku-aaa-fresh") == (5, 0)  # rolled back, nothing held
    assert await _stock(session, "sku-zzz-held") == (5, 1)  # the original hold, untouched
    assert await _outbox_types(session) == ["StockReserved"]  # only the original hold's event
    # And the session is clean: a following write commits nothing stray.
    await service.release_for_order(order_id)
    assert await _outbox_types(session) == ["StockReserved", "StockReleased"]


async def test_reserve_many_rejects_duplicate_skus_and_non_positive_quantities(session):
    await _seed(session, "sku-dup", 10)
    service = _service(session)

    with pytest.raises(InvalidReservationError):
        await service.reserve_many([("sku-dup", 1), ("sku-dup", 2)], uuid.uuid4())
    with pytest.raises(InvalidReservationError):
        await service.reserve_many([("sku-dup", 0)], uuid.uuid4())

    assert await _stock(session, "sku-dup") == (10, 0)  # nothing held, nothing attempted


async def test_parallel_batches_racing_the_same_unit_yield_exactly_one_winner(async_engine, session):
    """Two overlapping batches, one scarce SKU: no deadlock, no oversell, no split holds.

    Both batches take the inventory row locks in SKU-sorted order, so they queue
    on the shared SKU instead of cycling; the loser's CAS rejects and its whole
    batch — including the line it *could* have covered — rolls back.
    """
    await _seed(session, "sku-shared", 1)
    await _seed(session, "sku-only-a", 5)
    await _seed(session, "sku-only-b", 5)
    maker = async_sessionmaker(async_engine, expire_on_commit=False)

    async def attempt(private_sku: str) -> bool:
        async with maker() as own_session:
            try:
                await _service(own_session).reserve_many([(private_sku, 1), ("sku-shared", 1)], uuid.uuid4())
            except InsufficientStockError:
                return False
            return True

    results = await asyncio.gather(attempt("sku-only-a"), attempt("sku-only-b"))

    assert sum(results) == 1, "exactly one batch may win the shared unit"
    assert await _stock(session, "sku-shared") == (1, 1)
    # The winner's private line landed; the loser's rolled back with its batch.
    assert sorted([await _stock(session, "sku-only-a"), await _stock(session, "sku-only-b")]) == [(5, 0), (5, 1)]


async def test_retry_after_commit_does_not_deduct_stock_twice(session):
    """The committed-line regression: uniqueness must outlive the `held` status.

    A saga retry arriving after payment committed the line used to slip past a
    `held`-only guard, place a second reservation and decrement the same units
    again. It must get the committed reservation back instead.
    """
    await _seed(session, "sku-committed", 5)
    order_id = uuid.uuid4()
    service = _service(session)
    first = await service.reserve("sku-committed", 2, order_id)
    assert await service.commit_reservation(first.id) is True
    assert await _stock(session, "sku-committed") == (3, 0)  # 2 units really deducted

    late_retry = await service.reserve("sku-committed", 2, order_id)

    assert late_retry.id == first.id
    assert await _stock(session, "sku-committed") == (3, 0)  # NOT deducted a second time
    assert await _outbox_types(session) == ["StockReserved"]


async def test_a_released_line_can_be_reserved_again(session):
    """The uniqueness guard must stay narrow enough to allow a legitimate re-reserve."""
    await _seed(session, "sku-rereserve", 4)
    order_id = uuid.uuid4()
    service = _service(session)
    first = await service.reserve("sku-rereserve", 1, order_id)
    await service.release(first.id)

    second = await service.reserve("sku-rereserve", 1, order_id)

    assert second.id != first.id
    assert await _stock(session, "sku-rereserve") == (4, 1)


async def test_release_returns_stock_once_and_is_a_no_op_on_replay(session):
    await _seed(session, "sku-release", 3)
    service = _service(session)
    reservation = await service.reserve("sku-release", 3, uuid.uuid4())

    assert await service.release(reservation.id) is True
    assert await _stock(session, "sku-release") == (3, 0)
    assert await _outbox_types(session) == ["StockReserved", "StockReleased"]

    # Replayed compensation: no double-release, no duplicate event.
    assert await service.release(reservation.id) is False
    assert await _stock(session, "sku-release") == (3, 0)
    assert await _outbox_types(session) == ["StockReserved", "StockReleased"]


async def test_commit_turns_a_hold_into_a_deduction_and_survives_the_reaper(session, sessionmaker_factory):
    await _seed(session, "sku-paid", 4)
    service = _service(session)
    reservation = await service.reserve("sku-paid", 1, uuid.uuid4())

    assert await service.commit_reservation(reservation.id) is True
    assert await _stock(session, "sku-paid") == (3, 0)  # sold: on_hand and reserved both drop

    await _expire_all(session)
    assert await _reaper(sessionmaker_factory).sweep_once() == 0


async def test_commit_for_order_consumes_every_hold_and_replays_the_committed_total(session):
    await _seed(session, "sku-whole-a", 5)
    await _seed(session, "sku-whole-b", 5)
    order_id = uuid.uuid4()
    service = _service(session)
    await service.reserve_many([("sku-whole-a", 2), ("sku-whole-b", 1)], order_id)

    assert await service.commit_for_order(order_id, expected=2) == 2
    assert await _stock(session, "sku-whole-a") == (3, 0)
    assert await _stock(session, "sku-whole-b") == (4, 0)

    # A retry whose first attempt landed reports the end-state, not a false 0.
    assert await service.commit_for_order(order_id, expected=2) == 2
    assert await _stock(session, "sku-whole-a") == (3, 0)


async def test_restock_for_order_returns_committed_stock_once(session):
    """A full commit followed by the order dying: ``release_for_order`` cannot
    reach the ``committed`` rows, ``restock_for_order`` gives the units back —
    idempotently, with one ``StockReleased`` per line."""
    await _seed(session, "sku-restock-a", 5)
    await _seed(session, "sku-restock-b", 5)
    order_id = uuid.uuid4()
    service = _service(session)
    await service.reserve_many([("sku-restock-a", 2), ("sku-restock-b", 1)], order_id)
    assert await service.commit_for_order(order_id, expected=2) == 2
    assert await service.release_for_order(order_id) == 0  # committed rows are out of its reach
    assert await _stock(session, "sku-restock-a") == (3, 0)

    assert await service.restock_for_order(order_id) == 2
    assert await _stock(session, "sku-restock-a") == (5, 0)
    assert await _stock(session, "sku-restock-b") == (5, 0)

    assert await service.restock_for_order(order_id) == 0  # replay: nothing left to reverse
    assert await _stock(session, "sku-restock-a") == (5, 0)
    assert await _outbox_types(session) == ["StockReserved", "StockReserved", "StockReleased", "StockReleased"]


async def test_commit_for_order_consumes_nothing_when_a_hold_was_released_first(session):
    """All-or-nothing: the surviving hold must stay ``held`` so the compensation
    can give it back — consuming it would deduct stock on an order that gets cancelled."""
    await _seed(session, "sku-short-a", 5)
    await _seed(session, "sku-short-b", 5)
    order_id = uuid.uuid4()
    service = _service(session)
    reserved = await service.reserve_many([("sku-short-a", 2), ("sku-short-b", 1)], order_id)
    reaped = next(r for r in reserved if r.sku == "sku-short-b")
    assert await service.release(reaped.id) is True  # what the reaper does to an expired hold

    assert await service.commit_for_order(order_id, expected=2) == 0  # shortfall: nothing consumed
    assert await _stock(session, "sku-short-a") == (5, 2)  # still held, on_hand untouched
    assert await _stock(session, "sku-short-b") == (5, 0)

    # The caller's compensation now returns the survivor to the pool.
    assert await service.release_for_order(order_id) == 1
    assert await _stock(session, "sku-short-a") == (5, 0)


async def test_two_concurrent_reapers_split_the_expired_batch_without_double_release(
    async_engine, session, sessionmaker_factory
):
    """The multi-replica reaper race : two reapers start the
    same instant against six expired holds, batch size 4 — the ``FOR UPDATE SKIP
    LOCKED`` claim splits the batch (4 + 2 in some order; neither can take all
    six in one sweep), no hold is released twice, and the stock is returned
    exactly once."""
    await _seed(session, "sku-two-reapers", 6)
    service = _service(session)
    for _ in range(6):
        await service.reserve("sku-two-reapers", 1, uuid.uuid4())
    assert await _stock(session, "sku-two-reapers") == (6, 6)

    await _expire_all(session)  # the sagas stalled; the TTL lapsed
    assert await _expired_backlog(session) == 6
    before = _metric("inventory_reaper_released_total")

    reaper_a = _reaper(sessionmaker_factory, batch_size=4)
    reaper_b = _reaper(sessionmaker_factory, batch_size=4)
    released = await asyncio.wait_for(asyncio.gather(reaper_a.sweep_once(), reaper_b.sweep_once()), timeout=15)

    assert sum(released) == 6, f"the claim must split the whole batch: {released}"
    assert min(released) > 0, "the SKIP LOCKED claim must split, not let one reaper take everything"
    assert _metric("inventory_reaper_released_total") == before + 6
    assert await _expired_backlog(session) == 0  # backlog drained
    assert await _stock(session, "sku-two-reapers") == (6, 0)  # returned exactly once
    events = (
        await session.execute(text("SELECT count(*) FROM inventory.outbox WHERE event_type = 'StockReleased'"))
    ).scalar_one()
    assert events == 6  # one release event per hold — never a duplicate


async def test_reaper_releases_expired_holds_and_restores_available_stock(session, sessionmaker_factory):
    await _seed(session, "sku-stalled", 2)
    service = _service(session)
    await service.reserve("sku-stalled", 2, uuid.uuid4())
    assert await _stock(session, "sku-stalled") == (2, 2)  # nothing sellable while held

    await _expire_all(session)  # the saga stalled; the TTL lapsed
    assert await _expired_backlog(session) == 1  # what the reaper alert watches
    before = _metric("inventory_reaper_released_total")

    released = await _reaper(sessionmaker_factory).sweep_once()

    assert released == 1
    assert _metric("inventory_reaper_released_total") == before + 1
    assert await _expired_backlog(session) == 0  # backlog drained, so the alert clears
    assert await _stock(session, "sku-stalled") == (2, 0)  # available again
    assert await _outbox_types(session) == ["StockReserved", "StockReleased"]
    # A second sweep finds nothing: the released row is no longer `held`.
    assert await _reaper(sessionmaker_factory).sweep_once() == 0


async def test_repository_release_expired_leaves_unexpired_holds_alone(session):
    await _seed(session, "sku-fresh", 5)
    await _service(session).reserve("sku-fresh", 1, uuid.uuid4())

    released = await InventoryRepository(session).release_expired(batch_size=10, outbox_factory=stock_released_outbox)

    assert released == 0
    assert await _stock(session, "sku-fresh") == (5, 1)


async def test_a_stock_mutation_that_matches_no_row_rolls_back_instead_of_losing_units(session):
    """The guard on `_UNRESERVE_SQL`: if the counters disagree, fail loudly.

    Corrupt `reserved` behind the reservation's back, then release. Unchecked, the
    status flip and the `StockReleased` event would commit while the stock never
    moved — those units silently vanish from the pool forever.
    """
    await _seed(session, "sku-corrupt", 5)
    service = _service(session)
    reservation = await service.reserve("sku-corrupt", 3, uuid.uuid4())
    await session.execute(text("UPDATE inventory.inventory SET reserved = 0 WHERE sku = 'sku-corrupt'"))
    await session.commit()

    with pytest.raises(StockMutationError):
        await service.release(reservation.id)

    status = (
        await session.execute(text("SELECT status FROM inventory.reservations WHERE id = :id"), {"id": reservation.id})
    ).scalar_one()
    assert status == ReservationStatus.HELD.value  # rolled back, not half-applied
    assert await _outbox_types(session) == ["StockReserved"]  # no phantom release event


async def test_reserving_an_unknown_sku_reads_as_no_stock_not_a_crash(session):
    """FK violation ≠ duplicate. It used to fall into the duplicate lookup and 500."""
    service = _service(session)

    with pytest.raises(InsufficientStockError):
        await service.reserve("sku-does-not-exist", 1, uuid.uuid4())


async def test_a_non_positive_quantity_is_rejected_explicitly(session):
    """Not an oversell and not a 500 — no stock level makes qty <= 0 valid."""
    await _seed(session, "sku-badqty", 5)
    service = _service(session)
    oversells = _metric("inventory_oversell_blocked_total")

    for bad_qty in (0, -3):
        with pytest.raises(InvalidReservationError):
            await service.reserve("sku-badqty", bad_qty, uuid.uuid4())

    assert _metric("inventory_oversell_blocked_total") == oversells
    assert await _stock(session, "sku-badqty") == (5, 0)


async def test_a_check_violation_from_the_db_is_still_reported_as_invalid(session):
    """The service guard is the fast path; the DB CHECK is the backstop.

    Three layers reject a non-positive quantity: the service guard, the event
    schema (`StockChangeData.quantity > 0`), and `ck_reservations_qty_positive`.
    To reach the third the first two have to be stepped around — call the
    repository directly, and build the outbox message with a legal quantity. The
    CHECK must surface as an explicit rejection, not a duplicate lookup or a 500.
    """
    await _seed(session, "sku-checkbypass", 5)
    repo = InventoryRepository(session)

    with pytest.raises(InvalidReservationError):
        await repo.reserve(
            sku="sku-checkbypass",
            qty=0,
            order_id=uuid.uuid4(),
            expires_at=datetime.now(UTC) + timedelta(minutes=5),
            outbox=stock_reserved_outbox("sku-checkbypass", uuid.uuid4(), 1),
        )


async def test_retry_exhaustion_raises_contention_and_does_not_count_as_an_oversell(session):
    """Losing the uniqueness race twice is churn on the line, not a stock answer.

    Staged deterministically: an existing hold for the same line makes every
    INSERT hit ``uq_reservations_active_order_sku``, while a stubbed lookup never
    finds it (the mid-flight-release case pushed to its limit). The reserve must
    raise ``ReservationContendedError`` — and leave ``oversell_blocked_total``
    alone, since free stock was never the problem.
    """
    await _seed(session, "sku-churn", 5)
    order_id = uuid.uuid4()
    await _service(session).reserve("sku-churn", 1, order_id)  # the line's live hold

    repo = InventoryRepository(session)

    async def _never_found(*args, **kwargs):  # noqa: ANN002, ANN003
        return {}

    repo._active_map = _never_found
    oversells_before = _metric("inventory_oversell_blocked_total")

    with pytest.raises(ReservationContendedError):
        await InventoryService(repo, reservation_ttl_seconds=900).reserve("sku-churn", 1, order_id)

    assert _metric("inventory_oversell_blocked_total") == oversells_before


async def test_retry_succeeds_when_the_conflicting_hold_is_released_mid_flight(session, async_engine):
    """The retry/release race: the duplicate row can vanish between INSERT and lookup.

    Staged by releasing the existing hold from another session at the moment the
    retry's lookup runs. The old code raised `NoResultFound` (a 500); the retry
    must simply place the reservation, since the line is now free.
    """
    await _seed(session, "sku-vanish", 4)
    order_id = uuid.uuid4()
    service = _service(session)
    first = await service.reserve("sku-vanish", 1, order_id)

    maker = async_sessionmaker(async_engine, expire_on_commit=False)
    repo = InventoryRepository(session)
    original_map = repo._active_map
    released = False

    async def _release_then_map(*args, **kwargs):
        nonlocal released
        if not released:  # only on the first lookup, i.e. mid-flight
            released = True
            async with maker() as other:
                await _service(other).release(first.id)
        return await original_map(*args, **kwargs)

    repo._active_map = _release_then_map
    retry = await InventoryService(repo, reservation_ttl_seconds=900).reserve("sku-vanish", 1, order_id)

    assert retry.id != first.id  # a fresh hold, because the old one was released
    assert await _stock(session, "sku-vanish") == (4, 1)


async def test_reserved_outbox_payload_is_a_valid_registered_event():
    message = stock_reserved_outbox("sku-1", uuid.uuid4(), 2)
    assert message.event_type == "StockReserved"
    validate_event(message.payload)  # the stored payload IS the wire body


async def _expire_all(session) -> None:
    """Push every held reservation's TTL into the past (simulates a stalled saga)."""
    await session.execute(
        text("UPDATE inventory.reservations SET expires_at = :past WHERE status = 'held'"),
        {"past": datetime.now(UTC) - timedelta(minutes=1)},
    )
    await session.commit()
