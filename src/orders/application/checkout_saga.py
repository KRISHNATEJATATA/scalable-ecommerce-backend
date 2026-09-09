"""Checkout saga orchestrator — the genuinely hard slice, in one state machine.

Order-first, synchronous, in-process: the saga creates the ``pending`` order
(to anchor reservations and the idempotency guard), reserves each line through
the inventory service, charges through the payments service, commits the holds,
then marks the order ``paid`` — journaling every step to ``saga_log`` as it
goes. Each step has a compensating action (release holds + cancel order), and a
crash between steps leaves a ``pending`` order the recovery poller settles from
the payment row's terminal state — never by re-presenting the payment token,
which is never stored.

Idempotency rides two layers: the Valkey fast path answers exact replays
without touching the DB, and ``UNIQUE(user_id, idempotency_key)`` plus the
stored body hash is the truth that survives Valkey eviction. Same key + same
body replays the stored response; same key + different body is 409 — at either
layer; a replay of an already-cancelled checkout re-raises its 409 rather
than returning the cancelled order as a fresh ``201``.

Cross-module calls go through the saga's own ports
(:mod:`src.orders.ports.checkout`), implemented at the composition root over
the inventory/payments/cart services — this module never imports a sibling.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import uuid
from collections import Counter
from datetime import datetime
from decimal import Decimal
from typing import Any

from src.orders.application.dto import OrderResponse
from src.orders.application.mappers import to_domain
from src.orders.application.metrics import (
    checkout_attempts_total,
    checkout_compensation_total,
    checkout_orphaned_paid_payments_total,
    checkout_recovery_total,
)
from src.orders.application.outbox import order_placed_outbox
from src.orders.domain.order import OrderStatus
from src.orders.ports.checkout import (
    BasketPort,
    ChargePort,
    CheckoutLine,
    IdempotencyPort,
    StockHoldsPort,
)
from src.orders.ports.repository import OrdersRepositoryPort
from src.shared.errors.exceptions import (
    CheckoutIdempotencyConflictError,
    InsufficientStockError,
    OrderStateConflictError,
)

log = logging.getLogger(__name__)


def payment_key_for(user_id: uuid.UUID, idempotency_key: str) -> str:
    """The gateway-level idempotency key for one checkout (stable across retries).

    Derived deterministically so the live saga, its own retries, and the
    token-less recovery poller all address the same payment attempt — the
    gateway and our ``UNIQUE`` then dedupe every one of those paths into a
    single charge.
    """
    return f"checkout:{user_id}:{idempotency_key}"


def body_hash_for(payment_token: str) -> str:
    """sha256 over the checkout body — which is just the payment token.

    The cart is server-side, so the request body carries only the gateway
    token; the token is what the key pins. Hashing the cart lines instead
    would make every replay look "different" (a completed checkout clears the
    cart), turning exact retries into 409s. The token itself is never stored —
    only this hash, compared on replay to tell "same retry" (replay the stored
    response) from "same key, different request" (409).
    """
    canonical = json.dumps({"payment_token": payment_token}, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(canonical.encode()).hexdigest()


def _response(row: Any) -> OrderResponse:
    """Map an ORM order (with lines) to the wire shape."""
    return OrderResponse.model_validate(to_domain(row))


class CheckoutSaga:
    """Drives one cart to exactly one terminal order (``paid`` or ``cancelled``)."""

    def __init__(
        self,
        orders: OrdersRepositoryPort,
        basket: BasketPort,
        holds: StockHoldsPort,
        charges: ChargePort,
        idempotency: IdempotencyPort | None,
        *,
        step_timeout_seconds: int,
    ) -> None:
        self._orders = orders
        self._basket = basket
        self._holds = holds
        self._charges = charges
        self._idempotency = idempotency
        self._step_timeout = step_timeout_seconds

    # --- the checkout entry point -------------------------------------

    async def checkout(
        self, *, user_id: uuid.UUID, idempotency_key: str, payment_token: str
    ) -> tuple[OrderResponse, bool]:
        """Run the saga; returns ``(order, created)`` (``created=False`` = exact replay).

        Idempotency is consulted *before* the cart: a completed checkout clears
        the basket, so a retry arrives with an empty cart and must still replay
        (only the token pins the key). A replay of a stored 201 clears the
        basket only when it still matches the order's lines (mop-up for a crash
        between the drive's clear and the replay record); a basket rebuilt
        after the checkout is never touched. Raises ``OrderStateConflictError``
        (409) for an empty cart, a declined payment, a timed-out reservation,
        or a replay of an already-cancelled checkout,
        ``CheckoutIdempotencyConflictError`` (409) for same key + different
        body, and ``InsufficientStockError`` (409) when the shelves refuse.
        Every failure path compensates before raising — no half-state escapes
        except through a process crash, which is the recovery poller's job.
        """
        body_hash = body_hash_for(payment_token)

        # One outcome increment per call, from a single try around the WHOLE
        # call body (the Valkey fast path included — a poisoned replay record
        # failing model_validate must still count): the success paths tag
        # ``paid`` / ``replayed`` at their returns, and the except arms tag
        # every controlled failure by its exception type (``error`` catches the
        # 500-shaped residue). ``_replay_or_resume`` raising through here is
        # counted exactly like a fresh drive failing — same caller-visible end.
        try:
            if self._idempotency is not None:
                record = await self._idempotency.get(user_id, idempotency_key)
                if record is not None:
                    if record.body_hash != body_hash:
                        raise CheckoutIdempotencyConflictError()
                    log.info("checkout replay served from idempotency record (user=%s)", user_id)
                    response = OrderResponse.model_validate(record.response)
                    if response.status == OrderStatus.PAID:
                        await self._clear_basket_if_replay_mop_up(user_id, response.items)
                    checkout_attempts_total.labels("replayed").inc()
                    return response, False

            existing = await self._orders.get_by_idempotency(user_id, idempotency_key)
            if existing is not None:
                order, created = await self._replay_or_resume(
                    user_id=user_id,
                    idempotency_key=idempotency_key,
                    payment_token=payment_token,
                    body_hash=body_hash,
                    order=existing,
                )
            else:
                lines = await self._basket.get_lines(user_id)
                if not lines:
                    raise OrderStateConflictError("cart is empty; nothing to check out")
                total = _total(lines)
                order, created = await self._orders.create_pending_order(
                    user_id=user_id,
                    idempotency_key=idempotency_key,
                    body_hash=body_hash,
                    total=total,
                    lines=[(line.product_id, line.name, line.unit_price, line.quantity) for line in lines],
                )
                if not created:
                    # Lost the create race: the winner's row is the truth — replay or
                    # resume it exactly as above.
                    order, created = await self._replay_or_resume(
                        user_id=user_id,
                        idempotency_key=idempotency_key,
                        payment_token=payment_token,
                        body_hash=body_hash,
                        order=order,
                    )
                else:
                    order, created = await self._drive(
                        order.id, user_id, idempotency_key, payment_token, lines, body_hash
                    )
            checkout_attempts_total.labels("replayed" if not created else "paid").inc()
            return order, created
        except InsufficientStockError:
            checkout_attempts_total.labels("out_of_stock").inc()
            raise
        except CheckoutIdempotencyConflictError:
            checkout_attempts_total.labels("idempotency_conflict").inc()
            raise
        except OrderStateConflictError:
            checkout_attempts_total.labels("conflict").inc()
            raise
        except Exception:
            checkout_attempts_total.labels("error").inc()
            raise

    async def _replay_or_resume(
        self,
        *,
        user_id: uuid.UUID,
        idempotency_key: str,
        payment_token: str,
        body_hash: str,
        order: Any,
    ) -> tuple[OrderResponse, bool]:
        """A pre-existing order under ``(user_id, key)``: replay, resume, or refuse it.

        * ``paid`` → the stored 201 (the contract's replay), fast-path remembered.
          The basket is cleared only when it still matches the order's lines —
          mop-up for a crash between the drive's clear and the replay record; a
          rebuilt basket is never touched.
        * ``pending`` → a crash between create and finish: drive it home from the
          order's own stored lines (the cart may have been cleared or changed
          since). The row pre-existed, so this is a replay even though this call
          drove it.
        * ``cancelled`` → the checkout under this key already failed (decline or
          stock refusal). Replaying it must re-raise the failure class (409),
          never return 201 with a cancelled body — a retry needs a NEW key.
        """
        if order.idempotency_body_hash != body_hash:
            raise CheckoutIdempotencyConflictError()
        if order.status == OrderStatus.PAID:
            response = _response(order)
            await self._remember(user_id, idempotency_key, body_hash, 201, response)
            await self._clear_basket_if_replay_mop_up(user_id, response.items)
            return response, False
        if order.status == OrderStatus.PENDING:
            log.info("resuming crashed checkout for order %s", order.id)
            response, _drove = await self._drive(
                order.id, user_id, idempotency_key, payment_token, _lines_of(order), body_hash
            )
            # The row pre-existed, so this is a replay even though this call drove it.
            return response, False
        raise OrderStateConflictError("this checkout was already cancelled; start a new one with a new Idempotency-Key")

    # --- the drive: reserve → charge → commit → paid -------------------

    async def _drive(
        self,
        order_id: uuid.UUID,
        user_id: uuid.UUID,
        idempotency_key: str,
        payment_token: str,
        lines: list[CheckoutLine],
        body_hash: str,
    ) -> tuple[OrderResponse, bool]:
        """Run the steps for a ``pending`` order; compensate-then-raise on failure.

        One rule overrides compensation: once the payment has succeeded, the
        saga never unwinds money — a later failure leaves the order ``pending``
        for the recovery poller instead of cancelling a paid checkout.

        Terminations are counted on ``checkout_attempts_total`` (``paid`` on the
        success path; the post-payment conversion below lands on ``conflict``
        — see the metrics module docstring); the compensation counter carries
        only the failed step, so one saga is never counted twice.
        """
        total = _total(lines)
        charged = False
        try:
            await self._log(order_id, "reserve", "started")
            try:
                async with asyncio.timeout(self._step_timeout):
                    for line in lines:
                        # SKU mapping str(product.id).
                        await self._holds.reserve(str(line.product_id), line.quantity, order_id)
            except TimeoutError as exc:
                # The reserve may have landed without its answer returning —
                # compensate whatever holds exist rather than leaking them.
                await self._compensate(order_id, "reserve")
                raise OrderStateConflictError("checkout reservation timed out") from exc
            await self._log(order_id, "reserve", "completed")

            await self._log(order_id, "charge", "started")
            # Last word before money moves: a cancel (or a compensating poller)
            # that landed while we were reserving must abort the charge here,
            # not after it. The canceller owns the release in that case — this
            # branch only refuses to charge a dead order.
            live = await self._orders.get_order(order_id)
            if live is None or live.status != OrderStatus.PENDING:
                log.error("checkout order %s was cancelled mid-drive before charging", order_id)
                raise OrderStateConflictError("the order was cancelled while checking out; start a new checkout")
            try:
                async with asyncio.timeout(self._step_timeout):
                    charge = await self._charges.charge(
                        order_id=order_id,
                        idempotency_key=payment_key_for(user_id, idempotency_key),
                        amount=total,
                        payment_token=payment_token,
                    )
            except TimeoutError:
                # The charge may have landed without its answer returning —
                # settle from the recorded outcome instead of guessing.
                charge = await self._charges.find_by_idempotency_key(payment_key_for(user_id, idempotency_key))
                if charge is None or not charge.failed:
                    # Unknown or still-pending outcome: the gateway may confirm a
                    # moment later, and compensating here could cancel an order
                    # whose payment succeeds (money taken, no order). Leave it
                    # pending for the reconciler/recovery poller — never unwind.
                    await self._log(order_id, "charge", "unknown")
                    raise OrderStateConflictError(
                        "payment outcome unknown; retry with the same Idempotency-Key to settle"
                    ) from None
                await self._log(order_id, "charge", "failed")
                await self._compensate(order_id, "charge")
                raise OrderStateConflictError("checkout payment timed out; the order was cancelled") from None
            if not charge.succeeded:
                await self._log(order_id, "charge", "failed")
                await self._compensate(order_id, "charge")
                raise OrderStateConflictError("payment was declined; the order was cancelled")
            charged = True
            await self._log(order_id, "charge", "completed")

            await self._log(order_id, "commit", "started")
            try:
                async with asyncio.timeout(self._step_timeout):
                    await self._holds.commit_for_order(order_id)
            except TimeoutError:
                # Commits are idempotent per order: finish what the timeout
                # interrupted rather than compensating a paid checkout. Still
                # bounded — an unbounded retry could hang the request forever.
                # A second failure leaves the order pending for the recovery
                # poller (see below) — never a cancel after money moved.
                try:
                    async with asyncio.timeout(self._step_timeout):
                        await self._holds.commit_for_order(order_id)
                except TimeoutError as exc:
                    raise OrderStateConflictError(
                        "checkout commit timed out; it will be settled automatically"
                    ) from exc

            order = await self._orders.get_order(order_id)
            if order is None:  # defensive: we created it moments ago
                raise RuntimeError(f"checkout order {order_id} vanished mid-saga")
            await self._log(order_id, "mark_paid", "started")
            paid = await self._orders.transition_status(
                order_id,
                expect=[OrderStatus.PENDING],
                to_status=OrderStatus.PAID,
                outbox=order_placed_outbox(
                    order_id=order.id, user_id=order.user_id, total=order.total, items=order.items
                ),
            )
            if paid is None:
                # Lost the guarded flip — the recovery poller or a cancel
                # settled the order concurrently. A poller settle is benign
                # (read the truth); a cancel after the payment succeeded is
                # money taken without an order: never return it as a created
                # order, surface it loudly for reconciliation.
                paid = await self._orders.get_order(order_id)
                if paid is None:  # defensive: settled means present
                    raise RuntimeError(f"checkout order {order_id} vanished mid-saga")
                if paid.status == OrderStatus.PAID:
                    log.info("checkout order %s settled concurrently; reading final state", order_id)
                else:
                    # No auto-heal owns this state — the payment reconciler only
                    # scans `pending` charges — so count it loudly and leave
                    # reconciliation to a human (docs/RUNBOOK.md §9).
                    checkout_orphaned_paid_payments_total.inc()
                    log.error(
                        "checkout order %s was cancelled while the payment was completing; "
                        "reconciliation required (paid payment row attached)",
                        order_id,
                    )
                    raise OrderStateConflictError(
                        "the order was cancelled while the payment was completing; it will be reconciled"
                    )
            else:
                await self._log(order_id, "mark_paid", "completed")
            response = _response(paid)
            if paid.status == OrderStatus.PAID:
                await self._clear_basket(user_id)
                await self._remember(user_id, idempotency_key, body_hash, 201, response)
            return response, True
        except (OrderStateConflictError, CheckoutIdempotencyConflictError):
            raise
        except Exception:
            if charged:
                # Money already moved: unwinding would take payment without an
                # order. Leave pending — the recovery poller completes it — and
                # say so loudly instead of compensating. Covers the mark_paid
                # writes above too: a DB hiccup there is the same "paid, leave
                # pending" shape, not a 500-shaped dead end.
                log.error("checkout failed after payment succeeded; leaving pending for recovery", exc_info=True)
                raise OrderStateConflictError(
                    "checkout could not complete after payment; it will be settled automatically"
                ) from None
            # Reserve-step stock rejections (409s) and malformed tokens (400)
            # land here too: the holds taken so far are released, the order is
            # cancelled, and the original error keeps its shape.
            await self._compensate(order_id, "reserve")
            raise

    async def _compensate(self, order_id: uuid.UUID, failed_step: str) -> None:
        """Release every hold taken for the order, then cancel it (reverse order of the drive).

        ``failed_step`` is ``reserve``/``charge`` on the live path and
        ``crashed`` from the recovery poller — it labels
        ``checkout_compensation_total``. A user-facing cancel is the mirror
        image of compensation (Orders glossary) and deliberately never lands
        here.
        """
        checkout_compensation_total.labels(failed_step).inc()
        await self._log(order_id, "compensate", "started")
        await self._holds.release_for_order(order_id)
        await self._orders.transition_status(order_id, expect=[OrderStatus.PENDING], to_status=OrderStatus.CANCELLED)
        await self._log(order_id, "compensate", "completed")
        log.info("checkout order %s compensated after %s failed", order_id, failed_step)

    # --- recovery: settle what a crash left pending ---------------------

    async def recover_stuck(self, *, cutoff: datetime, batch_size: int) -> dict[str, int]:
        """Settle crashed checkouts; returns ``{completed, compensated, deferred}`` counts.

        Per stuck order, the payment row decides — never the token, which is
        never stored: ``succeeded`` → commit holds + mark paid; ``failed`` or
        absent (crash before any charge) → release + cancel; still ``pending``
        → leave for the payment reconciler and count as deferred (the claim
        already re-leased the order, so the next pass retries it). Every
        outcome increments ``checkout_recovery_total`` — in the poller process,
        so it is exported via the worker metrics, not the API's ``/metrics``.
        """
        stuck = await self._orders.claim_stuck_pending(cutoff=cutoff, batch_size=batch_size)
        outcome = {"completed": 0, "compensated": 0, "deferred": 0}
        # Ids as plain values up front: a rollback anywhere in the batch expires
        # every loaded instance, so the loop re-reads each order fresh instead
        # of trusting claim-time attributes (lazy loads have no greenlet here).
        claimed_ids = [order.id for order in stuck]
        for order_id in claimed_ids:
            try:
                order = await self._orders.get_order(order_id)
                if order is None:  # dropped mid-batch: the next lease retries it
                    outcome["deferred"] += 1
                    checkout_recovery_total.labels("deferred").inc()
                    continue
                # Claim→settle gap: between the claim and here, a client retry
                # can start driving this same order (its resume path journals
                # immediately). Settling on top of a live drive would race it;
                # one indexed journal check closes the window.
                if await self._orders.has_recent_saga_activity(order_id, since=cutoff):
                    log.info("saga recovery skipped order %s: journal shows fresh activity", order_id)
                    outcome["deferred"] += 1
                    checkout_recovery_total.labels("deferred").inc()
                    continue
                settled = await self._settle_crashed(order)
            except Exception:  # boundary: one bad order must not stall the batch
                # The failing statement may have aborted the transaction; roll
                # it back so the remaining orders in the batch are not poisoned
                # by ``PendingRollbackError``.
                await self._orders.rollback()
                log.exception("recovery failed for order %s; lease will retry it", order_id)
                outcome["deferred"] += 1
                checkout_recovery_total.labels("deferred").inc()
                continue
            outcome[settled] += 1
            checkout_recovery_total.labels(settled).inc()
        if stuck:
            log.info("saga recovery settled %d stuck order(s): %s", len(stuck), outcome)
        return outcome

    async def _settle_crashed(self, order: Any) -> str:
        """Settle one crashed order; returns which counter to bump."""
        payment = await self._charges.find_by_idempotency_key(payment_key_for(order.user_id, order.idempotency_key))
        if payment is not None and payment.succeeded:
            await self._holds.commit_for_order(order.id)
            await self._orders.log_saga_step(order.id, "mark_paid", "started")
            paid = await self._orders.transition_status(
                order.id,
                expect=[OrderStatus.PENDING],
                to_status=OrderStatus.PAID,
                outbox=order_placed_outbox(
                    order_id=order.id, user_id=order.user_id, total=order.total, items=order.items
                ),
            )
            if paid is not None and paid.status == OrderStatus.PAID:
                await self._orders.log_saga_step(order.id, "mark_paid", "completed")
                await self._clear_basket(order.user_id)
            return "completed"
        if payment is None or payment.failed:
            await self._compensate(order.id, "crashed")
            return "compensated"
        return "deferred"  # payment still pending: the reconciler owns it for now

    # --- internals -------------------------------------------------------

    async def _log(self, order_id: uuid.UUID, step: str, status: str) -> None:
        await self._orders.log_saga_step(order_id, step, status)

    async def _remember(
        self,
        user_id: uuid.UUID,
        idempotency_key: str,
        body_hash: str,
        status: int,
        response: OrderResponse,
    ) -> None:
        """Best-effort fast-path write: losing it only costs a DB re-read, never correctness."""
        if self._idempotency is None:
            return
        try:
            await self._idempotency.put(user_id, idempotency_key, body_hash=body_hash, status=status, response=response)
        except Exception:  # boundary: Valkey is a cache here, not the truth
            log.warning("idempotency fast-path write failed; DB backstop still guards", exc_info=True)

    async def _clear_basket(self, user_id: uuid.UUID) -> None:
        """Best-effort cart clear: the order is already terminal, so a Valkey
        outage must not fail the checkout — the cart simply survives (its
        rolling TTL still expires it) and the next replay clears it."""
        try:
            await self._basket.clear(user_id)
        except Exception:  # boundary: cleanup, not correctness
            log.warning("basket clear failed after terminal checkout; cart survives", exc_info=True)

    async def _clear_basket_if_replay_mop_up(self, user_id: uuid.UUID, order_items: Any) -> None:
        """Replay-side basket mop-up: clear ONLY a basket that still matches the order.

        The drive's success path already clears the basket *before* the replay
        record is stored, so a normal replay sees an empty (or rebuilt) basket
        and must not touch it. This exists for the crash window between that
        clear and ``_remember`` — a replay record written with the cart still
        holding the order's lines. Clearing unconditionally here would silently
        empty a basket the user built after the original checkout, so compare
        the current basket against the order's lines (ids + quantities as a
        multiset; drifted ``unit_price``/``name`` via ProductUpdated refresh
        must not block the clear) and clear only when they match.
        """
        lines = await self._basket.get_lines(user_id)
        if not lines:
            return  # nothing to mop up (the common replay case)
        if Counter((line.product_id, line.quantity) for line in lines) != Counter(
            (item.product_id, item.quantity) for item in order_items
        ):
            return  # a basket rebuilt after checkout is never touched
        await self._clear_basket(user_id)


def _total(lines: list[CheckoutLine]) -> Decimal:
    """The order total from its snapshots (money stays Decimal end to end)."""
    return sum((line.unit_price * line.quantity for line in lines), Decimal("0"))


def _lines_of(order: Any) -> list[CheckoutLine]:
    """Rebuild checkout lines from the order's own stored snapshots.

    A crashed checkout resumes from what it already persisted — never from the
    live cart, which may have been cleared or changed since the first attempt.
    """
    return [
        CheckoutLine(
            product_id=item.product_id,
            name=item.product_name,
            unit_price=item.unit_price if isinstance(item.unit_price, Decimal) else Decimal(str(item.unit_price)),
            quantity=item.quantity,
        )
        for item in order.items
    ]
