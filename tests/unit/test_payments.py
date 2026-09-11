"""payments: stub gateway, idempotent charges, webhooks, reconciliation.

Runs against the shared Testcontainers-Postgres (never SQLite): the guarantees
under test live in Postgres semantics — ``UNIQUE(idempotency_key)`` dedup,
guarded ``pending → terminal`` transitions, and the outbox row committed inside
the flip's transaction. The gateway is the only mocked external.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from fastapi import HTTPException, Request
from sqlalchemy import text

from src.events.registry import validate_event
from src.payments.adapters.db.repository import PaymentsRepository
from src.payments.adapters.stub_gateway import DeferredChargeWindow, StubPaymentGateway, stub_gateway_from_settings
from src.payments.api.routes import MAX_WEBHOOK_BODY_BYTES, payment_webhook
from src.payments.application.service import PaymentsService
from src.payments.ports.gateway import GatewayOutcome
from src.shared.config.setting import AppSettings
from src.shared.errors.exception_handlers import _unknown_payment_ref_handler
from src.shared.errors.exceptions import (
    AuthenticationError,
    DependencyUnavailableError,
    InvalidPaymentMethodError,
    PaymentIdempotencyConflictError,
    UnknownPaymentRefError,
)

SECRET = "test-webhook-secret"


def _service(
    session, gateway: StubPaymentGateway | None = None, secret: str | None = SECRET, max_age: int | None = None
) -> PaymentsService:
    return PaymentsService(
        PaymentsRepository(session),
        gateway or StubPaymentGateway(),
        webhook_secret=secret,
        reconciliation_grace_seconds=30,
        reconciliation_max_age_seconds=max_age,
    )


def _charge_kwargs(order_id: uuid.UUID | None = None, *, token: str = "tok_visa_123") -> dict:
    return {
        "order_id": order_id or uuid.uuid4(),
        "idempotency_key": f"checkout-{uuid.uuid4()}",
        "amount": Decimal("42.50"),
        "payment_method_token": token,
    }


def _sign(body: bytes, secret: str | None = SECRET) -> str:
    return "sha256=" + hmac.new((secret or "").encode(), body, hashlib.sha256).hexdigest()


async def _outbox_types(session) -> list[str]:
    rows = (
        await session.execute(text("SELECT event_type FROM payments.outbox ORDER BY occurred_at, event_type"))
    ).all()
    return [r.event_type for r in rows]


async def _status_of(session, idempotency_key: str) -> tuple[str, str | None]:
    row = (
        await session.execute(
            text("SELECT status, failure_reason FROM payments.payments WHERE idempotency_key = :k"),
            {"k": idempotency_key},
        )
    ).one()
    return row.status, row.failure_reason


async def _backdate_pending(session, idempotency_key: str, seconds: int = 120) -> None:
    await session.execute(
        text(
            "UPDATE payments.payments SET created_at = :past, updated_at = :past "
            "WHERE idempotency_key = :k AND status = 'pending'"
        ),
        {"past": datetime.now(UTC) - timedelta(seconds=seconds), "k": idempotency_key},
    )
    await session.commit()


# --- the saga's Payment step ---------------------------------------------------


async def test_charge_succeeds_and_emits_payment_succeeded_in_one_transaction(session):
    kwargs = _charge_kwargs()
    response = await _service(session).charge(**kwargs)

    assert response.status == "succeeded"
    assert response.gateway_ref.startswith("stub_")
    assert response.failure_reason is None
    # The flip and its announcement are one atomic fact.
    assert await _outbox_types(session) == ["PaymentSucceeded"]
    payload = (await session.execute(text("SELECT payload FROM payments.outbox"))).scalar_one()
    validate_event(payload)


async def test_retried_charge_with_the_same_key_never_double_charges(session):
    service = _service(session)
    gateway = service._gateway
    kwargs = _charge_kwargs()

    first = await service.charge(**kwargs)
    second = await service.charge(**kwargs)  # full replay: same key, same everything

    assert first.gateway_ref == second.gateway_ref
    assert gateway is not None
    assert len(gateway._charges) == 1  # the gateway itself saw ONE charge
    assert await _outbox_types(session) == ["PaymentSucceeded"]  # decided once, announced once


async def test_retry_after_a_decline_returns_the_failed_outcome_without_recharging(session):
    service = _service(session)
    kwargs = _charge_kwargs(token="tok-declined-card")

    first = await service.charge(**kwargs)
    second = await service.charge(**kwargs)

    assert first.status == "failed"
    assert second.status == "failed"
    assert second.gateway_ref == first.gateway_ref
    status, reason = await _status_of(session, kwargs["idempotency_key"])
    assert status == "failed" and reason == "declined_by_stub"
    assert await _outbox_types(session) == ["PaymentFailed"]


async def test_a_pending_resume_flips_once_even_if_the_first_attempt_crashed(session, sessionmaker_factory):
    """Crash-after-charge recovery: the row exists, the answer never landed.

    Staged by inserting the pending row in a separate session (the process died
    between create and flip). The retry re-asks the gateway, which answers from
    its own idempotency window; exactly one event ships."""
    kwargs = _charge_kwargs()
    gateway = StubPaymentGateway()
    await gateway.charge(
        amount=kwargs["amount"],
        idempotency_key=kwargs["idempotency_key"],
        payment_method_token=kwargs["payment_method_token"],
    )

    async with sessionmaker_factory() as fresh_session:
        await PaymentsRepository(fresh_session).create_pending(
            order_id=kwargs["order_id"], idempotency_key=kwargs["idempotency_key"], amount=kwargs["amount"]
        )

    async with sessionmaker_factory() as fresh_session:
        response = await PaymentsService(PaymentsRepository(fresh_session), gateway, webhook_secret=SECRET).charge(
            **kwargs
        )

    assert response.status == "succeeded"
    async with sessionmaker_factory() as fresh_session:
        assert len(await _outbox_types(fresh_session)) == 1


async def test_replaying_the_key_for_another_order_or_amount_is_a_conflict(session):
    service = _service(session)
    kwargs = _charge_kwargs()
    await service.charge(**kwargs)

    with pytest.raises(PaymentIdempotencyConflictError):
        await service.charge(**{**kwargs, "amount": Decimal("99.99")})
    with pytest.raises(PaymentIdempotencyConflictError):
        await service.charge(**{**kwargs, "order_id": uuid.uuid4()})
    assert await _outbox_types(session) == ["PaymentSucceeded"]


async def test_raw_card_data_is_rejected_before_anything_is_stored(session):
    with pytest.raises(InvalidPaymentMethodError):
        await _service(session).charge(**_charge_kwargs(token="4242 4242 4242 4242"))
    with pytest.raises(InvalidPaymentMethodError):
        await _service(session).charge(**_charge_kwargs(token="4242-4242-4242-4242"))
    count = (await session.execute(text("SELECT count(*) FROM payments.payments"))).scalar_one()
    assert count == 0


# --- webhooks --------------------------------------------------------------------


def _webhook_body(key: str, *, type_: str = "payment.succeeded", ref: str | None = None) -> bytes:
    body: dict = {"type": type_, "idempotency_key": key}
    if ref:
        body["gateway_ref"] = ref
    if type_ == "payment.failed":
        body["reason"] = "issuer_declined"
    return json.dumps(body).encode()


async def test_webhook_applies_the_outcome_and_emits_the_event(session):
    kwargs = _charge_kwargs()
    service = _service(session)
    row, _ = await PaymentsRepository(session).create_pending(
        order_id=kwargs["order_id"], idempotency_key=kwargs["idempotency_key"], amount=kwargs["amount"]
    )

    body = _webhook_body(kwargs["idempotency_key"], ref="wh_ref_1")
    applied = await service.handle_webhook(body, _sign(body))

    assert applied is True
    status, _ = await _status_of(session, kwargs["idempotency_key"])
    assert status == "succeeded"
    assert await _outbox_types(session) == ["PaymentSucceeded"]
    assert row is not None


async def test_duplicate_and_out_of_order_webhooks_are_idempotent_no_ops(session):
    kwargs = _charge_kwargs()
    service = _service(session)
    await PaymentsRepository(session).create_pending(
        order_id=kwargs["order_id"], idempotency_key=kwargs["idempotency_key"], amount=kwargs["amount"]
    )

    success = _webhook_body(kwargs["idempotency_key"])
    failure = _webhook_body(kwargs["idempotency_key"], type_="payment.failed")

    assert await service.handle_webhook(success, _sign(success)) is True
    assert await service.handle_webhook(success, _sign(success)) is False  # exact duplicate
    assert await service.handle_webhook(failure, _sign(failure)) is False  # out-of-order loser

    status, _ = await _status_of(session, kwargs["idempotency_key"])
    assert status == "succeeded"  # the first decision stands
    assert await _outbox_types(session) == ["PaymentSucceeded"]  # never a second event


async def test_webhooks_without_a_valid_signature_are_refused(session):
    kwargs = _charge_kwargs()
    service = _service(session)
    body = _webhook_body(kwargs["idempotency_key"])

    with pytest.raises(AuthenticationError):
        await service.handle_webhook(body, None)
    with pytest.raises(AuthenticationError):
        await service.handle_webhook(body, "sha256=" + "0" * 64)
    tampered = _webhook_body(kwargs["idempotency_key"], type_="payment.failed")
    with pytest.raises(AuthenticationError):
        await service.handle_webhook(tampered, _sign(body))  # valid sig over DIFFERENT body


async def test_webhooks_fail_closed_without_a_configured_secret(session):
    service = _service(session, secret=None)
    body = _webhook_body("any-key")
    with pytest.raises(DependencyUnavailableError):
        await service.handle_webhook(body, _sign(body))


async def test_a_webhook_for_an_unknown_reference_is_a_visible_404(session):
    body = _webhook_body("never-seen-key")
    with pytest.raises(UnknownPaymentRefError):
        await _service(session).handle_webhook(body, _sign(body))


async def test_a_signed_non_object_webhook_body_is_a_404_not_a_500(session):
    body = b'["payment.succeeded"]'  # valid JSON, not an object
    with pytest.raises(UnknownPaymentRefError):
        await _service(session).handle_webhook(body, _sign(body))


async def test_a_non_ascii_signature_header_is_a_401_not_a_type_error(session):
    kwargs = _charge_kwargs()
    body = _webhook_body(kwargs["idempotency_key"])
    with pytest.raises(AuthenticationError):
        await _service(session).handle_webhook(body, "sha256=é" * 32)


# --- reconciliation ----------------------------------------------------------------


async def test_reconciliation_resolves_a_missed_webhook(session):
    kwargs = _charge_kwargs()
    gateway = StubPaymentGateway()
    service = _service(session, gateway=gateway)
    await PaymentsRepository(session).create_pending(
        order_id=kwargs["order_id"], idempotency_key=kwargs["idempotency_key"], amount=kwargs["amount"]
    )
    await gateway.charge(
        amount=kwargs["amount"],
        idempotency_key=kwargs["idempotency_key"],
        payment_method_token=kwargs["payment_method_token"],
    )
    await _backdate_pending(session, kwargs["idempotency_key"])  # past the grace window

    resolved = await service.reconcile(batch_size=10)

    assert resolved == 1
    status, _ = await _status_of(session, kwargs["idempotency_key"])
    assert status == "succeeded"
    assert await _outbox_types(session) == ["PaymentSucceeded"]


async def test_reconciliation_skips_rows_the_gateway_never_saw(session):
    kwargs = _charge_kwargs()
    await PaymentsRepository(session).create_pending(
        order_id=kwargs["order_id"], idempotency_key=kwargs["idempotency_key"], amount=kwargs["amount"]
    )
    await _backdate_pending(session, kwargs["idempotency_key"])

    assert await _service(session).reconcile(batch_size=10) == 0
    status, _ = await _status_of(session, kwargs["idempotency_key"])
    assert status == "pending"  # nothing known yet: leave it for a later pass


async def test_a_gateway_fault_on_one_row_does_not_stall_the_batch(session):
    class FlakyLookupGateway(StubPaymentGateway):
        """Fails lookups for ONE key (the wedged row), answers the rest normally."""

        def __init__(self, fail_key: str) -> None:
            super().__init__()
            self._fail_key = fail_key

        async def lookup(self, idempotency_key: str):
            if idempotency_key == self._fail_key:
                raise RuntimeError("gateway down")
            return await super().lookup(idempotency_key)

    stuck, healthy = _charge_kwargs(), _charge_kwargs()
    repo = PaymentsRepository(session)
    await repo.create_pending(**{k: stuck[k] for k in ("order_id", "idempotency_key", "amount")})
    await repo.create_pending(**{k: healthy[k] for k in ("order_id", "idempotency_key", "amount")})
    await _backdate_pending(session, stuck["idempotency_key"])
    await _backdate_pending(session, healthy["idempotency_key"])

    flaky = FlakyLookupGateway(stuck["idempotency_key"])  # wedged row explodes...
    await flaky.charge(  # ...but the healthy charge lives in ITS idempotency window
        amount=healthy["amount"],
        idempotency_key=healthy["idempotency_key"],
        payment_method_token=healthy["payment_method_token"],
    )
    resolved = await _service(session, gateway=flaky).reconcile(batch_size=10)

    assert resolved == 1  # the healthy row resolved despite the stuck row's fault
    status, _ = await _status_of(session, healthy["idempotency_key"])
    assert status == "succeeded"


async def test_reconciliation_abandons_stale_pending_the_gateway_never_saw(session):
    kwargs = _charge_kwargs()
    await PaymentsRepository(session).create_pending(
        order_id=kwargs["order_id"], idempotency_key=kwargs["idempotency_key"], amount=kwargs["amount"]
    )
    await _backdate_pending(session, kwargs["idempotency_key"], seconds=3600)

    resolved = await _service(session, gateway=StubPaymentGateway(), max_age=60).reconcile(batch_size=10)

    assert resolved == 1
    status, reason = await _status_of(session, kwargs["idempotency_key"])
    assert status == "failed" and reason == "abandoned_by_reconciler"
    assert await _outbox_types(session) == ["PaymentFailed"]
    payload = (await session.execute(text("SELECT payload FROM payments.outbox"))).scalar_one()
    validate_event(payload)


async def test_reconciliation_does_not_abandon_when_lookups_keep_failing(session):
    class AlwaysDownGateway(StubPaymentGateway):
        async def lookup(self, idempotency_key: str):
            raise RuntimeError("gateway down")

    kwargs = _charge_kwargs()
    await PaymentsRepository(session).create_pending(
        order_id=kwargs["order_id"], idempotency_key=kwargs["idempotency_key"], amount=kwargs["amount"]
    )
    await _backdate_pending(session, kwargs["idempotency_key"], seconds=3600)

    assert await _service(session, gateway=AlwaysDownGateway(), max_age=60).reconcile(batch_size=10) == 0
    status, _ = await _status_of(session, kwargs["idempotency_key"])
    assert status == "pending"  # a down gateway abandons nothing
    assert await _outbox_types(session) == []


async def test_reconciliation_resolves_a_stale_charge_the_gateway_did_see(session):
    """Past max_age but known gateway-side: money may have moved, so the gateway's
    answer still wins — abandonment is only for affirmative 'never saw it'."""
    kwargs = _charge_kwargs()
    gateway = StubPaymentGateway()
    await gateway.charge(
        amount=kwargs["amount"],
        idempotency_key=kwargs["idempotency_key"],
        payment_method_token=kwargs["payment_method_token"],
    )
    await PaymentsRepository(session).create_pending(
        order_id=kwargs["order_id"], idempotency_key=kwargs["idempotency_key"], amount=kwargs["amount"]
    )
    await _backdate_pending(session, kwargs["idempotency_key"], seconds=3600)

    resolved = await _service(session, gateway=gateway, max_age=60).reconcile(batch_size=10)

    assert resolved == 1
    status, _ = await _status_of(session, kwargs["idempotency_key"])
    assert status == "succeeded"
    assert await _outbox_types(session) == ["PaymentSucceeded"]


async def test_bounded_sweep_resolves_fresh_rows_while_abandoning_stale_ones(session):
    """One orphaned row must not starve the batch: the sweep window skips it, the
    abandon pass retires it, and the fresh stuck row still resolves — even with
    batch_size=1. (On the old unbounded sweep the dead row eats the only slot.)"""
    dead, healthy = _charge_kwargs(), _charge_kwargs()
    repo = PaymentsRepository(session)
    await repo.create_pending(**{k: dead[k] for k in ("order_id", "idempotency_key", "amount")})
    await repo.create_pending(**{k: healthy[k] for k in ("order_id", "idempotency_key", "amount")})
    gateway = StubPaymentGateway()
    await gateway.charge(
        amount=healthy["amount"],
        idempotency_key=healthy["idempotency_key"],
        payment_method_token=healthy["payment_method_token"],
    )
    await _backdate_pending(session, dead["idempotency_key"], seconds=3600)
    await _backdate_pending(session, healthy["idempotency_key"], seconds=45)

    resolved = await _service(session, gateway=gateway, max_age=60).reconcile(batch_size=1)

    assert resolved == 2
    dead_status, dead_reason = await _status_of(session, dead["idempotency_key"])
    assert dead_status == "failed" and dead_reason == "abandoned_by_reconciler"
    healthy_status, _ = await _status_of(session, healthy["idempotency_key"])
    assert healthy_status == "succeeded"
    assert sorted(await _outbox_types(session)) == ["PaymentFailed", "PaymentSucceeded"]


# --- deferred settlement (dev/demo pending trigger) ---------------------------------


def _pending_gateway(valkey, *, settle_seconds: int) -> StubPaymentGateway:
    """The stub with the demo trigger on, over a shared (Valkey) window."""
    return StubPaymentGateway(
        "decline",
        pending_token_substring="pending",
        pending_settle_seconds=settle_seconds,
        deferred_window=DeferredChargeWindow(valkey, settle_seconds=settle_seconds),
    )


async def test_pending_token_answers_processing_and_keeps_the_payment_pending(session, real_valkey):
    """The demo trigger: the gateway accepts the charge but does not decide it,
    so the payment row stays ``pending`` with no event shipped — the checkout
    ends in the documented "outcome unknown" 409 and the workers take over."""
    kwargs = _charge_kwargs(token="tok_pending_demo")
    service = _service(session, gateway=_pending_gateway(real_valkey, settle_seconds=3600))

    response = await service.charge(**kwargs)

    assert response.status == "pending"
    status, _ = await _status_of(session, kwargs["idempotency_key"])
    assert status == "pending"
    assert await _outbox_types(session) == []


async def test_reconciliation_resolves_a_deferred_charge_from_another_process(session, real_valkey):
    """The recovery story's engine: the reconciler runs in its OWN process with
    its OWN stub map — the shared Valkey window is what lets its ``lookup``
    resolve a charge the API deferred."""
    kwargs = _charge_kwargs(token="tok_pending_demo")
    await _service(session, gateway=_pending_gateway(real_valkey, settle_seconds=0)).charge(**kwargs)
    await _backdate_pending(session, kwargs["idempotency_key"])  # past the grace window

    reconciler_side = _pending_gateway(real_valkey, settle_seconds=0)  # fresh instance, shared window
    resolved = await _service(session, gateway=reconciler_side).reconcile(batch_size=10)

    assert resolved == 1
    status, _ = await _status_of(session, kwargs["idempotency_key"])
    assert status == "succeeded"
    assert await _outbox_types(session) == ["PaymentSucceeded"]


async def test_a_deferred_charge_replays_one_answer_and_one_ref_across_instances(real_valkey):
    """Provider dedup for deferred charges: every replay — including through a
    different stub instance sharing the window — returns the same ref, pending
    until the settle deadline."""
    key = f"checkout-{uuid.uuid4()}"

    async def deferred_charge(gateway: StubPaymentGateway):
        return await gateway.charge(
            amount=Decimal("42.50"), idempotency_key=key, payment_method_token="tok_pending_demo"
        )

    gateway = _pending_gateway(real_valkey, settle_seconds=3600)
    first = await deferred_charge(gateway)
    replay = await deferred_charge(gateway)
    from_other_instance = await deferred_charge(_pending_gateway(real_valkey, settle_seconds=3600))

    assert first.outcome == GatewayOutcome.PENDING
    assert replay.ref == first.ref
    assert from_other_instance.ref == first.ref  # cross-process dedup via the shared window
    lookup = await gateway.lookup(key)
    assert lookup is not None and lookup.outcome == GatewayOutcome.PENDING


async def test_decline_wins_over_the_pending_trigger(session, real_valkey):
    """A token carrying both substrings declines: the failure knob keeps
    precedence, so the decline demo is never swallowed by the deferral."""
    kwargs = _charge_kwargs(token="tok_pending_decline")
    service = _service(session, gateway=_pending_gateway(real_valkey, settle_seconds=0))

    response = await service.charge(**kwargs)

    assert response.status == "failed"


def test_stub_factory_wires_the_pending_trigger_only_when_configured():
    """Default settings build the plain stub; the trigger needs the shared
    window, so configuring it without a Valkey client is refused loudly."""
    settings = AppSettings(
        _env_file=None, database_url="postgresql+asyncpg://u:p@localhost:5432/db", environment="local"
    )
    assert stub_gateway_from_settings(settings)._deferred is None

    triggered = AppSettings(
        _env_file=None,
        database_url="postgresql+asyncpg://u:p@localhost:5432/db",
        environment="local",
        payment_stub_pending_token_substring="tok_pending",
    )
    with pytest.raises(RuntimeError, match="Valkey"):
        stub_gateway_from_settings(triggered, valkey=None)


@pytest.mark.parametrize("env", ["staging", "prod"])
def test_pending_trigger_is_refused_outside_dev(env):
    """The demo trigger must be impossible to enable where real checkouts run."""
    with pytest.raises(ValueError, match="payment_stub_pending_token_substring"):
        AppSettings(
            _env_file=None,
            database_url="postgresql+asyncpg://u:p@localhost:5432/db",
            environment=env,
            payment_stub_pending_token_substring="tok_pending",
        )


# --- webhook body cap (route-level) --------------------------------------------------


def _route_request(*, content_length: str | None = None, body: bytes = b"", signature: str | None = None) -> Request:
    headers = []
    if content_length is not None:
        headers.append((b"content-length", content_length.encode()))
    if signature is not None:
        headers.append((b"x-payment-signature", signature.encode()))
    scope = {"type": "http", "method": "POST", "path": "/v1/payments/webhook", "headers": headers}
    chunks = [body]

    async def receive():
        if chunks:
            return {"type": "http.request", "body": chunks.pop(0), "more_body": False}
        return {"type": "http.disconnect"}

    return Request(scope, receive)


class _UnreachableService:
    """The service must never be called for an oversized body."""

    async def handle_webhook(self, body: bytes, signature: str | None) -> bool:
        raise AssertionError("oversized body reached the service")


async def test_webhook_with_huge_declared_length_is_rejected_before_reading():
    request = _route_request(content_length=str(10 * 1024 * 1024))
    with pytest.raises(HTTPException) as exc_info:
        await payment_webhook(request, _UnreachableService())
    assert exc_info.value.status_code == 413


async def test_webhook_with_huge_actual_body_is_rejected():
    big = b"x" * (MAX_WEBHOOK_BODY_BYTES + 1)
    request = _route_request(content_length=str(len(big)), body=big)
    with pytest.raises(HTTPException) as exc_info:
        await payment_webhook(request, _UnreachableService())
    assert exc_info.value.status_code == 413


async def test_webhook_with_normal_body_reaches_the_service():
    seen: dict = {}

    class _RecordingService:
        async def handle_webhook(self, body: bytes, signature: str | None) -> bool:
            seen["body"] = body
            seen["signature"] = signature
            return True

    small = b'{"type":"payment.succeeded"}'
    request = _route_request(content_length=str(len(small)), body=small, signature="sha256=abc")
    assert await payment_webhook(request, _RecordingService()) is None
    assert seen == {"body": small, "signature": "sha256=abc"}


# --- unknown-ref handler regression --------------------------------------------------
# UnknownPaymentRefError once lacked `.detail`, so the 404 handler raised
# AttributeError (a 500) on exactly the path meant to answer 404.


async def test_unknown_payment_ref_handler_answers_404_with_detail():
    request = _route_request(body=b"{}")
    response = await _unknown_payment_ref_handler(request, UnknownPaymentRefError("no payment exists for 'k'"))

    assert response.status_code == 404
    assert json.loads(response.body)["detail"] == "no payment exists for 'k'"
