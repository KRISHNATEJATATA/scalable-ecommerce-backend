"""Notification consumer tests: the send path, the claim state, and the routing.

Real Postgres via the session fixtures (never SQLite — ``claim_send`` leans on
the UNIQUE(order_id, email_type) guard); the sender is a recording test
double behind the port (no real SMTP in tests).
"""

import email as stdlib_email
import uuid
from decimal import Decimal

import pytest

from src.events.models import (
    OrderPlaced,
    OrderPlacedData,
    OrderPlacedLine,
    PaymentRefunded,
    PaymentRefundedData,
    UserCreated,
    UserCreatedData,
)
from src.notifications.adapters.db.models import EmailSuppression
from src.notifications.adapters.db.repository import NotificationRepository
from src.notifications.adapters.notification_worker import make_notification_handler
from src.notifications.adapters.senders import SesSender, _build_mime
from src.notifications.application.service import NotificationService, UnknownRecipientError
from src.notifications.domain.email import EmailType
from src.notifications.themes import EmailTheme

# The packaged minimal theme (from_settings("") = the default): exercises the
# loader + the template files alongside the send path.
_THEME = EmailTheme.from_settings("")


def _order_placed_event(order_id: uuid.UUID, user_id: uuid.UUID, user_email: str | None = None) -> dict:
    """A validated, normalized OrderPlaced (the shape SqsConsumer hands the handler)."""
    return OrderPlaced.new(
        trace_id="",
        data=OrderPlacedData(
            order_id=order_id,
            user_id=user_id,
            total=Decimal("25.00"),
            items=[
                OrderPlacedLine(
                    product_id=uuid.uuid4(), product_name="Leather Ankle Boots", quantity=2, unit_price=Decimal("12.50")
                )
            ],
            user_email=user_email,
        ),
    ).model_dump(mode="json")


def _user_created_event(user_id: uuid.UUID, email: str) -> dict:
    return UserCreated.new(trace_id="", data=UserCreatedData(user_id=user_id, email=email)).model_dump(mode="json")


def _payment_refunded_event(order_id: uuid.UUID, amount: str = "25.00") -> dict:
    """A validated, normalized PaymentRefunded (the shape SqsConsumer hands the handler).

    The wire carries NO recipient — only the charge's ids, the amount and the
    gateway ref — which is exactly why the refund email resolves the Order
    Recipient projection.
    """
    return PaymentRefunded.new(
        trace_id="",
        data=PaymentRefundedData(
            payment_id=uuid.uuid4(),
            order_id=order_id,
            amount=Decimal(amount),
            gateway_ref="chrg_123",
        ),
    ).model_dump(mode="json")


class _RecordingSender:
    """Test double for the sender port: records sends, optionally fails."""

    def __init__(self, fail: bool = False) -> None:
        self.calls: list[dict[str, str | None]] = []
        self.fail = fail

    async def send(
        self, *, to: str, subject: str, body: str, body_html: str | None = None, message_id: str | None = None
    ) -> None:
        if self.fail:
            raise RuntimeError("smtp down")
        self.calls.append(
            {"to": to, "subject": subject, "body": body, "body_html": body_html, "message_id": message_id}
        )


async def test_claim_send_semantics_via_the_unique_guard(session):
    """claimed → takeover → already_sent: the durable send state's three answers,
    with ONE persisted uuid4 message_id across every claim of the same row."""
    repo = NotificationRepository(session)
    order_id = uuid.uuid4()
    email_type = EmailType.ORDER_CONFIRMATION.value
    first = await repo.claim_send(order_id, email_type, "buyer@example.com")
    assert first.outcome == "claimed"
    # A redelivery over the un-marked claim (the crash window) takes it over —
    # and reuses the PERSISTED id, not a fresh one.
    takeover = await repo.claim_send(order_id, email_type, "buyer@example.com")
    assert takeover.outcome == "takeover"
    assert takeover.message_id == first.message_id
    await repo.mark_sent(order_id, email_type, "buyer@example.com")
    # A redelivery after the recorded send (dedupe-TTL expiry) never resends.
    done = await repo.claim_send(order_id, email_type, "buyer@example.com")
    assert done.outcome == "already_sent"
    assert done.message_id == first.message_id
    assert await repo.has_sent(order_id, email_type) is True


async def test_claim_generates_distinct_uuid4_ids_per_send(session):
    """Ids are plain uuid4 (codebase-consistent), unique per (order, type) claim."""
    repo = NotificationRepository(session)
    a = await repo.claim_send(uuid.uuid4(), "order_confirmation", "a@example.com")
    b = await repo.claim_send(uuid.uuid4(), "order_confirmation", "b@example.com")
    assert a.message_id != b.message_id
    assert a.message_id.version == 4


def test_build_mime_carries_the_deterministic_headers():
    """Both headers ride the MIME: Message-ID (threading) + the SES-proof X- header."""
    msg = _build_mime("shop@example.com", "buyer@example.com", "Subject", "text", "<p>html</p>", "abc-123")
    assert msg["Message-ID"] == "<abc-123@example.com>"
    assert msg["X-Notification-Message-Id"] == "abc-123"
    assert msg["From"] == "shop@example.com"
    assert msg["To"] == "buyer@example.com"
    assert msg.get_content_type() == "multipart/alternative"  # text + html parts


class _StubSesClient:
    """Records send_raw_email kwargs (the aioboto3 client's async shape)."""

    def __init__(self) -> None:
        self.calls: list[dict] = []

    async def send_raw_email(self, **kwargs):
        self.calls.append(kwargs)
        return {"MessageId": "ses-provider-id"}


async def test_ses_sender_sends_raw_mime_with_the_deterministic_header():
    """SES gets raw MIME (the structured API takes no custom headers); the
    deterministic id is in the bytes, under the header SES preserves."""
    stub = _StubSesClient()
    sender = SesSender(stub, "shop@example.com")
    await sender.send(to="buyer@example.com", subject="Sub", body="text", message_id="abc-123")
    assert len(stub.calls) == 1
    call = stub.calls[0]
    assert call["Source"] == "shop@example.com"
    assert call["Destinations"] == ["buyer@example.com"]
    parsed = stdlib_email.message_from_bytes(call["RawMessage"]["Data"])
    assert parsed["X-Notification-Message-Id"] == "abc-123"
    assert parsed["Message-ID"] == "<abc-123@example.com>"
    assert parsed["Subject"] == "Sub"


async def test_order_placed_sends_the_confirmation_and_records_it(session):
    repo = NotificationRepository(session)
    sender = _RecordingSender()
    service = NotificationService(repo, sender, _THEME)
    user_id, order_id = uuid.uuid4(), uuid.uuid4()
    await service.handle_user_created(_user_created_event(user_id, "buyer@example.com"))
    await service.handle_order_placed(_order_placed_event(order_id, user_id))
    assert len(sender.calls) == 1
    assert sender.calls[0]["to"] == "buyer@example.com"
    assert "Order confirmation" in sender.calls[0]["subject"]
    assert str(order_id) in sender.calls[0]["body"]
    # The item line carries the checkout-time product snapshot's NAME, not the id.
    assert "Leather Ankle Boots" in sender.calls[0]["body"]
    assert "Leather Ankle Boots" in (sender.calls[0]["body_html"] or "")
    # The theme-driven HTML alternative carries the order too (the <li> lines).
    assert sender.calls[0]["body_html"] is not None
    assert str(order_id) in sender.calls[0]["body_html"]
    assert "<li>" in sender.calls[0]["body_html"]
    # The send carries the claim's persisted uuid4 message identifier.
    assert uuid.UUID(sender.calls[0]["message_id"]).version == 4
    assert await repo.has_sent(order_id, "order_confirmation") is True


async def test_redelivery_after_dedupe_ttl_expiry_cannot_double_send(session):
    """A prior delivery that already sent + recorded suppresses the redelivery:
    the sent_emails backstop is the check that keeps a dedupe-TTL expiry from
    ever double-sending."""
    repo = NotificationRepository(session)
    sender = _RecordingSender()
    service = NotificationService(repo, sender, _THEME)
    user_id, order_id = uuid.uuid4(), uuid.uuid4()
    event = _order_placed_event(order_id, user_id)
    await service.handle_user_created(_user_created_event(user_id, "buyer@example.com"))
    await service.handle_order_placed(event)
    await service.handle_order_placed(event)  # the redelivery
    assert len(sender.calls) == 1


async def test_suppressed_recipient_is_never_sent(session):
    repo = NotificationRepository(session)
    sender = _RecordingSender()
    service = NotificationService(repo, sender, _THEME)
    user_id, order_id = uuid.uuid4(), uuid.uuid4()
    email = "bounced@example.com"
    session.add(EmailSuppression(recipient=email, reason="hard_bounce"))
    await session.commit()
    await service.handle_user_created(_user_created_event(user_id, email))
    await service.handle_order_placed(_order_placed_event(order_id, user_id))
    assert sender.calls == []
    assert await repo.has_sent(order_id, "order_confirmation") is False


async def test_unknown_recipient_raises_for_redrive(session):
    """No recipient materialized (user predates the consumer) AND no event-carried
    address → the handler raises: the message is left for SQS redrive → DLQ,
    never silently dropped."""
    repo = NotificationRepository(session)
    sender = _RecordingSender()
    service = NotificationService(repo, sender, _THEME)
    with pytest.raises(UnknownRecipientError):
        await service.handle_order_placed(_order_placed_event(uuid.uuid4(), uuid.uuid4()))
    assert sender.calls == []


async def test_event_carried_email_sends_without_the_recipient_row(session):
    """The ordering-independence fix: an OrderPlaced that carries the buyer's
    checkout-time user_email sends even when this user's UserCreated has NOT
    reached the consumer yet — cross-subscription ordering can no longer DLQ a
    healthy order's confirmation."""
    repo = NotificationRepository(session)
    sender = _RecordingSender()
    service = NotificationService(repo, sender, _THEME)
    user_id, order_id = uuid.uuid4(), uuid.uuid4()
    # No handle_user_created — the recipients table is empty for this user.
    await service.handle_order_placed(_order_placed_event(order_id, user_id, user_email="buyer@example.com"))
    assert len(sender.calls) == 1
    assert sender.calls[0]["to"] == "buyer@example.com"
    assert await repo.has_sent(order_id, "order_confirmation") is True


async def test_event_carried_email_wins_over_the_materialized_recipient(session):
    """The event's checkout-time address is authoritative: a stale recipients row
    does not redirect the confirmation."""
    repo = NotificationRepository(session)
    sender = _RecordingSender()
    service = NotificationService(repo, sender, _THEME)
    user_id, order_id = uuid.uuid4(), uuid.uuid4()
    await service.handle_user_created(_user_created_event(user_id, "old@example.com"))
    await service.handle_order_placed(_order_placed_event(order_id, user_id, user_email="new@example.com"))
    assert [c["to"] for c in sender.calls] == ["new@example.com"]


async def test_send_failure_after_the_claim_retries_cleanly(session):
    """A transient send failure raises AFTER the claim: the row stays ``pending``
    (not yet sent), so the redrive takes the claim over and the retry sends."""
    repo = NotificationRepository(session)
    failing = _RecordingSender(fail=True)
    service = NotificationService(repo, failing, _THEME)
    user_id, order_id = uuid.uuid4(), uuid.uuid4()
    event = _order_placed_event(order_id, user_id)
    await service.handle_user_created(_user_created_event(user_id, "buyer@example.com"))
    try:
        await service.handle_order_placed(event)
    except RuntimeError:
        pass
    assert await repo.has_sent(order_id, "order_confirmation") is False
    sender = _RecordingSender()
    await NotificationService(repo, sender, _THEME).handle_order_placed(event)
    assert len(sender.calls) == 1
    assert await repo.has_sent(order_id, "order_confirmation") is True


async def test_crash_window_takeover_resends_with_the_same_message_id(session):
    """Crash AFTER the provider accepted but BEFORE the mark: the redelivery finds
    the ``pending`` claim, takes it over, and resends with the SAME persisted
    message_id — the at-least-once duplicate is now detectable and reconcilable."""
    repo = NotificationRepository(session)
    sender = _RecordingSender()
    service = NotificationService(repo, sender, _THEME)
    user_id, order_id = uuid.uuid4(), uuid.uuid4()
    event = _order_placed_event(order_id, user_id)
    await service.handle_user_created(_user_created_event(user_id, "buyer@example.com"))
    email_type = EmailType.ORDER_CONFIRMATION.value
    # Simulate the crashed attempt: claim + send, no mark (process died here).
    claim = await repo.claim_send(order_id, email_type, "buyer@example.com")
    assert claim.outcome == "claimed"
    assert await repo.has_sent(order_id, email_type) is False
    # The redelivery: takeover → resend with the identical persisted id.
    await service.handle_order_placed(event)
    assert len(sender.calls) == 1
    assert sender.calls[0]["message_id"] == str(claim.message_id)
    assert await repo.has_sent(order_id, email_type) is True


async def test_handler_routes_user_created_and_order_placed(sessionmaker_factory):
    sender = _RecordingSender()
    handler = make_notification_handler(sessionmaker_factory, sender, _THEME)
    user_id = uuid.uuid4()
    await handler(_user_created_event(user_id, "buyer@example.com"))
    await handler(_order_placed_event(uuid.uuid4(), user_id))
    assert len(sender.calls) == 1
    assert sender.calls[0]["to"] == "buyer@example.com"


async def test_handler_routes_payment_refunded(sessionmaker_factory):
    """The full worker path: OrderPlaced feeds the projection, PaymentRefunded
    sends the refund email — two emails, one order."""
    sender = _RecordingSender()
    handler = make_notification_handler(sessionmaker_factory, sender, _THEME)
    order_id = uuid.uuid4()
    await handler(_order_placed_event(order_id, uuid.uuid4(), user_email="buyer@example.com"))
    await handler(_payment_refunded_event(order_id))
    assert [c["to"] for c in sender.calls] == ["buyer@example.com", "buyer@example.com"]


async def test_payment_refunded_sends_the_refund_email_and_records_it(session):
    """The refund email names the order and the amount in customer-safe copy,
    carries the claim's persisted message id, and is recorded under
    ``refund_issued`` — its own Claimed Send beside the confirmation's."""
    repo = NotificationRepository(session)
    sender = _RecordingSender()
    service = NotificationService(repo, sender, _THEME)
    order_id = uuid.uuid4()
    await service.handle_order_placed(_order_placed_event(order_id, uuid.uuid4(), user_email="buyer@example.com"))
    assert len(sender.calls) == 1  # the confirmation
    await service.handle_payment_refunded(_payment_refunded_event(order_id, "42.50"))
    assert len(sender.calls) == 2
    call = sender.calls[1]
    assert call["to"] == "buyer@example.com"
    assert str(order_id) in call["subject"]
    assert str(order_id) in call["body"]
    assert "42.50" in call["body"]
    assert str(order_id) in (call["body_html"] or "")
    assert "42.50" in (call["body_html"] or "")
    # Customer-safe: the charge's provider reference never reaches the mail.
    assert "chrg_123" not in call["body"] and "chrg_123" not in call["subject"]
    assert uuid.UUID(call["message_id"]).version == 4
    assert await repo.has_sent(order_id, "refund_issued") is True
    # The confirmation's Claimed Send is untouched by the refund's.
    assert await repo.has_sent(order_id, "order_confirmation") is True


async def test_refund_redelivery_cannot_double_send(session):
    """The sent_emails backstop (UNIQUE(order_id, email_type)) keeps a
    dedupe-TTL-expiry redelivery of PaymentRefunded from ever double-sending."""
    repo = NotificationRepository(session)
    sender = _RecordingSender()
    service = NotificationService(repo, sender, _THEME)
    order_id = uuid.uuid4()
    await service.handle_order_placed(_order_placed_event(order_id, uuid.uuid4(), user_email="buyer@example.com"))
    event = _payment_refunded_event(order_id)
    await service.handle_payment_refunded(event)
    await service.handle_payment_refunded(event)  # the redelivery
    assert len(sender.calls) == 2  # confirmation + ONE refund email


async def test_refund_email_resolves_the_projection_even_when_the_confirmation_was_suppressed(session):
    """The Order Recipient projection is fed BEFORE any send decision: an order
    whose confirmation was suppressed still has its address recorded, so the
    refund handler RESOLVES the recipient (and then honours the same
    suppression — a hard-bounced address is never mailed again) rather than
    raising a projection miss. The orders where mail matters most are not the
    ones that lose their address source."""
    repo = NotificationRepository(session)
    sender = _RecordingSender()
    service = NotificationService(repo, sender, _THEME)
    order_id = uuid.uuid4()
    session.add(EmailSuppression(recipient="bounced@example.com", reason="hard_bounce"))
    await session.commit()
    await service.handle_order_placed(_order_placed_event(order_id, uuid.uuid4(), user_email="bounced@example.com"))
    assert sender.calls == []  # confirmation suppressed
    # The address is recorded anyway — the projection is not the send state.
    assert await repo.get_order_email(order_id) == "bounced@example.com"
    # Resolvable, so no raise; the same suppression list applies to the refund.
    await service.handle_payment_refunded(_payment_refunded_event(order_id))
    assert sender.calls == []
    assert await repo.has_sent(order_id, "refund_issued") is False


async def test_refund_email_sends_when_the_confirmation_send_never_completed(session):
    """The complementary half: an order whose confirmation is still ``pending``
    (claimed, never sent — the crash window) also resolves the refund email's
    recipient from the projection."""
    repo = NotificationRepository(session)
    sender = _RecordingSender()
    service = NotificationService(repo, sender, _THEME)
    order_id = uuid.uuid4()
    # Simulate the confirmation's crashed attempt: claim, no send, no mark.
    await repo.upsert_order_email(order_id, "buyer@example.com")
    claim = await repo.claim_send(order_id, "order_confirmation", "buyer@example.com")
    assert claim.outcome == "claimed"
    await service.handle_payment_refunded(_payment_refunded_event(order_id))
    assert [c["to"] for c in sender.calls] == ["buyer@example.com"]
    assert await repo.has_sent(order_id, "refund_issued") is True


async def test_refund_without_a_projection_row_raises_for_redrive(session):
    """No Order Recipient for the refunded order → the handler raises: the
    message is left for SQS redrive → DLQ, never silently skipped (a transient
    cross-topic race self-heals on redelivery; a permanent miss stays visible)."""
    repo = NotificationRepository(session)
    sender = _RecordingSender()
    service = NotificationService(repo, sender, _THEME)
    with pytest.raises(UnknownRecipientError):
        await service.handle_payment_refunded(_payment_refunded_event(uuid.uuid4()))
    assert sender.calls == []


async def test_refund_send_failure_after_the_claim_retries_cleanly(session):
    """The shared claim discipline: a transient send failure raises AFTER the
    refund claim (row stays ``pending``), and the retry takes the claim over
    and sends — the same recovery the confirmation exercises."""
    repo = NotificationRepository(session)
    order_id = uuid.uuid4()
    placed = _order_placed_event(order_id, uuid.uuid4(), user_email="buyer@example.com")
    event = _payment_refunded_event(order_id)
    await NotificationService(repo, _RecordingSender(), _THEME).handle_order_placed(placed)
    failing = NotificationService(repo, _RecordingSender(fail=True), _THEME)
    with pytest.raises(RuntimeError):
        await failing.handle_payment_refunded(event)
    assert await repo.has_sent(order_id, "refund_issued") is False
    sender = _RecordingSender()
    await NotificationService(repo, sender, _THEME).handle_payment_refunded(event)
    assert len(sender.calls) == 1
    assert await repo.has_sent(order_id, "refund_issued") is True


async def test_refund_crash_window_takeover_resends_with_the_same_message_id(session):
    """Crash AFTER the provider accepted but BEFORE the mark: the redelivery finds
    the ``pending`` refund claim, takes it over, and resends with the SAME persisted
    message_id — the at-least-once duplicate is now detectable and reconcilable."""
    repo = NotificationRepository(session)
    sender = _RecordingSender()
    service = NotificationService(repo, sender, _THEME)
    order_id = uuid.uuid4()
    await service.handle_order_placed(_order_placed_event(order_id, uuid.uuid4(), user_email="buyer@example.com"))
    email_type = EmailType.REFUND_ISSUED.value
    # Simulate the crashed attempt: claim + send, no mark (process died here).
    claim = await repo.claim_send(order_id, email_type, "buyer@example.com")
    assert claim.outcome == "claimed"
    assert await repo.has_sent(order_id, email_type) is False
    # The redelivery: takeover → resend with the identical persisted id.
    await service.handle_payment_refunded(_payment_refunded_event(order_id))
    assert len(sender.calls) == 2  # confirmation + the refund redelivery
    assert sender.calls[1]["message_id"] == str(claim.message_id)
    assert await repo.has_sent(order_id, email_type) is True


def test_missing_theme_dir_fails_fast(tmp_path):
    """A missing theme dir/file is a worker-BOOT failure (the fail-fast contract):
    from_settings raises before any queue or SMTP contact."""
    with pytest.raises(RuntimeError, match="missing"):
        EmailTheme.from_settings(str(tmp_path / "no-such-theme"))


def test_missing_refund_files_fail_fast(tmp_path):
    """The theme-dir contract covers EVERY email type: a dir carrying only the
    confirmation triple (a pre-ticket-02 mounted theme) is a boot failure, not
    a silent fall-back to packaged refund copy."""
    theme_dir = tmp_path / "legacy-acme"
    theme_dir.mkdir()
    for name in ("subject.txt", "body.txt", "body.html"):
        (theme_dir / name).write_text("x\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="refund_subject.txt"):
        EmailTheme.from_settings(str(theme_dir))


def test_packaged_theme_renders_the_refund_email():
    """The packaged minimal theme carries the refund triple: both placeholders
    substituted, no template syntax leaking into customer-facing copy."""
    subject, body, body_html = _THEME.render_refund_issued("o-1", "42.50")
    assert "o-1" in subject and "42.50" not in subject  # the subject names only the order
    assert "o-1" in body and "42.50" in body
    assert "o-1" in body_html and "42.50" in body_html
    for rendered in (subject, body, body_html):
        assert "$" not in rendered


def test_custom_theme_dir_override(tmp_path):
    """A frontend-mounted dir overrides the packaged default: the copy comes from
    the pointed-at files (the startup-decided branding switch) for BOTH types."""
    theme_dir = tmp_path / "acme-mail"
    theme_dir.mkdir()
    (theme_dir / "subject.txt").write_text("Your ACME order — $order_id\n", encoding="utf-8")
    (theme_dir / "body.txt").write_text("Thanks!\n\n$order_id\n", encoding="utf-8")
    (theme_dir / "body.html").write_text("<p>$order_id</p>\n", encoding="utf-8")
    (theme_dir / "refund_subject.txt").write_text("ACME refund — $order_id\n", encoding="utf-8")
    (theme_dir / "refund_body.txt").write_text("Refunded: $amount\n", encoding="utf-8")
    (theme_dir / "refund_body.html").write_text("<p>Refunded: $amount</p>\n", encoding="utf-8")
    theme = EmailTheme.from_settings(str(theme_dir))
    subject, body, body_html = theme.render_order_confirmation("o-1", "1.00", "- x1", "<li>x1</li>")
    assert subject == "Your ACME order — o-1"
    assert "Thanks!" in body and "o-1" in body
    assert body_html == "<p>o-1</p>\n"
    refund_subject, refund_body, refund_html = theme.render_refund_issued("o-2", "7.00")
    assert refund_subject == "ACME refund — o-2"
    assert refund_body == "Refunded: 7.00\n"
    assert refund_html == "<p>Refunded: 7.00</p>\n"
