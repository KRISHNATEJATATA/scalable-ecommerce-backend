"""Notification consumer tests: the send path, the backstops, and the routing.

Real Postgres via the session fixtures (never SQLite — ``record_sent`` leans on
the UNIQUE(order_id, email_type) backstop); the sender is a recording test
double behind the port (no real SMTP in tests).
"""

import uuid
from decimal import Decimal

import pytest

from src.events.models import OrderPlaced, OrderPlacedData, OrderPlacedLine, UserCreated, UserCreatedData
from src.notifications.adapters.db.models import EmailSuppression
from src.notifications.adapters.db.repository import NotificationRepository
from src.notifications.adapters.notification_worker import make_notification_handler
from src.notifications.application.service import NotificationService, UnknownRecipientError
from src.notifications.themes import EmailTheme

# The packaged minimal theme (from_settings("") = the default): exercises the
# loader + the template files alongside the send path.
_THEME = EmailTheme.from_settings("")


def _order_placed_event(order_id: uuid.UUID, user_id: uuid.UUID) -> dict:
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
        ),
    ).model_dump(mode="json")


def _user_created_event(user_id: uuid.UUID, email: str) -> dict:
    return UserCreated.new(trace_id="", data=UserCreatedData(user_id=user_id, email=email)).model_dump(mode="json")


class _RecordingSender:
    """Test double for the sender port: records sends, optionally fails."""

    def __init__(self, fail: bool = False) -> None:
        self.calls: list[dict[str, str | None]] = []
        self.fail = fail

    async def send(self, *, to: str, subject: str, body: str, body_html: str | None = None) -> None:
        if self.fail:
            raise RuntimeError("smtp down")
        self.calls.append({"to": to, "subject": subject, "body": body, "body_html": body_html})


async def test_record_sent_replay_returns_false_via_the_unique_backstop(session):
    repo = NotificationRepository(session)
    order_id = uuid.uuid4()
    assert await repo.record_sent(order_id, "order_confirmation", "buyer@example.com") is True
    # The replay (a redelivery after the dedupe-TTL expiry) hits the UNIQUE backstop.
    assert await repo.record_sent(order_id, "order_confirmation", "buyer@example.com") is False


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
    """No recipient materialized (user predates the consumer) → the handler raises:
    the message is left for SQS redrive → DLQ, never silently dropped."""
    repo = NotificationRepository(session)
    sender = _RecordingSender()
    service = NotificationService(repo, sender, _THEME)
    with pytest.raises(UnknownRecipientError):
        await service.handle_order_placed(_order_placed_event(uuid.uuid4(), uuid.uuid4()))
    assert sender.calls == []


async def test_send_failure_before_the_record_retries_cleanly(session):
    """A transient send failure raises BEFORE anything is recorded, so the redrive
    retries cleanly and the second attempt sends."""
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


async def test_handler_routes_user_created_and_order_placed(sessionmaker_factory):
    sender = _RecordingSender()
    handler = make_notification_handler(sessionmaker_factory, sender, _THEME)
    user_id = uuid.uuid4()
    await handler(_user_created_event(user_id, "buyer@example.com"))
    await handler(_order_placed_event(uuid.uuid4(), user_id))
    assert len(sender.calls) == 1
    assert sender.calls[0]["to"] == "buyer@example.com"


def test_missing_theme_dir_fails_fast(tmp_path):
    """A missing theme dir/file is a worker-BOOT failure (the fail-fast contract):
    from_settings raises before any queue or SMTP contact."""
    with pytest.raises(RuntimeError, match="missing"):
        EmailTheme.from_settings(str(tmp_path / "no-such-theme"))


def test_custom_theme_dir_override(tmp_path):
    """A frontend-mounted dir overrides the packaged default: the copy comes from
    the pointed-at files (the startup-decided branding switch)."""
    theme_dir = tmp_path / "acme-mail"
    theme_dir.mkdir()
    (theme_dir / "subject.txt").write_text("Your ACME order — $order_id\n", encoding="utf-8")
    (theme_dir / "body.txt").write_text("Thanks!\n\n$order_id\n", encoding="utf-8")
    (theme_dir / "body.html").write_text("<p>$order_id</p>\n", encoding="utf-8")
    theme = EmailTheme.from_settings(str(theme_dir))
    subject, body, body_html = theme.render_order_confirmation("o-1", "1.00", "- x1", "<li>x1</li>")
    assert subject == "Your ACME order — o-1"
    assert "Thanks!" in body and "o-1" in body
    assert body_html == "<p>o-1</p>\n"
