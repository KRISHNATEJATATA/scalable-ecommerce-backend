"""Sender adapters behind the sender port + the settings-driven selection.

Local = SMTP (Mailpit in compose — the worker reaches ``mailpit:1025`` on the
compose network); prod = **AWS SES via ``aioboto3``** with the ECS task role —
no credentials in code, same pattern as the S3 client. SMTP's blocking
round-trip is offloaded with ``run_in_threadpool`` (golden rule: never a
blocking call in an async path); SES is async-native.
"""

from __future__ import annotations

import smtplib
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from email.message import EmailMessage
from typing import Any, cast

import aioboto3
from fastapi.concurrency import run_in_threadpool

from src.notifications.ports.sender import NotificationSenderPort
from src.shared.config.setting import AppSettings

_session = aioboto3.Session()

# a constant, not a setting — a blackholed SMTP host pins one
# threadpool thread per in-flight message for 10s, bounded by the worker's
# small batch; per-environment retuning is not a real need yet.
_SMTP_TIMEOUT_SECONDS = 10


def _build_mime(
    from_address: str, to: str, subject: str, body: str, body_html: str | None, message_id: str | None
) -> EmailMessage:
    """The MIME message both transports send — identical bytes on a takeover
    resend. The persisted id rides in TWO headers:
    ``Message-ID`` (RFC 5322 threading; honored by SMTP receivers) and
    ``X-Notification-Message-Id`` — the correlation key that survives SES,
    which overwrites ``Message-ID`` even on raw sends but preserves custom
    ``X-`` headers."""
    message = EmailMessage()
    message["From"] = from_address
    message["To"] = to
    message["Subject"] = subject
    if message_id is not None:
        domain = from_address.rpartition("@")[2] or "localhost"
        message["Message-ID"] = f"<{message_id}@{domain}>"
        message["X-Notification-Message-Id"] = message_id
    message.set_content(body)
    if body_html is not None:
        # multipart/alternative: text-first clients get the plain part.
        message.add_alternative(body_html, subtype="html")
    return message


class SmtpSender(NotificationSenderPort):
    """SMTP transport (Mailpit locally; any SMTP host via ``NOTIFICATION_SMTP_HOST``)."""

    def __init__(self, host: str, port: int, from_address: str) -> None:
        self._host = host
        self._port = port
        self._from = from_address

    def _send_sync(self, message: EmailMessage) -> None:
        """The blocking round-trip (connect + send); offloaded to a threadpool."""
        with smtplib.SMTP(self._host, self._port, timeout=_SMTP_TIMEOUT_SECONDS) as smtp:
            smtp.send_message(message)

    async def send(
        self, *, to: str, subject: str, body: str, body_html: str | None = None, message_id: str | None = None
    ) -> None:
        await run_in_threadpool(self._send_sync, _build_mime(self._from, to, subject, body, body_html, message_id))


class SesSender(NotificationSenderPort):
    """AWS SES transport (prod). The entered aioboto3 client is the task-role caller.

    Sends RAW MIME, not ``send_email``: the structured API accepts no custom
    headers at all, so only a raw message carries the persisted id. Note
    SES **overwrites the ``Message-ID`` header even on raw sends** — the
    reconciliation key is the preserved ``X-Notification-Message-Id`` header
    (ADR 0024).
    """

    def __init__(self, client: Any, from_address: str) -> None:
        self._client = client
        self._from = from_address

    async def send(
        self, *, to: str, subject: str, body: str, body_html: str | None = None, message_id: str | None = None
    ) -> None:
        message = _build_mime(self._from, to, subject, body, body_html, message_id)
        await self._client.send_raw_email(
            Source=self._from,
            Destinations=[to],
            RawMessage={"Data": message.as_bytes()},
        )


@asynccontextmanager
async def make_sender_cm(settings: AppSettings) -> AsyncIterator[NotificationSenderPort]:
    """Yield the configured sender for the worker's lifetime.

    SMTP is stateless (a connection per send); SES yields the entered aioboto3
    client. The transport is selected once at worker boot — fail-fast here, not
    at the first send.
    """
    if settings.notifications_transport == "smtp":
        if not settings.notification_smtp_host:
            raise RuntimeError("NOTIFICATION_SMTP_HOST must be configured for the smtp transport")
        yield SmtpSender(
            settings.notification_smtp_host,
            settings.notification_smtp_port,
            settings.notification_from_address,
        )
        return
    # aioboto3 ships no stubs, so the bare client CM reads as unknown and every
    # ``async with`` on it warns (see src/shared/clients/s3_client.py) — the
    # object already *is* an async CM at runtime; this only writes that down.
    client_cm = cast(
        "AbstractAsyncContextManager[Any]",
        _session.client("ses", region_name=settings.notification_region),
    )
    async with client_cm as client:
        yield SesSender(client, settings.notification_from_address)
