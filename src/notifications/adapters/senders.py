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

    async def send(self, *, to: str, subject: str, body: str, body_html: str | None = None) -> None:
        message = EmailMessage()
        message["From"] = self._from
        message["To"] = to
        message["Subject"] = subject
        message.set_content(body)
        if body_html is not None:
            # multipart/alternative: text-first clients get the plain part.
            message.add_alternative(body_html, subtype="html")
        await run_in_threadpool(self._send_sync, message)


class SesSender(NotificationSenderPort):
    """AWS SES transport (prod). The entered aioboto3 client is the task-role caller."""

    def __init__(self, client: Any, from_address: str) -> None:
        self._client = client
        self._from = from_address

    async def send(self, *, to: str, subject: str, body: str, body_html: str | None = None) -> None:
        body_block: dict[str, dict[str, str]] = {"Text": {"Data": body, "Charset": "UTF-8"}}
        if body_html is not None:
            body_block["Html"] = {"Data": body_html, "Charset": "UTF-8"}
        await self._client.send_email(
            Source=self._from,
            Destination={"ToAddresses": [to]},
            Message={
                "Subject": {"Data": subject, "Charset": "UTF-8"},
                "Body": body_block,
            },
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
