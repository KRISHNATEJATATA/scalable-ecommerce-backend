"""SQS consumer — idempotent, DLQ-backed, trace-propagating.

Reads one SQS subscription (SNS → SQS with **raw message delivery**, so the body
is the verbatim event payload and the ``traceparent`` rides as a message
attribute). For each message:

1. Parse the envelope; read ``event_id``.
2. If ``event:{event_id}`` already exists in Valkey → duplicate → ack (delete) and skip.
3. Extract ``traceparent`` → pin the trace-id onto the log context for the handler.
4. Run the handler. On success, set the dedup key (TTL) and delete the message.
5. On handler error, **leave the message** — SQS redelivers it, and after the
   queue's ``maxReceiveCount`` it moves to the per-subscription DLQ (replay via
   ``docs/RUNBOOK.md``).

The Valkey dedup key is a best-effort fast-duplicate suppressor sized to the
redrive window; the true "effectively-once" guarantee is the handler's own
idempotent DB write, which is why reprocessing after a crash-before-set is safe.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from src.shared.bus.tracecontext import TRACEPARENT_ATTR, parse_trace_id
from src.shared.config.logging import request_id_ctx

log = logging.getLogger(__name__)

Handler = Callable[[dict[str, Any]], Awaitable[None]]


class SqsConsumer:
    """Drains one SQS queue into an async ``handler``.

    ``sqs`` is an entered aioboto3 SQS client; ``valkey`` is an async
    redis-py-compatible client used only for the ``event:{id}`` dedup key.
    """

    def __init__(
        self,
        sqs,
        valkey,
        queue_url: str,
        handler: Handler,
        *,
        dedup_ttl_seconds: int,
        max_messages: int = 10,
        wait_time_seconds: int = 10,
    ) -> None:
        self._sqs = sqs
        self._valkey = valkey
        self._queue_url = queue_url
        self._handler = handler
        self._ttl = dedup_ttl_seconds
        self._max_messages = max_messages
        self._wait = wait_time_seconds

    async def _process(self, message: dict[str, Any]) -> None:
        event = json.loads(message["Body"])
        event_id = event["event_id"]
        dedup_key = f"event:{event_id}"

        if await self._valkey.exists(dedup_key):
            log.debug("duplicate event %s deduped", event_id)
            return  # already processed → ack (delete happens in poll_once)

        traceparent = (message.get("MessageAttributes") or {}).get(TRACEPARENT_ATTR, {}).get("StringValue")
        token = request_id_ctx.set(parse_trace_id(traceparent) or "")
        try:
            await self._handler(event)
        finally:
            request_id_ctx.reset(token)

        # Mark only after the handler succeeds — a crash before this re-processes,
        # which is safe because the handler's DB effect is idempotent.
        await self._valkey.set(dedup_key, "1", ex=self._ttl)

    async def poll_once(self) -> int:
        """Receive one batch; process + delete each. Returns messages handled.

        A handler error leaves the message on the queue (no delete) so SQS
        redelivers it and, past ``maxReceiveCount``, routes it to the DLQ.
        """
        resp = await self._sqs.receive_message(
            QueueUrl=self._queue_url,
            MaxNumberOfMessages=self._max_messages,
            WaitTimeSeconds=self._wait,
            MessageAttributeNames=["All"],
        )
        messages = resp.get("Messages", [])
        handled = 0
        for message in messages:
            try:
                await self._process(message)
            except Exception:  # boundary: poison message stays for SQS redrive → DLQ
                log.exception("event handler failed; leaving message for redrive")
                continue
            await self._sqs.delete_message(QueueUrl=self._queue_url, ReceiptHandle=message["ReceiptHandle"])
            handled += 1
        return handled

    async def run(self, stop) -> None:
        """Long-poll loop until ``stop`` (an ``asyncio.Event``) is set."""
        while not stop.is_set():
            await self.poll_once()
