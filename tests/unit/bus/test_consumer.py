"""Consumer-logic tests: dedupe, trace propagation, DLQ-on-error.

These exercise the code we own — the effectively-once dedupe ordering, the
``traceparent`` → log-context propagation, and the leave-message-on-handler-error
behaviour that lets SQS redrive route poison messages to the DLQ. The SQS/Valkey
edges are tiny in-memory fakes; the real SNS→SQS→DLQ wiring is verified locally on
LocalStack (see ``docs/RUNBOOK.md`` / ``scripts/bus_bootstrap.py``), not here.
"""

from __future__ import annotations

import json
import uuid

import pytest

from src.shared.bus.consumer import SqsConsumer
from src.shared.bus.tracecontext import format_traceparent
from src.shared.config.logging import request_id_ctx


class FakeValkey:
    """Minimal async stand-in: just the ``exists``/``set`` the consumer uses."""

    def __init__(self) -> None:
        self.store: dict[str, str] = {}

    async def exists(self, key: str) -> int:
        return 1 if key in self.store else 0

    async def set(self, key: str, value: str, ex: int | None = None) -> None:
        self.store[key] = value


class FakeSqs:
    """In-memory SQS: hands out queued messages, records deletes."""

    def __init__(self, messages: list[dict]) -> None:
        self._inbox = list(messages)
        self.deleted: list[str] = []

    async def receive_message(self, *, QueueUrl, MaxNumberOfMessages, WaitTimeSeconds, MessageAttributeNames):  # noqa: N803
        batch, self._inbox = self._inbox[:MaxNumberOfMessages], self._inbox[MaxNumberOfMessages:]
        return {"Messages": batch} if batch else {}

    async def delete_message(self, *, QueueUrl, ReceiptHandle):  # noqa: N803
        self.deleted.append(ReceiptHandle)


def _message(event_id: str, *, trace_id: str = "0af7651916cd43dd8448eb211c80319c", handle: str = "rh") -> dict:
    body = json.dumps({"type": "OrderPlaced", "schema_version": 1, "event_id": event_id, "trace_id": trace_id})
    return {
        "Body": body,
        "ReceiptHandle": handle,
        "MessageAttributes": {"traceparent": {"DataType": "String", "StringValue": format_traceparent(trace_id)}},
    }


def _consumer(sqs, valkey, handler) -> SqsConsumer:
    return SqsConsumer(sqs, valkey, "q-url", handler, dedup_ttl_seconds=60, max_messages=10, wait_time_seconds=0)


@pytest.mark.asyncio
async def test_duplicate_delivery_is_processed_once() -> None:
    event_id = str(uuid.uuid4())
    calls: list[str] = []

    async def handler(event: dict) -> None:
        calls.append(event["event_id"])

    valkey = FakeValkey()
    sqs = FakeSqs([_message(event_id, handle="a"), _message(event_id, handle="b")])
    consumer = _consumer(sqs, valkey, handler)

    await consumer.poll_once()

    assert calls == [event_id]  # handler ran exactly once
    assert sqs.deleted == ["a", "b"]  # both messages acked (dupe deleted without reprocessing)
    assert f"event:{event_id}" in valkey.store


@pytest.mark.asyncio
async def test_traceparent_propagates_into_log_context() -> None:
    trace_id = "0af7651916cd43dd8448eb211c80319c"
    seen: list[str] = []

    async def handler(event: dict) -> None:
        seen.append(request_id_ctx.get())

    sqs = FakeSqs([_message(str(uuid.uuid4()), trace_id=trace_id)])
    await _consumer(sqs, FakeValkey(), handler).poll_once()

    assert seen == [trace_id]
    assert request_id_ctx.get() == ""  # context reset after handling


@pytest.mark.asyncio
async def test_handler_error_leaves_message_for_redrive() -> None:
    async def handler(event: dict) -> None:
        raise RuntimeError("poison")

    valkey = FakeValkey()
    sqs = FakeSqs([_message(str(uuid.uuid4()), handle="poison-1")])
    handled = await _consumer(sqs, valkey, handler).poll_once()

    assert handled == 0
    assert sqs.deleted == []  # not deleted → SQS redelivers → DLQ after maxReceiveCount
    assert valkey.store == {}  # never marked processed
