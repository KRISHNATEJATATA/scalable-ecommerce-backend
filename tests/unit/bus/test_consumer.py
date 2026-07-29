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
    """Minimal async stand-in: the get/set(nx)/eval the consumer uses.

    ``eval`` emulates the consumer's two owner-checked Lua scripts (complete-if-owner
    and release-if-owner): both gate on ``get(KEYS[1]) == ARGV[1]`` (the lease token),
    then either set the key to the ``done`` marker or delete it.
    """

    def __init__(self) -> None:
        self.store: dict[str, str] = {}

    async def get(self, key: str):
        return self.store.get(key)

    async def set(self, key: str, value: str, *, nx: bool = False, ex: int | None = None):
        if nx and key in self.store:
            return None  # not acquired → duplicate
        self.store[key] = value
        return True

    async def delete(self, key: str) -> int:
        return 1 if self.store.pop(key, None) is not None else 0

    async def eval(self, script: str, numkeys: int, *args):
        key = args[:numkeys][0]
        argv = args[numkeys:]
        if self.store.get(key) != argv[0]:  # owner check on the lease token
            return False if "'set'" in script else 0
        if "'set'" in script:  # complete-if-owner → write the completion marker
            self.store[key] = argv[1]
            return True
        self.store.pop(key, None)  # release-if-owner
        return 1


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
    # A contract-valid OrderPlaced envelope: the consumer now validates every
    # event against its registered schema before handling (poison → DLQ).
    body = json.dumps(
        {
            "type": "OrderPlaced",
            "schema_version": 1,
            "event_id": event_id,
            "trace_id": trace_id,
            "data": {
                "order_id": str(uuid.uuid4()),
                "user_id": str(uuid.uuid4()),
                "total": "19.99",
                "items": [{"product_id": str(uuid.uuid4()), "quantity": 1, "unit_price": "19.99"}],
            },
        }
    )
    return {
        "Body": body,
        "ReceiptHandle": handle,
        "MessageAttributes": {"traceparent": {"DataType": "String", "StringValue": format_traceparent(trace_id)}},
    }


def _consumer(sqs, valkey, handler) -> SqsConsumer:
    return SqsConsumer(
        sqs, valkey, "q-url", handler, dedup_ttl_seconds=60, lease_ttl_seconds=30, max_messages=10, wait_time_seconds=0
    )


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
async def test_contract_invalid_event_is_left_for_redrive() -> None:
    async def handler(event: dict) -> None:
        raise AssertionError("handler must not run for a contract-invalid event")

    # Missing required ``data`` payload → fails schema validation before handling.
    bad = json.dumps({"type": "OrderPlaced", "schema_version": 1, "event_id": str(uuid.uuid4()), "trace_id": "t"})
    valkey = FakeValkey()
    sqs = FakeSqs([{"Body": bad, "ReceiptHandle": "poison", "MessageAttributes": {}}])
    handled = await _consumer(sqs, valkey, handler).poll_once()

    assert handled == 0
    assert sqs.deleted == []  # not acked → redrive → DLQ
    assert valkey.store == {}  # never claimed


@pytest.mark.asyncio
async def test_inflight_lease_leaves_message_for_redrive() -> None:
    """A message whose event is mid-handle elsewhere (lease held, not done) is left
    for SQS redrive — never acked into the void, never double-processed."""
    event_id = str(uuid.uuid4())

    async def handler(event: dict) -> None:
        raise AssertionError("handler must not run while another worker holds the lease")

    valkey = FakeValkey()
    valkey.store[f"event:{event_id}"] = uuid.uuid4().hex  # another worker's lease token
    sqs = FakeSqs([_message(event_id, handle="inflight")])
    handled = await _consumer(sqs, valkey, handler).poll_once()

    assert handled == 0
    assert sqs.deleted == []  # left on the queue → redelivered after visibility timeout


@pytest.mark.asyncio
async def test_expired_worker_cannot_clobber_reclaimed_lease() -> None:
    """The lease is owner-safe: a worker whose lease expired and was re-claimed by
    another worker must not complete/delete the new owner's lease."""
    event_id = str(uuid.uuid4())
    marker = f"event:{event_id}"

    async def slow_then_check(event: dict) -> None:
        # Simulate our lease expiring and another worker re-claiming it mid-handle.
        valkey.store[marker] = "other-worker-token"

    valkey = FakeValkey()
    sqs = FakeSqs([_message(event_id, handle="a")])
    await _consumer(sqs, valkey, slow_then_check).poll_once()

    # Our success path must NOT have overwritten the other worker's lease with "done".
    assert valkey.store[marker] == "other-worker-token"
    # And because our completion CAS failed, we must NOT ack — the message is left
    # for redrive so the new owner (or a redelivery) completes it, never lost.
    assert sqs.deleted == []


@pytest.mark.asyncio
async def test_completed_event_marker_survives_for_dedup() -> None:
    """After a successful handle the marker is the long-lived ``done`` completion
    marker (not the short ``processing`` lease), so a later duplicate is deduped."""
    event_id = str(uuid.uuid4())
    calls: list[str] = []

    async def handler(event: dict) -> None:
        calls.append(event["event_id"])

    valkey = FakeValkey()
    sqs = FakeSqs([_message(event_id, handle="a")])
    await _consumer(sqs, valkey, handler).poll_once()

    assert calls == [event_id]
    assert valkey.store[f"event:{event_id}"] == "done"  # completion marker, not the lease


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
