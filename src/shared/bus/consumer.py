"""SQS consumer — idempotent, DLQ-backed, trace-propagating.

Reads one SQS subscription (SNS → SQS with **raw message delivery**, so the body
is the verbatim event payload and the ``traceparent`` rides as a message
attribute). For each message:

1. Parse the envelope and validate it against its registered, versioned contract.
2. If ``event:{event_id}`` is already the ``done`` marker → duplicate → ack and skip.
3. Atomically claim a **short processing lease** under a unique token (``SET
   event:{id} <token> NX EX lease_ttl``). If the claim is lost (another worker in
   flight), leave the message for redrive rather than double-processing.
4. Extract ``traceparent`` → pin the trace-id onto the log context for the handler.
5. Run the handler. On success, upgrade the lease to a long ``done`` completion
   marker **only if we still own the token**; the message is deleted (acked) only
   when that completion CAS succeeds. If the lease was lost (handler outran its
   TTL, another worker re-claimed), the CAS fails and the message is **left for
   redrive** rather than acked — the new owner will complete it.
6. On handler error **or cancellation**, **release the lease (only if still ours,
   shielded so a cancellation can't abort the release) and leave the message** —
   SQS redelivers it, and after the queue's ``maxReceiveCount`` it moves to the
   per-subscription DLQ (replay via ``docs/RUNBOOK.md``).

The Valkey dedup keys give best-effort effectively-once processing on top of the
handler's own idempotent DB write. Two states share the ``event:{id}`` key so a
crash can't lose an event: a **short processing lease** (a unique per-delivery
token, TTL sized to the SQS visibility window) claimed before the handler runs,
and a **long completion marker** (``event:{id}`` = ``done``) written only after the
handler succeeds. A worker that crashes mid-handle lets the *lease* expire, so
redelivery re-claims and reprocesses instead of the event being discarded for the
full dedup TTL. The lease carries a **token** and is completed/released with
owner-checked Lua, so a worker whose lease already expired can never overwrite or
delete the lease a different worker has since claimed. A handler error releases the
lease immediately (owner-checked ``DEL``) so SQS redrive can retry at once.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

from src.events.registry import validate_event
from src.shared.bus.tracecontext import TRACEPARENT_ATTR, parse_trace_id
from src.shared.config.logging import request_id_ctx

log = logging.getLogger(__name__)

Handler = Callable[[dict[str, Any]], Awaitable[None]]

_DONE = "done"  # completion-marker value (processed)

# Complete-if-owner: upgrade our lease to the long-lived ``done`` marker only if we
# still own the lease token. A worker whose lease expired (and was re-claimed by
# another) thus can't overwrite the new owner's state.
_COMPLETE_IF_OWNER_LUA = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('set', KEYS[1], ARGV[2], 'EX', ARGV[3])
end
return false
"""

# Release-if-owner: drop our lease only if we still own the token, so an error/
# expiry can't delete a lease a different worker has since claimed.
_RELEASE_IF_OWNER_LUA = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
end
return 0
"""


class SqsConsumer:
    """Drains one SQS queue into an async ``handler``.

    ``sqs`` is an entered aioboto3 SQS client; ``valkey`` is an async
    redis-py-compatible client used for the ``event:{id}`` lease/completion marker.
    """

    def __init__(
        self,
        sqs,
        valkey,
        queue_url: str,
        handler: Handler,
        *,
        dedup_ttl_seconds: int,
        lease_ttl_seconds: int,
        max_messages: int = 10,
        wait_time_seconds: int = 10,
    ) -> None:
        self._sqs = sqs
        self._valkey = valkey
        self._queue_url = queue_url
        self._handler = handler
        self._ttl = dedup_ttl_seconds
        self._lease_ttl = lease_ttl_seconds
        self._max_messages = max_messages
        self._wait = wait_time_seconds

    async def _is_done(self, key: str) -> bool:
        value = await self._valkey.get(key)
        if isinstance(value, bytes | bytearray):
            value = value.decode()
        return value == _DONE

    async def _process(self, message: dict[str, Any]) -> bool:
        """Handle one message. Returns ``True`` if it may be acked (deleted).

        ``True``  → processed now, or already completed by another worker → ack.
        ``False`` → currently in flight elsewhere (lease held) → leave for redrive.
        raises    → contract-invalid or handler error → leave for redrive → DLQ.
        """
        event = json.loads(message["Body"])

        # Contract gate: the payload must validate against its registered, versioned
        # event schema before any handling. An unknown or malformed event is poison —
        # it raises here, the message is left on the queue, and SQS redrives it to
        # the DLQ (never silently handled).
        validate_event(event)

        event_id = event["event_id"]
        marker_key = f"event:{event_id}"

        # Already completed by a prior/concurrent delivery → dedupe, ack.
        if await self._is_done(marker_key):
            log.debug("duplicate event %s (already done) deduped", event_id)
            return True

        # Claim a SHORT processing lease under a UNIQUE token. Exactly one worker
        # wins; a crash after this only holds the event for the lease TTL (not the
        # full dedup TTL), after which redelivery re-claims and reprocesses — so a
        # crash can't lose the event. The token makes the lease owner-safe: an
        # expired worker resuming late can neither complete nor delete a lease that a
        # different worker has since claimed (see the owner-checked Lua below).
        token = uuid.uuid4().hex
        acquired = await self._valkey.set(marker_key, token, nx=True, ex=self._lease_ttl)
        if not acquired:
            # Another worker is mid-handle (or just finished) → don't double-process.
            # Leave the message; redelivery after the visibility timeout will see the
            # completion marker (or a re-claimable expired lease).
            if await self._is_done(marker_key):
                return True
            log.debug("event %s in flight elsewhere; leaving for redrive", event_id)
            return False

        traceparent = (message.get("MessageAttributes") or {}).get(TRACEPARENT_ATTR, {}).get("StringValue")
        ctx_token = request_id_ctx.set(parse_trace_id(traceparent) or "")
        try:
            await self._handler(event)
        except BaseException:
            # Release the lease (only if still ours) so the redelivered message can
            # be reprocessed at once, without clobbering a re-claimed lease.
            # ``BaseException`` so a **cancellation** also frees the lease now instead
            # of leaving it held until expiry. Retain the release task and await it to
            # completion even if a *second* cancellation lands mid-flight (a bare
            # ``shield`` would then orphan it) — this can't leave the lease held.
            release = asyncio.ensure_future(self._valkey.eval(_RELEASE_IF_OWNER_LUA, 1, marker_key, token))
            while not release.done():
                try:
                    await asyncio.shield(release)
                except asyncio.CancelledError:
                    continue  # our await was cancelled again; keep waiting for the release
                except Exception:
                    break  # the release itself failed — don't mask the original handler error
            raise
        finally:
            request_id_ctx.reset(ctx_token)

        # Handler succeeded → upgrade OUR lease to a long-lived completion marker,
        # but ONLY if we still own the token. If our lease already expired (the
        # handler outran the lease TTL) another worker has re-claimed it and is
        # reprocessing; the completion CAS then fails and we must NOT ack — deleting
        # the message here would drop the redelivery the new owner is relying on.
        # Leaving it for redrive is safe: the real owner writes the ``done`` marker,
        # so the next delivery dedupes and acks.
        completed = await self._valkey.eval(_COMPLETE_IF_OWNER_LUA, 1, marker_key, token, _DONE, self._ttl)
        if not completed:
            log.warning("event %s lease lost before completion; leaving for redrive (not acking)", event_id)
            return False
        return True

    async def poll_once(self) -> int:
        """Receive one batch; process + delete each. Returns messages acked.

        A handler error or an in-flight duplicate leaves the message on the queue
        (no delete) so SQS redelivers it and, past ``maxReceiveCount``, routes a
        genuine poison message to the DLQ.
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
                acked = await self._process(message)
            except Exception:  # boundary: poison message stays for SQS redrive → DLQ
                log.exception("event handler failed; leaving message for redrive")
                continue
            if not acked:  # in-flight elsewhere → leave for redrive, no error
                continue
            await self._sqs.delete_message(QueueUrl=self._queue_url, ReceiptHandle=message["ReceiptHandle"])
            handled += 1
        return handled

    async def run(self, stop) -> None:
        """Long-poll loop until ``stop`` (an ``asyncio.Event``) is set."""
        while not stop.is_set():
            await self.poll_once()
