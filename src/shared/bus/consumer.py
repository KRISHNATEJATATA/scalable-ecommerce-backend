"""SQS consumer — idempotent, DLQ-backed, trace-propagating.

Reads one SQS subscription (SNS → SQS with **raw message delivery**, so the body
is the verbatim event payload and the ``traceparent`` rides as a message
attribute). For each message:

1. Parse the envelope and validate it against its registered, versioned contract.
2. If ``event:{consumer}:{event_id}`` is already the ``done`` marker → duplicate → ack and skip.
3. Atomically claim a **short processing lease** under a unique token (``SET
   event:{consumer}:{id} <token> NX EX lease_ttl``). If the claim is lost (another worker in
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
handler's own idempotent DB write. Keys are **namespaced by consumer**
(``event:{consumer}:{event_id}``) because SNS fans one event out to several
subscriptions: a global ``event:{id}`` key would let the first subscriber that
finishes suppress every *other* subscriber's handler. Dedup is therefore
per-subscription, which is the only scope at which "already processed" is true.

Two states share the ``event:{consumer}:{id}`` key so a
crash can't lose an event: a **short processing lease** (a unique per-delivery
token, TTL sized to the SQS visibility window) claimed before the handler runs,
and a **long completion marker** (= ``done``) written only after the
handler succeeds. A worker that crashes mid-handle lets the *lease* expire, so
redelivery re-claims and reprocesses instead of the event being discarded for the
full dedup TTL. The lease carries a **token** and is completed/released with
owner-checked Lua, so a worker whose lease already expired can never overwrite or
delete the lease a different worker has since claimed. A handler error releases the
lease immediately (owner-checked ``DEL``) so SQS redrive can retry at once.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

from src.events.registry import validate_event
from src.shared.bus.polling import poll_forever
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
    redis-py-compatible client used for the ``event:{consumer}:{id}`` lease/completion
    marker. ``consumer_name`` is the stable identity of this subscription (e.g.
    ``catalog-cache``) — it scopes dedup so one subscriber completing an event never
    suppresses another subscriber's handler for the same fanned-out event.
    """

    def __init__(
        self,
        sqs,
        valkey,
        queue_url: str,
        handler: Handler,
        *,
        consumer_name: str,
        dedup_ttl_seconds: int,
        lease_ttl_seconds: int,
        max_messages: int = 10,
        wait_time_seconds: int = 10,
    ) -> None:
        self._sqs = sqs
        self._valkey = valkey
        self._queue_url = queue_url
        self._handler = handler
        self._consumer_name = consumer_name
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
        # Contract gate: the payload must validate against its registered, versioned
        # event schema before any handling. An unknown or malformed event is poison —
        # it raises here, the message is left on the queue, and SQS redrives it to
        # the DLQ (never silently handled). The handler gets the *validated,
        # normalized* event back, never the raw body.
        event = validate_event(message["Body"])

        event_id = event["event_id"]
        # Per-subscription scope: the same event delivered to another subscription
        # gets its own marker, so fan-out consumers never dedupe each other away.
        marker_key = f"event:{self._consumer_name}:{event_id}"

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
        """Receive one batch; process + delete each concurrently. Returns messages acked.

        Messages are processed **concurrently** (one coroutine per message): SQS
        starts every message's visibility clock at receive time, so a serial
        for-loop makes batch duration ≈ ``sum(handlers)`` and tail messages
        reappear mid-batch, burning redrive attempts toward the DLQ despite
        handler success. Concurrently the batch takes ≈ ``max(handler)`` — the
        correctness of in-flight duplicates is already guaranteed by the
        per-event Valkey lease (``_process`` claims ``SET NX EX`` first).

        Per-message boundaries (one message can never disturb its siblings):
        - handler error or contract-invalid event → log.exception, no delete
          (left for redrive → DLQ);
        - in-flight duplicate (``_process`` → False) → no delete, no error;
        - ``delete_message`` failure → log.exception, no delete counted — the
          message is redelivered and deduped/acked on its next delivery.
        """
        resp = await self._sqs.receive_message(
            QueueUrl=self._queue_url,
            MaxNumberOfMessages=self._max_messages,
            WaitTimeSeconds=self._wait,
            MessageAttributeNames=["All"],
        )
        messages = resp.get("Messages", [])
        handled = 0
        for ok in await asyncio.gather(*(self._process_and_ack(message) for message in messages)):
            if ok:  # deleted = acked; anything else is left for SQS redrive
                handled += 1
        return handled

    async def _process_and_ack(self, message: dict[str, Any]) -> bool:
        """Process one message and delete (ack) it on success.

        Owns its own try/except so one poison message or a failing delete never
        escapes into :meth:`poll_once` and cancels its batch siblings. Returns
        ``True`` only when the message was actually deleted (counted as handled).
        """
        try:
            acked = await self._process(message)
        except Exception:  # boundary: poison message stays for SQS redrive → DLQ
            log.exception("event handler failed; leaving message for redrive")
            return False
        if not acked:  # in-flight elsewhere → leave for redrive, no error
            return False
        try:
            await self._sqs.delete_message(QueueUrl=self._queue_url, ReceiptHandle=message["ReceiptHandle"])
        except Exception:
            # Don't count it as handled: the message is redelivered after the
            # visibility timeout and the ``done`` marker dedupes/acks that delivery.
            log.exception("failed to delete message after successful handling; leaving for redrive")
            return False
        return True

    async def run(self, stop) -> None:
        """Long-poll loop until ``stop`` (an ``asyncio.Event``) is set.

        Transient receive/delete failures are retried with backoff rather than
        killing the worker — see :mod:`src.shared.bus.polling`.
        """
        await poll_forever(self.poll_once, stop, log)
