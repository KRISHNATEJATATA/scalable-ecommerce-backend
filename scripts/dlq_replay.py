"""DLQ replay — move dead-lettered messages back onto their source queues.

A consumer queue forwards a message to its per-subscription DLQ (``<queue>-dlq``,
after ``MAX_RECEIVE_COUNT`` receives — ``scripts/bus_bootstrap.py``). The message
is usually *fine*: the consumer was broken, or the dependency behind it was. Once
the fault is fixed the dead letters belong back on the source queue, not deleted
by hand — this is that operator one-shot, the in-repo twin of ``aws sqs
start-message-move-task`` (``docs/RUNBOOK.md`` §4).

It speaks to whatever ``BUS_ENDPOINT_URL`` points at, so the same command works
against LocalStack locally and real SQS in prod (the AWS-native move needs two
queue ARNs and isn't reliably implemented on LocalStack — one mechanism that
works in both places beats two that don't).

* ``--list`` reports every DLQ's depth and moves nothing: look before you leap.
* A pass receives a batch, re-sends each body **with its message attributes**
  (the ``traceparent`` the consumer logs under), and only then deletes the DLQ
  copy. Send-then-delete is deliberate: a crash in between leaves the message on
  both queues, and the consumers are idempotent (ADR 0007) — an at-least-once
  duplicate beats an at-most-once lost message.
* Replay is a *second* delivery, so it is safe exactly because the handlers are
  idempotent: the Valkey dedupe marker is a fast path, and every effect is
  naturally idempotent or has a backstop (the notification consumer's
  ``sent_emails`` record covers its dedupe-TTL expiry).
* A genuinely un-processable message simply redrives again after
  ``MAX_RECEIVE_COUNT`` receives. Fix the consumer/data first, then replay —
  the DLQ is not a trash can.

Per-run bound: ``--limit`` (default 1000) caps how much one invocation moves, so
a 100k-deep DLQ is drained in deliberate steps, not one accidental command.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from collections.abc import Sequence

from prometheus_client import Counter

from scripts.bus_bootstrap import CONSUMERS
from src.shared.bus.client import sqs_client
from src.shared.config.setting import get_settings

log = logging.getLogger("dlq_replay")

#: Queues that own a DLQ. The event-bus ones come from the bootstrap's own map
#: (the single source of truth for the topology); ``image-uploads`` is the S3
#: path (``scripts/s3_bootstrap.py``), which is not part of the event bus.
_SOURCES: tuple[str, ...] = (*CONSUMERS, "image-uploads")

#: SQS caps ReceiveMessage at 10; also the batch size per send/delete round.
_BATCH = 10

#: Cap per DLQ per run — see the module docstring.
DEFAULT_LIMIT = 1000

replayed_total = Counter(
    "dlq_replayed_total",
    "Messages moved from a dead-letter queue back onto its source queue.",
    ["queue"],
)


def dlq_name(queue: str) -> str:
    """The DLQ name for a consumer queue: ``f"{queue}-dlq"`` (bus_bootstrap's rule)."""
    return f"{queue}-dlq"


async def _queue_url(sqs, name: str) -> str | None:
    """URL for a queue name, or ``None`` when the queue doesn't exist yet."""
    try:
        return (await sqs.get_queue_url(QueueName=name))["QueueUrl"]
    except sqs.exceptions.QueueDoesNotExist:
        return None


async def dlq_depth(sqs, queue: str) -> int:
    """Approximate visible depth of ``<queue>-dlq`` (``0`` when the DLQ is absent)."""
    url = await _queue_url(sqs, dlq_name(queue))
    if url is None:
        return 0
    attributes = await sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["ApproximateNumberOfMessages"])
    return int(attributes["Attributes"]["ApproximateNumberOfMessages"])


async def replay_queue(sqs, queue: str, *, limit: int = DEFAULT_LIMIT) -> int:
    """Move up to ``limit`` messages from ``<queue>-dlq`` back onto ``<queue>``.

    Returns how many moved. A queue (or DLQ) that doesn't exist is a logged
    no-op, not an error: a fleet that never dead-lettered anything has nothing
    to replay.
    """
    source = await _queue_url(sqs, queue)
    dead = await _queue_url(sqs, dlq_name(queue))
    if source is None or dead is None:
        log.info("no %s/%s queue on this bus; nothing to replay", queue, dlq_name(queue))
        return 0

    moved = 0
    while moved < limit:
        batch = await sqs.receive_message(
            QueueUrl=dead,
            MaxNumberOfMessages=min(_BATCH, limit - moved),
            MessageAttributeNames=["All"],
            WaitTimeSeconds=1,  # one short long-poll per round: drain promptly, never hang
        )
        messages = batch.get("Messages") or []
        if not messages:
            break
        for message in messages:
            # Send first, delete second — the crash window is a duplicate, not a loss.
            await sqs.send_message(
                QueueUrl=source,
                MessageBody=message["Body"],
                MessageAttributes=message.get("MessageAttributes") or {},
            )
            await sqs.delete_message(QueueUrl=dead, ReceiptHandle=message["ReceiptHandle"])
            moved += 1
        log.info("replayed %d/%d message(s) from %s", moved, limit, dlq_name(queue))

    if moved:
        replayed_total.labels(queue=queue).inc(moved)
    return moved


async def replay_all(sqs, *, queues: Sequence[str] | None = None, limit: int = DEFAULT_LIMIT) -> dict[str, int]:
    """Replay every DLQ in ``queues`` (default: the whole known topology).

    Returns ``{queue: messages moved}`` — one entry per queue, ``0`` included, so
    the caller can report what it found as well as what it moved.
    """
    moved: dict[str, int] = {}
    for queue in queues or _SOURCES:
        moved[queue] = await replay_queue(sqs, queue, limit=limit)
    return moved


def main() -> None:  # pragma: no cover - process entrypoint
    """`python -m scripts.dlq_replay [--queue NAME]... [--limit N] [--list]`."""
    from src.shared.config.logging import setup_logging
    from src.shared.observability.worker_metrics import push_worker_metrics

    parser = argparse.ArgumentParser(description="Move dead-lettered messages back onto their source queues.")
    parser.add_argument(
        "--queue",
        action="append",
        metavar="NAME",
        help="replay just this queue's DLQ (repeatable; default: every known queue)",
    )
    parser.add_argument(
        "--limit", type=int, default=DEFAULT_LIMIT, help=f"max messages per DLQ (default {DEFAULT_LIMIT})"
    )
    parser.add_argument("--list", action="store_true", help="report DLQ depths and exit (moves nothing)")
    args = parser.parse_args()

    unknown = [queue for queue in (args.queue or []) if queue not in _SOURCES]
    if unknown:  # CLI boundary: a typo must not read as "nothing to do"
        parser.error(f"unknown queue(s) {unknown}; known queues: {', '.join(sorted(_SOURCES))}")
    if args.limit < 1:
        parser.error("--limit must be a positive integer")

    settings = get_settings()
    setup_logging(settings.log_level)

    async def _run() -> dict[str, int]:
        async with sqs_client(settings) as sqs:
            if args.list:
                return {queue: await dlq_depth(sqs, queue) for queue in (args.queue or _SOURCES)}
            return await replay_all(sqs, queues=args.queue, limit=args.limit)

    counts = asyncio.run(_run())
    for queue, count in counts.items():
        log.info("%s %s: %d", "depth" if args.list else "replayed", queue, count)
    log.info("total across %d queue(s): %d", len(counts), sum(counts.values()))
    if not args.list:
        # One-shot shape: no scrape port to expose, so report through the
        # Pushgateway when one is configured (a no-op otherwise), like the prune.
        push_worker_metrics(settings, job="dlq-replay")


if __name__ == "__main__":  # pragma: no cover
    main()
