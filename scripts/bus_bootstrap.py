"""Create the local SNS/SQS bus topology on LocalStack.

Idempotent bootstrap for `make compose-up`: one SNS topic per registered event
type (kept in sync with `src/events` automatically), plus each consumer's SQS
queue + DLQ + subscription (raw message delivery, so the payload and the
`traceparent` attribute pass through untouched) with an SQS redrive policy.

Real infra is Terraform in the cloud; this is the local mirror. Add a consumer
here in the same phase you add its handler.
"""

from __future__ import annotations

import asyncio
import json
import logging

from src.events import EVENT_MODELS
from src.shared.bus.client import sns_client, sqs_client
from src.shared.bus.constants import topic_name
from src.shared.config.logging import setup_logging
from src.shared.config.setting import get_settings

log = logging.getLogger("bus_bootstrap")

# Local consumer wiring: queue name -> event types it subscribes to.
# Extend this as real consumers land (Phase 8+). One documented example so the
# end-to-end loop (relay → SNS → SQS → consumer → DLQ) is exercisable locally.
CONSUMERS: dict[str, list[str]] = {
    "order-events": ["OrderPlaced"],
    "catalog-cache": ["ProductUpdated", "ProductDeleted"],
}
MAX_RECEIVE_COUNT = 5


def _event_types() -> list[str]:
    return [m.model_fields["type"].default for m in EVENT_MODELS]


async def _ensure_topics(sns, prefix: str) -> dict[str, str]:
    arns: dict[str, str] = {}
    for event_type in _event_types():
        resp = await sns.create_topic(Name=topic_name(prefix, event_type))
        arns[event_type] = resp["TopicArn"]
    log.info("ensured %d SNS topics", len(arns))
    return arns


async def _queue_arn(sqs, url: str) -> str:
    resp = await sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])
    return resp["Attributes"]["QueueArn"]


async def _ensure_consumer(
    sqs, sns, name: str, topics: dict[str, str], event_types: list[str], visibility_timeout: int
) -> None:
    dlq = (await sqs.create_queue(QueueName=f"{name}-dlq"))["QueueUrl"]
    dlq_arn = await _queue_arn(sqs, dlq)
    redrive = json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": MAX_RECEIVE_COUNT})
    # VisibilityTimeout >= the consumer's processing-lease TTL, so a crashed
    # worker's lease has expired before SQS redelivers (no premature DLQ; see
    # AppSettings.consumer_lease_ttl_seconds and docs/DEPLOYMENT.md).
    attributes = {"RedrivePolicy": redrive, "VisibilityTimeout": str(visibility_timeout)}
    queue = (await sqs.create_queue(QueueName=name, Attributes=attributes))["QueueUrl"]
    # create_queue only applies Attributes when it *creates* the queue; an already
    # existing queue keeps its old settings. Re-apply so a changed lease/visibility
    # invariant actually lands on re-run (idempotent bootstrap).
    await sqs.set_queue_attributes(QueueUrl=queue, Attributes=attributes)
    queue_arn = await _queue_arn(sqs, queue)
    for event_type in event_types:
        await sns.subscribe(
            TopicArn=topics[event_type],
            Protocol="sqs",
            Endpoint=queue_arn,
            Attributes={"RawMessageDelivery": "true"},
            ReturnSubscriptionArn=True,
        )
    log.info("ensured consumer %s (dlq, %d subscriptions)", name, len(event_types))


async def bootstrap() -> None:
    settings = get_settings()
    visibility_timeout = settings.consumer_lease_ttl_seconds
    async with sns_client(settings) as sns, sqs_client(settings) as sqs:
        topics = await _ensure_topics(sns, settings.bus_topic_prefix)
        for name, event_types in CONSUMERS.items():
            await _ensure_consumer(sqs, sns, name, topics, event_types, visibility_timeout)


if __name__ == "__main__":
    setup_logging(get_settings().log_level)
    asyncio.run(bootstrap())
