"""SNS publisher — one topic per event type.

The relay hands the publisher the verbatim outbox ``payload`` (the serialized
``DomainEvent`` envelope) and its ``event_type``. The publisher resolves the
per-type topic ARN (``create_topic`` is idempotent — it returns the existing ARN
in the cloud where Terraform pre-creates topics, and creates on demand on
LocalStack) and publishes the payload as the SNS ``Message``, with the W3C
``traceparent`` (derived from the envelope ``trace_id``) as a message attribute.

The payload is shipped **verbatim**: consumers validate it against the
event registry, so the wire body must be exactly what the producer stored.
"""

from __future__ import annotations

import json

from src.shared.bus.constants import topic_name
from src.shared.bus.tracecontext import TRACEPARENT_ATTR, format_traceparent


class SnsPublisher:
    """Publishes outbox payloads to per-event-type SNS topics.

    ``sns`` is an entered aioboto3 SNS client. Topic ARNs are cached per process
    so steady-state publishing is a single ``publish`` call.
    """

    def __init__(self, sns, topic_prefix: str) -> None:
        self._sns = sns
        self._prefix = topic_prefix
        self._arns: dict[str, str] = {}

    async def _topic_arn(self, event_type: str) -> str:
        arn = self._arns.get(event_type)
        if arn is None:
            resp = await self._sns.create_topic(Name=topic_name(self._prefix, event_type))
            arn = resp["TopicArn"]
            self._arns[event_type] = arn
        return arn

    async def publish(self, event_type: str, payload: str) -> None:
        """Publish one verbatim outbox payload to its event-type topic."""
        trace_id = json.loads(payload).get("trace_id", "")
        await self._sns.publish(
            TopicArn=await self._topic_arn(event_type),
            Message=payload,
            MessageAttributes={
                TRACEPARENT_ATTR: {"DataType": "String", "StringValue": format_traceparent(trace_id)},
            },
        )
