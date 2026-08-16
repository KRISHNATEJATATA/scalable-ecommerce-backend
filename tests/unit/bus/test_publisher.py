"""Publisher tests: topic resolution and the traceparent attribute.

The production concern here is IAM, not plumbing: with the topic ARN namespace
configured (Terraform owns the topics) the relay must resolve ARNs by string and
never call ``sns:CreateTopic``, because its task role won't be allowed to. The
``create_topic`` fallback stays for LocalStack, where nothing pre-creates topics.
"""

from __future__ import annotations

import json
import uuid

import pytest

from src.shared.bus.publisher import SnsPublisher
from src.shared.bus.tracecontext import TRACEPARENT_ATTR, parse_trace_id

_ARN_PREFIX = "arn:aws:sns:us-east-1:123456789012:"
_DSN = "postgresql+asyncpg://u:p@localhost:5432/db"


def _settings(**overrides):
    from src.shared.config.setting import AppSettings

    return AppSettings(_env_file=None, database_url=_DSN, **overrides)


class FakeSns:
    """Records ``create_topic``/``publish`` calls; mints a plausible ARN."""

    def __init__(self) -> None:
        self.created: list[str] = []
        self.published: list[dict] = []

    async def create_topic(self, *, Name: str) -> dict:  # noqa: N803 - boto3 kwarg
        self.created.append(Name)
        return {"TopicArn": f"{_ARN_PREFIX}{Name}"}

    async def publish(self, *, TopicArn: str, Message: str, MessageAttributes: dict) -> dict:  # noqa: N803
        self.published.append({"arn": TopicArn, "body": Message, "attrs": MessageAttributes})
        return {"MessageId": str(uuid.uuid4())}


def _payload(trace_id: str = "0af7651916cd43dd8448eb211c80319c") -> str:
    return json.dumps({"type": "OrderPlaced", "event_id": str(uuid.uuid4()), "trace_id": trace_id})


@pytest.mark.asyncio
async def test_configured_arn_prefix_publishes_without_creating_topics() -> None:
    sns = FakeSns()
    publisher = SnsPublisher(sns, "ecommerce-", _ARN_PREFIX)

    await publisher.publish("OrderPlaced", _payload())

    assert sns.created == []  # the task role has sns:Publish only
    assert sns.published[0]["arn"] == f"{_ARN_PREFIX}ecommerce-OrderPlaced"


@pytest.mark.asyncio
async def test_without_an_arn_prefix_the_topic_is_created_on_demand() -> None:
    """The LocalStack path: no pre-created topics, so create_topic resolves them."""
    sns = FakeSns()
    publisher = SnsPublisher(sns, "ecommerce-")

    await publisher.publish("OrderPlaced", _payload())

    assert sns.created == ["ecommerce-OrderPlaced"]
    assert sns.published[0]["arn"] == f"{_ARN_PREFIX}ecommerce-OrderPlaced"


@pytest.mark.asyncio
async def test_topic_arns_are_resolved_once_per_process() -> None:
    sns = FakeSns()
    publisher = SnsPublisher(sns, "ecommerce-")

    for _ in range(3):
        await publisher.publish("OrderPlaced", _payload())

    assert sns.created == ["ecommerce-OrderPlaced"]  # cached after the first resolve
    assert len(sns.published) == 3


@pytest.mark.asyncio
async def test_payload_ships_verbatim_with_the_envelope_traceparent() -> None:
    """Consumers re-validate the body, so a byte-for-byte round trip is the contract."""
    sns = FakeSns()
    trace_id = "0af7651916cd43dd8448eb211c80319c"
    body = _payload(trace_id)

    await SnsPublisher(sns, "ecommerce-", _ARN_PREFIX).publish("OrderPlaced", body)

    sent = sns.published[0]
    assert sent["body"] == body
    assert parse_trace_id(sent["attrs"][TRACEPARENT_ATTR]["StringValue"]) == trace_id


@pytest.mark.asyncio
async def test_an_empty_envelope_trace_id_still_yields_a_valid_traceparent() -> None:
    """A producer that lost its trace context must not emit the invalid all-zero id."""
    sns = FakeSns()

    await SnsPublisher(sns, "ecommerce-", _ARN_PREFIX).publish("OrderPlaced", _payload(""))

    parsed = parse_trace_id(sns.published[0]["attrs"][TRACEPARENT_ATTR]["StringValue"])
    assert parsed is not None and parsed.strip("0")


# --- the ARN namespace this publisher concatenates onto -----------------------


def test_arn_prefix_is_normalised_and_blank_reads_as_unset() -> None:
    assert _settings(bus_topic_arn_prefix=_ARN_PREFIX).bus_topic_arn_prefix == _ARN_PREFIX
    # a missing trailing ':' would silently glue the account id to the topic name
    assert _settings(bus_topic_arn_prefix=_ARN_PREFIX.rstrip(":")).bus_topic_arn_prefix == _ARN_PREFIX
    assert _settings(bus_topic_arn_prefix="   ").bus_topic_arn_prefix is None
    assert _settings().bus_topic_arn_prefix is None


@pytest.mark.parametrize(
    "bad",
    [
        "arn:aws:sqs:us-east-1:123456789012:",  # wrong service
        "arn:aws:sns:us-east-1:",  # no account id
        "arn:aws:sns::123456789012:",  # no region
        "https://sns.us-east-1.amazonaws.com/",  # not an ARN at all
    ],
)
def test_a_malformed_arn_prefix_fails_at_startup_not_per_publish(bad: str) -> None:
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        _settings(bus_topic_arn_prefix=bad)
