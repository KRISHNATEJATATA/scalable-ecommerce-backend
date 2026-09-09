"""The real-bus crown jewels: outbox → relay → SNS → SQS → consumer → DLQ, end to end.

``tests/unit/bus/*`` pin the relay/consumer *semantics* against Testcontainers-
Postgres with fakes on the SNS/SQS edge; this module is the one place the real
edge runs, because no fake can prove the things that actually break in transit:

* the SNS subscription ships **raw** bodies — an envelope-wrapped body would
  fail ``validate_event`` and poison every consumer;
* the ``traceparent`` message attribute survives the SNS→SQS hop intact;
* a contract-invalid message is never handled — it redrives to the DLQ;
* duplicate delivery is **effectively-once downstream** (real Valkey dedupe);
* a state-write that committed with nothing on the bus (the outbox's crash
  window) is shipped by the relay on its next pass.

Topology mirrors ``scripts/bus_bootstrap.py`` (raw delivery + RedrivePolicy),
with ``maxReceiveCount=1`` and a short visibility timeout so the DLQ path
completes inside the test instead of after five 30s cycles. LocalStack image
matches docker-compose. Real Postgres (the shared Testcontainers fixture) and
real Valkey — never faked, never SQLite.
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from types import SimpleNamespace

import aioboto3
import pytest
from sqlalchemy import text
from testcontainers.core.container import DockerContainer
from valkey.asyncio import Valkey

from src.events import REGISTRY
from src.inventory.adapters.db.repository import InventoryRepository
from src.inventory.application.service import InventoryService
from src.shared.bus.constants import topic_name
from src.shared.bus.consumer import SqsConsumer
from src.shared.bus.publisher import SnsPublisher
from src.shared.bus.relay import OutboxRelay
from src.shared.bus.tracecontext import TRACEPARENT_ATTR, parse_trace_id

_REGION = "us-east-1"
_TOPIC_PREFIX = "ecommerce-"
_LEASE_TTL = 1
_VISIBILITY = 3  # > 2× lease, per the bootstrap's invariant


@pytest.fixture(scope="module")
def _localstack():
    """LocalStack with SNS/SQS, image-matched to docker-compose."""
    container = DockerContainer("localstack/localstack:3.8").with_exposed_ports(4566)
    with container:
        host = container.get_container_host_ip()
        port = int(container.get_exposed_port(4566))
        endpoint = f"http://{host}:{port}"

        import boto3

        probe = boto3.client(
            "sqs", endpoint_url=endpoint, region_name=_REGION, aws_access_key_id="test", aws_secret_access_key="test"
        )
        for _ in range(100):  # wait out the container's slow start; never a skip
            try:
                probe.list_queues()
                yield endpoint
                return
            except Exception:
                time.sleep(0.3)
        raise RuntimeError("LocalStack testcontainer never became ready")


@pytest.fixture(scope="module")
def _valkey_url():
    with DockerContainer("valkey/valkey:8").with_exposed_ports(6379) as container:
        yield f"redis://{container.get_container_host_ip()}:{container.get_exposed_port(6379)}/0"


@pytest.fixture
async def valkey(_valkey_url):
    client = Valkey.from_url(_valkey_url)
    await client.flushall()
    yield client
    await client.flushall()
    await client.aclose()


@pytest.fixture
async def aws(_localstack):
    session = aioboto3.Session(aws_access_key_id="test", aws_secret_access_key="test", region_name=_REGION)
    async with (
        session.client("sns", endpoint_url=_localstack, region_name=_REGION) as sns,
        session.client("sqs", endpoint_url=_localstack, region_name=_REGION) as sqs,
    ):
        yield SimpleNamespace(sns=sns, sqs=sqs)


async def _wire_queue(aws, *, event_type: str, queue_name: str, max_receive_count: int = 1) -> str:
    """Topic + queue + DLQ + raw-delivery subscription, as bus_bootstrap does."""
    sns, sqs = aws.sns, aws.sqs
    dlq_url = (await sqs.create_queue(QueueName=f"{queue_name}-dlq"))["QueueUrl"]
    dlq_arn = (await sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["QueueArn"]))["Attributes"]["QueueArn"]
    queue_url = (await sqs.create_queue(QueueName=queue_name, Attributes={"VisibilityTimeout": str(_VISIBILITY)}))[
        "QueueUrl"
    ]
    queue_arn = (await sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"]))["Attributes"][
        "QueueArn"
    ]
    await sqs.set_queue_attributes(
        QueueUrl=queue_url,
        Attributes={
            "RedrivePolicy": json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": max_receive_count}),
            "VisibilityTimeout": str(_VISIBILITY),
        },
    )
    topic_arn = (await sns.create_topic(Name=topic_name(_TOPIC_PREFIX, event_type)))["TopicArn"]
    await sns.subscribe(
        TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn, Attributes={"RawMessageDelivery": "true"}
    )
    return queue_url


def _publisher(sns) -> SnsPublisher:
    return SnsPublisher(sns, _TOPIC_PREFIX)  # no ARN prefix: the LocalStack create_topic path


def _consumer(aws, valkey, queue_url: str, handler, *, wait: int = 5) -> SqsConsumer:
    """``handler`` may be a sync callable; it is wrapped so the consumer's
    ``await self._handler(...)`` sees a coroutine function."""

    async def _handler(event):
        result = handler(event)
        if asyncio.iscoroutine(result):
            await result

    return SqsConsumer(
        aws.sqs,
        valkey,
        queue_url,
        _handler,
        consumer_name="localstack-test",
        dedup_ttl_seconds=300,
        lease_ttl_seconds=_LEASE_TTL,
        wait_time_seconds=wait,
    )


async def _drain_dlq(aws, queue_url: str) -> list[dict]:
    """Read the wired DLQ (``<queue-name>-dlq``) — LocalStack's list_queues is
    prefix-based, so the name is derived from the queue name, not the URL."""
    queue_name = queue_url.rsplit("/", 1)[-1]
    listing = await aws.sqs.list_queues(QueueNamePrefix=f"{queue_name}-dlq")
    dlq_url = listing["QueueUrls"][0]
    received = await aws.sqs.receive_message(
        QueueUrl=dlq_url, WaitTimeSeconds=5, MessageAttributeNames=["All"], VisibilityTimeout=0
    )
    return received.get("Messages", [])


async def test_sns_ships_the_body_verbatim_with_the_traceparent_attribute(aws, valkey):
    """The raw-delivery + trace-propagation contract, end to end over real SNS/SQS."""
    event = _registered_order_placed()
    body = event.model_dump_json()
    queue_url = await _wire_queue(aws, event_type="OrderPlaced", queue_name="trace-queue")

    await _publisher(aws.sns).publish("OrderPlaced", body)

    received = await aws.sqs.receive_message(
        QueueUrl=queue_url, WaitTimeSeconds=10, MessageAttributeNames=["All"], VisibilityTimeout=0
    )
    (message,) = received["Messages"]
    assert message["Body"] == body, "raw delivery: the consumer must see the producer's exact bytes"
    traceparent = message["MessageAttributes"][TRACEPARENT_ATTR]["StringValue"]
    assert parse_trace_id(traceparent) == event.trace_id


def _registered_order_placed():
    """A contract-valid OrderPlaced built from the registry model."""
    model = REGISTRY[("OrderPlaced", 1)]
    data = {
        "order_id": str(uuid.uuid4()),
        "user_id": str(uuid.uuid4()),
        "total": "19.99",
        "items": [{"product_id": str(uuid.uuid4()), "quantity": 1, "unit_price": "9.99"}],
    }
    return model.new(trace_id=uuid.uuid4().hex, data=data)


async def test_contract_invalid_message_redrives_to_the_dlq_and_is_never_handled(aws, valkey):
    """A poison payload is left, redelivered, and moved to the DLQ — the handler
    never sees it (SqsConsumer re-validates against the event registry)."""
    poison = json.dumps({"type": "NotRegistered", "schema_version": 1, "event_id": str(uuid.uuid4())})
    queue_url = await _wire_queue(aws, event_type="OrderPlaced", queue_name="poison-queue")
    handled: list[dict] = []
    consumer = _consumer(aws, valkey, queue_url, handled.append, wait=1)

    await _publisher(aws.sns).publish("OrderPlaced", poison)

    assert await consumer.poll_once() == 0  # left for redrive, not handled
    assert handled == []
    await asyncio.sleep(_VISIBILITY + 0.5)  # let the visibility window lapse
    assert await consumer.poll_once() == 0  # this receive pushes it past maxReceiveCount → DLQ

    dead = await _drain_dlq(aws, queue_url)
    assert len(dead) == 1
    assert dead[0]["Body"] == poison  # verbatim into the DLQ for replay (docs/RUNBOOK.md)


async def test_duplicate_delivery_is_effectively_once_downstream(aws, valkey):
    """Two SQS copies of one event → the handler runs exactly once (real Valkey
    dedupe: lease claim suppresses the concurrent copy, the ``done`` marker
    dedupes the redelivered one), and the queue drains to empty."""
    event = _registered_order_placed()
    body = event.model_dump_json()
    queue_url = await _wire_queue(aws, event_type="OrderPlaced", queue_name="dedup-queue")
    handled: list[dict] = []
    consumer = _consumer(aws, valkey, queue_url, handled.append, wait=1)

    publisher = _publisher(aws.sns)
    await publisher.publish("OrderPlaced", body)
    await publisher.publish("OrderPlaced", body)  # the duplicate the outbox crash-window guarantees

    await consumer.poll_once()
    assert len(handled) == 1, "at most one handler run can ever claim the lease"

    await asyncio.sleep(_VISIBILITY + 0.5)  # the suppressed copy becomes visible again
    await consumer.poll_once()
    assert len(handled) == 1, "the redelivery is deduped by the done marker, not re-handled"

    depth = await aws.sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["ApproximateNumberOfMessages"])
    assert depth["Attributes"]["ApproximateNumberOfMessages"] == "0"


async def test_outbox_state_write_ships_on_the_relay_next_pass(aws, valkey, sessionmaker_factory):
    """The crash window: state + outbox row commit in one transaction
    (a real inventory reservation), nothing is on the bus, and the relay's next
    pass ships it — where a real consumer validates and handles it."""
    sessionmaker = sessionmaker_factory
    sku = f"sku-{uuid.uuid4().hex[:12]}"
    order_id = uuid.uuid4()
    async with sessionmaker() as session:
        await session.execute(
            text("INSERT INTO inventory.inventory (sku, on_hand, reserved, version) VALUES (:sku, 5, 0, 1)"),
            {"sku": sku},
        )
        await session.commit()

    async with sessionmaker() as session:
        service = InventoryService(InventoryRepository(session), reservation_ttl_seconds=600)
        await service.reserve(sku, 2, order_id)
    # State is committed; nothing was ever pushed to the bus in the request path.

    queue_url = await _wire_queue(aws, event_type="StockReserved", queue_name="relay-queue")
    received = await aws.sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=1)
    assert received.get("Messages", []) == [], "a committed state-write must not reach the bus on its own"

    relay = OutboxRelay(sessionmaker, _publisher(aws.sns), batch_size=10, schemas=("inventory",))
    assert await relay.drain_once() == 1
    async with sessionmaker() as session:
        assert (
            await session.execute(text("SELECT count(*) FROM inventory.outbox WHERE published_at IS NULL"))
        ).scalar_one() == 0

    handled: list[dict] = []
    consumer = _consumer(aws, valkey, queue_url, handled.append)
    await consumer.poll_once()
    (event,) = handled
    assert event["type"] == "StockReserved"
    assert event["data"]["sku"] == sku and event["data"]["order_id"] == str(order_id)
    assert event["data"]["quantity"] == 2
