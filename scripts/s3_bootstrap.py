"""Create the local S3 upload topology on LocalStack.

Idempotent bootstrap for `make compose-up`: the uploads bucket, the
``image-uploads`` SQS queue + DLQ, and the bucket's ObjectCreated→SQS
notification scoped to the ``uploads/`` prefix (so the worker's own ``public/``
writes never re-trigger it). Real infra is Terraform in the cloud (S3 event
notification → SQS); this is the local mirror.

Run: ``python -m scripts.s3_bootstrap`` (compose one-shot ``s3-setup``).
"""

from __future__ import annotations

import asyncio
import json
import logging

from src.catalog.domain.image_keys import PUBLIC_PREFIX, UPLOAD_PREFIX
from src.shared.bus.client import sqs_client
from src.shared.clients.s3_client import s3_client
from src.shared.config.logging import setup_logging
from src.shared.config.setting import get_settings

log = logging.getLogger("s3_bootstrap")

QUEUE_NAME = "image-uploads"
DLQ_NAME = "image-uploads-dlq"
MAX_RECEIVE_COUNT = 5


async def _ensure_bucket(s3, bucket: str) -> None:
    existing = await s3.list_buckets()
    if bucket not in {b["Name"] for b in existing.get("Buckets", [])}:
        await s3.create_bucket(Bucket=bucket)
    log.info("ensured bucket %s", bucket)


async def _queue_arn(sqs, url: str) -> str:
    resp = await sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])
    return resp["Attributes"]["QueueArn"]


async def _ensure_queue(sqs) -> str:
    dlq_url = (await sqs.create_queue(QueueName=DLQ_NAME))["QueueUrl"]
    dlq_arn = await _queue_arn(sqs, dlq_url)
    redrive = json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": MAX_RECEIVE_COUNT})
    url = (await sqs.create_queue(QueueName=QUEUE_NAME, Attributes={"RedrivePolicy": redrive}))["QueueUrl"]
    queue_arn = await _queue_arn(sqs, url)
    # Allow S3 to deliver notifications to the queue (required on real AWS).
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "s3.amazonaws.com"},
                "Action": "sqs:SendMessage",
                "Resource": queue_arn,
            }
        ],
    }
    await sqs.set_queue_attributes(QueueUrl=url, Attributes={"Policy": json.dumps(policy)})
    log.info("ensured queue %s (+ dlq)", QUEUE_NAME)
    return queue_arn


async def _ensure_notification(s3, bucket: str, queue_arn: str) -> None:
    await s3.put_bucket_notification_configuration(
        Bucket=bucket,
        NotificationConfiguration={
            "QueueConfigurations": [
                {
                    "QueueArn": queue_arn,
                    "Events": ["s3:ObjectCreated:*"],
                    "Filter": {"Key": {"FilterRules": [{"Name": "prefix", "Value": f"{UPLOAD_PREFIX}/"}]}},
                }
            ]
        },
    )
    log.info("ensured ObjectCreated→SQS notification on %s/%s/", bucket, UPLOAD_PREFIX)


async def _ensure_public_read(s3, bucket: str) -> None:
    """Grant anonymous ``s3:GetObject`` on the ``public/`` prefix only.

    Public product images serve UNSIGNED, so processed objects under ``public/``
    must be world-readable while raw ``uploads/`` stay private. In the cloud the
    equivalent is a private bucket fronted by CloudFront (OAC) — this local policy
    mirrors "public reads, private uploads" so the unsigned-GET path is real, not
    an artefact of LocalStack's permissive default.
    """
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "PublicReadProcessedImages",
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": f"arn:aws:s3:::{bucket}/{PUBLIC_PREFIX}/*",
            }
        ],
    }
    await s3.put_bucket_policy(Bucket=bucket, Policy=json.dumps(policy))
    log.info("ensured public-read policy on %s/%s/", bucket, PUBLIC_PREFIX)


async def bootstrap() -> None:
    settings = get_settings()
    if not settings.s3_bucket:
        raise RuntimeError("S3_BUCKET must be set to bootstrap uploads")
    async with s3_client(settings) as s3, sqs_client(settings) as sqs:
        await _ensure_bucket(s3, settings.s3_bucket)
        await _ensure_public_read(s3, settings.s3_bucket)
        queue_arn = await _ensure_queue(sqs)
        await _ensure_notification(s3, settings.s3_bucket, queue_arn)


if __name__ == "__main__":
    setup_logging(get_settings().log_level)
    asyncio.run(bootstrap())
