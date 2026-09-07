"""Create the local S3 upload topology on LocalStack.

Idempotent bootstrap for `make compose-up`: the uploads bucket (+ a lifecycle rule
that expires raw ``uploads/`` objects), the ``image-uploads`` SQS queue + DLQ, and
the bucket's ObjectCreated→SQS notification scoped to the ``uploads/`` prefix (so
the worker's own ``public/`` writes never re-trigger it). Real infra is Terraform
in the cloud (S3 event notification → SQS); this is the local mirror.

Re-runnable: queue attributes are re-applied with ``set_queue_attributes`` because
``create_queue`` only honours ``Attributes`` when it actually creates the queue.

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


async def _ensure_queue(sqs, bucket: str, visibility_timeout: int) -> str:
    dlq_url = (await sqs.create_queue(QueueName=DLQ_NAME))["QueueUrl"]
    dlq_arn = await _queue_arn(sqs, dlq_url)
    redrive = json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": MAX_RECEIVE_COUNT})
    # VisibilityTimeout >= the worst-case single-image processing time (download +
    # sniff + three WebP encodes). SQS's 30s default would redeliver a slow-but-
    # succeeding message mid-flight and burn redrive attempts until it DLQ'd despite
    # every attempt succeeding — see AppSettings.image_visibility_timeout_seconds.
    attributes = {"RedrivePolicy": redrive, "VisibilityTimeout": str(visibility_timeout)}
    # Create bare, then apply attributes: create_queue only honours Attributes when
    # it *creates* the queue, and passing values that differ from an existing queue's
    # is an outright QueueAlreadyExists error — so a changed redrive/visibility
    # invariant could never land on re-run. set_queue_attributes is the idempotent path.
    url = (await sqs.create_queue(QueueName=QUEUE_NAME))["QueueUrl"]
    queue_arn = await _queue_arn(sqs, url)
    # Allow S3 to deliver notifications to the queue (required on real AWS), scoped
    # to THIS bucket + account. Without the aws:SourceArn/aws:SourceAccount
    # conditions the S3 service principal is a confused deputy: any account's bucket
    # could send forged ObjectCreated records to this queue (AWS's documented
    # requirement for S3→SQS notifications).
    account_id = queue_arn.split(":")[4]
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "s3.amazonaws.com"},
                "Action": "sqs:SendMessage",
                "Resource": queue_arn,
                "Condition": {
                    "ArnLike": {"aws:SourceArn": f"arn:aws:s3:::{bucket}"},
                    "StringEquals": {"aws:SourceAccount": account_id},
                },
            }
        ],
    }
    # create_queue only applies Attributes when it *creates* the queue (and errors
    # outright if an existing queue's differ), so the queue is created bare above and
    # everything is applied here — the same idempotency contract as
    # scripts/bus_bootstrap.
    await sqs.set_queue_attributes(QueueUrl=url, Attributes={**attributes, "Policy": json.dumps(policy)})
    log.info("ensured queue %s (+ dlq, visibility=%ss)", QUEUE_NAME, visibility_timeout)
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


async def _ensure_lifecycle(s3, bucket: str, retention_days: int) -> None:
    """Expire raw ``uploads/`` objects after ``retention_days``.

    Nothing references a raw upload once the worker has written its public
    renditions, and a **rejected** (possibly malicious) upload must not be retained
    forever — S3 reclaims them so the app never needs a delete pass in the ingest
    path (deleting there would break a redelivery, which must still be able to
    download the object). Scoped to ``uploads/`` only: ``public/`` objects are live
    CDN content. Also aborts abandoned multipart uploads, which are invisible
    orphans otherwise.
    """
    await s3.put_bucket_lifecycle_configuration(
        Bucket=bucket,
        LifecycleConfiguration={
            "Rules": [
                {
                    "ID": "expire-raw-uploads",
                    "Status": "Enabled",
                    "Filter": {"Prefix": f"{UPLOAD_PREFIX}/"},
                    "Expiration": {"Days": retention_days},
                    "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 1},
                }
            ]
        },
    )
    log.info("ensured %s/ lifecycle expiry after %dd", UPLOAD_PREFIX, retention_days)


async def _ensure_cors(s3, bucket: str, origins: list[str]) -> None:
    """Allow the browser to POST presigned uploads from the SPA origin.

    Direct-from-browser presigned uploads are cross-origin: without bucket CORS
    the browser refuses to read (or even send) the multipart POST, so the upload
    leg dies even with a reachable presign URL. Scoped to the configured SPA
    origins (``CORS_ALLOW_ORIGINS``) — never ``*`` — mirroring what Terraform
    must configure on the real bucket in the cloud. Empty origins skip the rule
    (fail visibly in logs rather than open the bucket to every origin).
    """
    if not origins:
        log.warning("CORS_ALLOW_ORIGINS is empty — skipping bucket CORS (browser presigned POSTs would fail)")
        return
    await s3.put_bucket_cors(
        Bucket=bucket,
        CORSConfiguration={
            "CORSRules": [
                {
                    "AllowedOrigins": origins,
                    "AllowedMethods": ["POST", "PUT"],
                    "AllowedHeaders": ["*"],
                    "ExposeHeaders": ["ETag"],
                    "MaxAgeSeconds": 3000,
                }
            ]
        },
    )
    log.info("ensured bucket CORS on %s for %s", bucket, origins)


async def bootstrap() -> None:
    settings = get_settings()
    if not settings.s3_bucket:
        raise RuntimeError("S3_BUCKET must be set to bootstrap uploads")
    async with s3_client(settings) as s3, sqs_client(settings) as sqs:
        await _ensure_bucket(s3, settings.s3_bucket)
        await _ensure_public_read(s3, settings.s3_bucket)
        await _ensure_cors(s3, settings.s3_bucket, settings.cors_allow_origins)
        await _ensure_lifecycle(s3, settings.s3_bucket, settings.image_upload_retention_days)
        queue_arn = await _ensure_queue(sqs, settings.s3_bucket, settings.image_visibility_timeout_seconds)
        await _ensure_notification(s3, settings.s3_bucket, queue_arn)


if __name__ == "__main__":
    setup_logging(get_settings().log_level)
    asyncio.run(bootstrap())
