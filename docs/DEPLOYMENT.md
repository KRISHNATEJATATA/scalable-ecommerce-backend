# Deployment

Target: **AWS ECS Fargate** behind an ALB, image in **ECR**, IaC in **Terraform**.
Full Fargate is built (Phase 12b); EKS is described only.

## Build & push

```bash
# Tag by git SHA, never `latest`.
SHA=$(git rev-parse --short HEAD)
docker build -t <account>.dkr.ecr.<region>.amazonaws.com/ecommerce-backend:$SHA .
aws ecr get-login-password --region <region> | docker login --username AWS \
  --password-stdin <account>.dkr.ecr.<region>.amazonaws.com
docker push <account>.dkr.ecr.<region>.amazonaws.com/ecommerce-backend:$SHA
```

## Apply infrastructure

```bash
cd infra/terraform          # (Phase 12b)
terraform init
terraform validate
terraform plan  -var "image_tag=$SHA"
terraform apply -var "image_tag=$SHA"
```

The ECS service runs multiple identical Fargate tasks (monolith-with-replicas)
behind the ALB. The task role grants S3 access — **no AWS keys in code or env**.

### Service-role workers (separate Fargate services)

Alongside the web tasks, run each `service`-role worker as its own long-running
ECS service (same image, different `command`, no ALB target — scaled on queue
depth):

| Worker | Command | Drains | Purpose |
|---|---|---|---|
| Relay | `python -m src.shared.bus.relay` | Postgres `outbox` | ships unpublished rows → SNS (SKIP LOCKED) |
| Image worker | `python -m src.catalog.adapters.image_worker` | `image-uploads` | sniff · re-encode · thumbnails → `image_status` |
| Cache worker | `python -m src.catalog.adapters.cache_worker` | `catalog-cache` | invalidate Valkey read-cache on `ProductUpdated`/`ProductDeleted` |
| Cart consumer | `python -m src.cart.adapters.cart_consumer` | `cart-events` | refresh/prune Valkey cart snapshots on `ProductUpdated`/`ProductDeleted` |
| Reservation reaper | `python -m src.inventory.adapters.reaper` | Postgres `reservations` | release holds past `expires_at` (SKIP LOCKED) so a stalled saga can't leak stock |
| Payment reconciler | `python -m src.payments.adapters.reconciler` | Postgres `payments` | resolve charges still `pending` past their grace window by asking the gateway (missed-webhook backstop) |

The reaper polls Postgres, not a queue: run it as a small always-on service, or as
an **EventBridge-scheduled one-off task** with `--once` (it exits after a single
sweep). Either way it is safe to run N replicas — the claim takes `FOR UPDATE SKIP
LOCKED`. Losing it doesn't oversell (holds stay held); it means abandoned checkouts
keep stock out of circulation until it returns, so alarm on the **expired-hold backlog**
(`SELECT count(*) FROM inventory.reservations WHERE status = 'held' AND expires_at <= now()`)
for liveness rather than on counter silence — a worker that never runs emits nothing. Its
`inventory_reaper_released_total` still reaches Prometheus via `WORKER_METRICS_PORT` (looping)
or `METRICS_PUSHGATEWAY_URL` (`--once`); see `docs/RUNBOOK.md` §8. Set
`RESERVATION_TTL_SECONDS` **longer than the checkout saga's step timeouts**.

The **image, cache, and cart workers** drain a standard SQS queue with a DLQ; set the queue
**visibility timeout ≥ the consumer's processing lease** (`CONSUMER_LEASE_TTL_SECONDS`)
so a crashed worker's in-flight message is redelivered rather than lost or
double-processed. The **relay, reaper, and payment reconciler poll Postgres instead**
(`outbox`, `reservations`, `payments`), so their pacing is
`RELAY_POLL_INTERVAL_SECONDS` / `RESERVATION_REAPER_POLL_INTERVAL_SECONDS` /
`PAYMENT_RECONCILIATION_POLL_INTERVAL_SECONDS`. The reconciler needs no extra IAM:
it speaks to the gateway over HTTPS from whatever egress the task already has.
Losing the cache worker degrades read latency (more DB reads, staleness bounded by
`PRODUCT_CACHE_TTL_SECONDS`) but is not a correctness incident; losing the relay or
image worker stalls events/uploads until it recovers (both replay safely). Losing
the reconciler strands paid charges in `pending` (and their orders with them) until
it returns — alarm on the stuck-pending count in RUNBOOK §9. The cart consumer is
pure Valkey (no Postgres): size it by memory, not connections.
`CART_MAX_ITEMS` / `CART_MAX_QTY_PER_LINE` cap one caller's memory amplification;
`CART_TTL_SECONDS` is the rolling inactivity expiry (~30d) — eviction empties a
cart, which is acceptable here and never is for an order.

**Topic ARNs.** Terraform provisions the per-event-type SNS topics, so give the relay task
role `sns:Publish` only and point `BUS_TOPIC_ARN_PREFIX` at the ARN namespace
(`arn:aws:sns:<region>:<account-id>:` — the topic name from `BUS_TOPIC_PREFIX` is appended).
The relay then resolves ARNs by string with no API call; **it refuses to start** when
`BUS_ENDPOINT_URL` is unset (real AWS) and this is missing, rather than falling back to
`sns:CreateTopic` and failing `AccessDenied` on the first event. Locally the variable stays
empty and `scripts/bus_bootstrap.py` creates the topics on LocalStack.

**Worker metrics.** Only the API serves `/metrics`, so every worker above needs its own
export or its counters are invisible. Set `WORKER_METRICS_PORT` on the long-running worker
services and scrape it like any other target (docker-compose sets it on all six workers and
publishes 9101–9106). Scheduled `--once` tasks (the reaper) exit
between scrapes, so they push at exit instead — point `METRICS_PUSHGATEWAY_URL` at a
Pushgateway. Both are opt-in; unset means no port bound and no push attempted. Alert rules
and the postgres_exporter query behind the reaper's liveness signal ship in
`ops/prometheus/`.

**Connection budget.** Every process builds its own pool, and they all draw on the same
RDS `max_connections`:

```
api    = (DB_POOL_SIZE + DB_MAX_OVERFLOW) x WEB_CONCURRENCY x api_tasks     # 15 x 2 = 30/task
worker = (DB_WORKER_POOL_SIZE + DB_WORKER_MAX_OVERFLOW) x worker_tasks      #  2     =  2/task
probe  = 1 per in-flight /v1/ready (NullPool: opened and closed per probe)
total  = api + worker + probe  <  max_connections  (db.t3.medium ≈ 340)
```

Workers are single-task loops holding one session at a time, so they use the small
worker pool automatically (`create_engine(settings, worker=True)`) — at the API's sizing
four workers would have burned ~60 connections for nothing. At the defaults ten API tasks
plus four workers is ~308, which fits but leaves little headroom: past that, either shrink
`DB_POOL_SIZE`/`WEB_CONCURRENCY` or front RDS with **RDS Proxy / PgBouncer**
rather than raising `max_connections`.

**Health checks.** Point the ALB at `/v1/ready` and set its timeout **above**
`READINESS_PROBE_TIMEOUT_SECONDS` (default 2s per dependency). Only Postgres gates
readiness — a Valkey outage returns `200 {"status": "degraded"}` because the app falls
through to the DB, and deregistering every task over a cache blip would be a
self-inflicted outage. The Postgres probe runs on its own `NullPool` engine, so a
saturated request pool shows up as slow requests, not as a dead database that
deregisters the task and pushes its load onto the tasks that are already saturated.

**`/metrics` is unauthenticated** and served on the same port as the API. Do not route it
from the public ALB — add a listener rule denying `/metrics` (or scrape it privately on a
separate port) so pod-level counters aren't world-readable.

## Rolling out a new event version (consumer-first, non-negotiable order)

Every service runs the **same image** but as separate ECS services, so a rollout
updates them one service at a time — and that order is a correctness decision, not
a preference. A consumer that meets a `(type, schema_version)` it doesn't have
registered raises `UnknownEventError`, never deletes the message, and the queue
routes it to the DLQ after `maxReceiveCount` (5) receives. Deploying producers
first therefore **manufactures a DLQ backlog** out of perfectly valid traffic.

Order for any `schema_version` bump (the live example is product events v1 → v2,
which added `data.product_version`):

1. **Consumers first.** Update every service that *reads domain events off the bus*
   to the image that registers the new version, and wait for it to reach a steady
   state (`aws ecs wait services-stable`). Old-version messages keep validating
   because the previous model stays in `EVENT_MODELS` — the new code accepts
   **both**. Today that is exactly one service: the **cache worker**
   (`catalog-cache`), the only `validate_event` consumer of product events.
2. **Verify.** DLQ depth flat, consumer error rate flat, `/v1/health` green.
3. **Producers last.** Deploy every service that *emits* events:
   - the **API/web** service (create · update · delete · presign), and
   - the **image worker** — it consumes raw **S3 ObjectCreated** notifications (no
     `schema_version`, so it is not a bus consumer), but it *writes*
     `ProductUpdatedV2` outbox rows when it flips `image_status`. Shipping it in the
     consumer phase would put V2 on the bus before the cache worker is replaced,
     which is the failure this ordering exists to prevent.

   The **relay** is version-agnostic — it ships opaque outbox rows and never
   validates — so it can go in either phase; keep it with the producers to hold the
   fleet on one tag.
4. **Retire the old version** only after it can no longer exist anywhere: queue
   retention **plus** the DLQ replay window. Until then the old model stays
   registered.

```bash
# 1. consumers (bus readers only)
aws ecs update-service --cluster ecommerce --service ecommerce-cache-worker --force-new-deployment
aws ecs wait services-stable --cluster ecommerce --services ecommerce-cache-worker
# 3. producers (API + image worker, relay alongside)
aws ecs update-service --cluster ecommerce --service ecommerce-api          --force-new-deployment
aws ecs update-service --cluster ecommerce --service ecommerce-image-worker --force-new-deployment
aws ecs update-service --cluster ecommerce --service ecommerce-relay        --force-new-deployment
```

A rollback runs the mirror image: roll **producers back first**, consumers after —
never leave producers on a version the consumers can't parse.

`src/events/registry.py` pins `PRODUCED_VERSIONS` (what producers put on the wire)
and a test fails if production code emits anything else, so the producer half of the
bump can't merge as an unremarked one-line change. Recovering a DLQ that filled
because the order was violated: `docs/RUNBOOK.md` § 4.

## Migrations (one-off task, not at app boot)

Run Alembic as a dedicated one-off ECS task against RDS, before shifting traffic:

```bash
aws ecs run-task \
  --cluster ecommerce \
  --task-definition ecommerce-migrate \
  --launch-type FARGATE \
  --overrides '{"containerOverrides":[{"name":"app","command":["alembic","upgrade","head"]}]}'
```

Alembic uses the **sync** `psycopg2` driver; the app uses async `asyncpg`.

## Secret & config wiring

- Config is typed on `AppSettings`; supply values via ECS task-definition
  environment / secrets.
- Secrets (DB password, JWT private key) come from **Secrets Manager / SSM**,
  injected as env vars — never baked into the image.
- Every env var maps to an `AppSettings` field and appears in `.env.example`.

## Health checks

- `GET /v1/health` — liveness (always 200 while the process is up).
- `GET /v1/ready` — readiness (200 only when critical deps are reachable; 503
  otherwise). Wire the ALB target-group health check to `/v1/ready`.

## Local setup (parity)

`make compose-up` runs the prod-equivalent stack locally:

| Prod | Local |
|---|---|
| RDS Postgres | Postgres container |
| S3 | LocalStack S3 (`S3_ENDPOINT_URL`) |
| ElastiCache for Valkey | Valkey container |
| SQS / SNS | LocalStack (bus); ElasticMQ (relay dev) |
| Secrets Manager / SSM | `.env` + env vars |

## LocalStack S3 vs real S3 caveat

Locally, uploads go to **LocalStack S3** via `S3_ENDPOINT_URL` (not MinIO —
LocalStack can emit S3 `ObjectCreated` → SQS notifications, which the image
worker consumes to mirror prod). In AWS, leave `S3_ENDPOINT_URL` unset so
`aioboto3` targets real S3 and uses the ECS task role. Store the **object key**
(not a full URL); public product images are served unsigned via the CDN base
(`S3_PUBLIC_BASE_URL`), private assets via short-TTL presigned GET URLs.

### Image upload pipeline

Merchant calls `POST /products/{id}/image:presign` (ownership + content-type +
size validated) → uploads raw bytes to a presigned S3 POST under
`uploads/{product_id}/…` → S3 `ObjectCreated` → SQS `image-uploads` → the
**image worker** sniffs the real bytes (`python-magic`), re-encodes to WebP
(stripping EXIF) and generates thumbnails off the event loop, writes
`public/{product_id}/…`, and flips `products.image_status` to `ready`
(spoofed/oversize → `failed`, poison messages → DLQ). A `ready` product exposes
`image_url` plus `image_thumbnail_urls` (`thumb_256`/`thumb_64`). The presigned
POST's size policy is pinned to the declared `content_length` (clamped by
`IMAGE_MAX_UPLOAD_BYTES`). Bootstrap the local bucket, lifecycle rule, queue and
notification with `make s3-setup` (or the `s3-setup` compose service); the queue's
visibility timeout comes from `IMAGE_VISIBILITY_TIMEOUT_SECONDS` and the S3→SQS
send policy is scoped with `aws:SourceArn`/`aws:SourceAccount` — mirror both in
Terraform.

#### Required task-role S3 permissions

| Action | On | Why |
|---|---|---|
| `s3:PutObject` | `arn:aws:s3:::<bucket>/*` | worker writes `public/` renditions |
| `s3:GetObject` | `arn:aws:s3:::<bucket>/*` | worker downloads the raw upload |
| `s3:DeleteObject` | `arn:aws:s3:::<bucket>/*` | reclaim of superseded renditions |
| **`s3:ListBucket`** | `arn:aws:s3:::<bucket>` | **required** — see below |

`s3:ListBucket` is not optional. Without it S3 answers a *missing* key with
`AccessDenied` (403) instead of `NoSuchKey`, so the adapter cannot map it to a
terminal `ObjectNotFoundError` — a lifecycle-expired raw upload would then retry
until it burns its redrive attempts and DLQs, leaving the product `pending`
forever. Grant `ListBucket` on the bucket ARN (the app never lists objects
itself; the grant exists purely so 404s stay 404s).
