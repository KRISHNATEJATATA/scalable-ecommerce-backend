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

Each drains a standard SQS queue with a DLQ; set the queue **visibility timeout ≥
the consumer's processing lease** (`CONSUMER_LEASE_TTL_SECONDS`) so a crashed
worker's in-flight message is redelivered rather than lost or double-processed.
Losing the cache worker degrades read latency (more DB reads, staleness bounded by
`PRODUCT_CACHE_TTL_SECONDS`) but is not a correctness incident; losing the relay or
image worker stalls events/uploads until it recovers (both replay safely).

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
(spoofed/oversize → `failed`, poison messages → DLQ). Bootstrap the local bucket,
queue and notification with `make s3-setup` (or the `s3-setup` compose service).
