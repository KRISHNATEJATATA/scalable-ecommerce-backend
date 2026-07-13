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
| S3 | MinIO (`S3_ENDPOINT_URL`) |
| ElastiCache for Valkey | Valkey container |
| SQS | ElasticMQ |
| Secrets Manager / SSM | `.env` + env vars |

## MinIO vs real S3 caveat

Locally, uploads go to **MinIO** via `S3_ENDPOINT_URL`. In AWS, leave
`S3_ENDPOINT_URL` unset so `aioboto3` targets real S3 and uses the ECS task role.
Store the **object key** (not a full URL); build public/CDN URLs for product
images and presigned URLs for private assets.
