# scalable-ecommerce-backend

Async, **API-first FastAPI** e-commerce backend (JSON only, no server-rendered
HTML) targeting **AWS ECS Fargate**. Topology is **monolith-with-replicas**: one
service, one shared auth dependency across routers. The app is a pure **OIDC
resource server against Keycloak** — it only validates Keycloak-issued RS256
access tokens (JWKS), never handles credentials/refresh, which keeps a future
auth-service split free.

## Architecture (summary)

Request flow — **do not skip layers**:

```
Route → Schema → Service → Repository → Model
```

- **Fully async, top to bottom.** Every route/service/repository is `async def`.
  Blocking/CPU-bound work is offloaded with `run_in_threadpool` /
  `asyncio.to_thread`. Alembic is the one deliberate sync exception.
- Routes are thin; **all DB queries live in `repositories/`**; **services return
  Pydantic schemas, never ORM models**.
- Errors are **RFC 9457 Problem Details** (one flat shape).
- **Valkey** holds ephemeral state only (rate-limit counters, idempotency keys,
  event-dedup keys, JWKS cache, cache-aside, cart state). Revocation is a short
  access-token TTL owned by Keycloak, not a `jti` denylist.

See [`docs/architecture.md`](docs/architecture.md) for the full picture.

## Tech stack

| Area | Choice |
|---|---|
| Runtime | Python 3.13, FastAPI, Uvicorn/Gunicorn |
| Data | PostgreSQL (async SQLAlchemy 2.x + `asyncpg`); Alembic (sync) |
| Ephemeral state | Valkey (redis-py-compatible) |
| Auth | OIDC resource server against Keycloak — validate-only RS256 (PyJWT `PyJWKClient`); `python-keycloak` for the Admin API |
| Config/validation | Pydantic v2 + pydantic-settings |
| Storage | S3 via `aioboto3` (LocalStack S3 locally) — presigned uploads + S3-event image worker |
| Event bus | Transactional outbox → SNS/SQS relay + idempotent consumers + DLQs (LocalStack locally) |
| Async worker | SQS (ElasticMQ locally); image worker drains S3 ObjectCreated events |
| Observability | `ecs-logging` + `python-json-logger`, Prometheus `/metrics` |
| Testing | pytest + pytest-asyncio, `httpx.AsyncClient`, Testcontainers-Postgres |
| Lint | Ruff (line-length 120) + Ruff-format; Spectral for OpenAPI |
| Deploy | Docker → ECR → ECS Fargate (Terraform) |

## Quickstart (local)

```bash
cp .env.example .env          # DATABASE_URL is required (app fails fast if unset)
make install                  # pip install -r requirements.txt && pip install -e ".[dev]"
make compose-up               # Postgres + Valkey + LocalStack (S3/SNS/SQS) + ElasticMQ + relay + image-worker
make run                      # uvicorn main:app --reload
make lint                     # ruff check + ruff format --check
make test                     # pytest tests/unit/ (coverage reported, not gated)
make hooks                    # install pre-commit (Ruff + Ruff-format + Spectral)
```

## Environment variables

All config is typed on `AppSettings` (`src/config/setting.py`) — code never reads
`os.environ` directly. `DATABASE_URL` is required; everything else has a local
default. See [`.env.example`](.env.example) for the full list.
