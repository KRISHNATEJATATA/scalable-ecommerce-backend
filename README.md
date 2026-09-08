# scalable-ecommerce-backend

Async, **API-first FastAPI** e-commerce backend (JSON only, no server-rendered
HTML) targeting **AWS ECS Fargate**. Topology is **monolith-with-replicas**: one
service, one shared auth dependency across routers. Structured as a **modular
monolith** (`catalog · inventory · orders · payments · identity · cart`) with
schema-per-module boundaries — microservices-ready, not yet split. The app is a
pure **OIDC resource server against Keycloak** — it only validates
Keycloak-issued RS256 access tokens (JWKS), never handles
credentials/refresh, which keeps a future auth-service split free.

## Architecture (summary)

Request flow — **do not skip layers**:

```
Route → Schema → Service → Repository → Model
```

- **Fully async, top to bottom.** Every route/service/repository is `async def`.
  Blocking/CPU-bound work is offloaded with `run_in_threadpool` /
  `asyncio.to_thread`. Alembic is the one deliberate sync exception.
- Routes are thin; **all DB queries live in each module's `adapters/db`**;
  **services return Pydantic schemas, never ORM models**.
- **Checkout is an orchestrated saga** (order-first, persisted `saga_log`,
  compensation, recovery poller) with two-layer idempotency: a Valkey fast path
  over the durable `UNIQUE(user_id, idempotency_key)` + body-hash backstop.
- Errors are **RFC 9457 Problem Details** (one flat shape).
- **Valkey** holds ephemeral state only (rate-limit counters, checkout
  idempotency fast path, event-dedup keys, product cache-aside, cart state).
  Revocation is a short access-token TTL owned by Keycloak, not a `jti`
  denylist; JWKS caching is a process-wide `PyJWKClient`, not Valkey.

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
| Async worker | SNS/SQS consumers (LocalStack locally; ElasticMQ hosts the legacy `emails` queue); `service`-role workers — outbox relay, image worker (S3 ObjectCreated), catalog cache-invalidation consumer, cart product-event consumer, reservation reaper, payment reconciler, saga recovery poller |
| Observability | `ecs-logging` + `python-json-logger`, Prometheus `/metrics` |
| Testing | pytest + pytest-asyncio, `httpx.AsyncClient`, Testcontainers-Postgres |
| Lint | Ruff (line-length 120) + Ruff-format; Spectral for OpenAPI |
| Deploy | Docker → ECR → ECS Fargate (Terraform) |

## Quickstart (local)

```bash
cp .env.example .env          # DATABASE_URL is required (app fails fast if unset)
make install                  # pip install -r requirements.txt && pip install -e ".[dev]"
make compose-up               # Postgres + Valkey + LocalStack (S3/SNS/SQS) + ElasticMQ + Keycloak + Mailpit
                              # + one-shot migrate/bus-setup/s3-setup
                              # + app + relay + image-worker + cache-worker + cart-consumer
                              # + reaper + payment-reconciler + saga-recovery
make seed                     # opt-in demo state: 4 users, 11 products (5+6 across two
                              # merchants) with images + stock (9x25, one sold-out, one low).
                              # Idempotent; `make seed-reset` wipes + re-seeds (fresh user subs)
make run                      # uvicorn main:app --reload
make lint                     # ruff check + ruff format --check + import-linter
make test                     # pytest tests/unit/ (coverage reported, not gated)
make typecheck                # basedpyright (advisory only — baseline carries pre-existing errors)
make hooks                    # install pre-commit (Ruff + Ruff-format + Spectral)
```

## Environment variables

All config is typed on `AppSettings` (`src/shared/config/setting.py`) — code
never reads `os.environ` directly. `DATABASE_URL` is required; everything else
has a local default. See [`.env.example`](.env.example) for the full list.

### Demo seeding

`make seed` (the `catalog-seed` compose one-shot, profile-gated under
`profiles: ["seed"]`) populates demo users, products, images and stock into the
**running** stack. It is never triggered implicitly by `compose up`. It refuses
to run unless `SEED_DEMO_DATA=1` is set (the compose service sets it; the
`.env.example` default is `0`), so hardcoded demo credentials can never reach a
real deployment. Demo accounts (usernames `demo.consumer`, `demo.merchant`,
`demo.merchant2`, `demo.admin`) are created via the Keycloak Admin API with
known passwords — dev/local only.
