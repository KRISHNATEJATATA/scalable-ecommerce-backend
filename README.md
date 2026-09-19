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
| Async worker | SNS/SQS consumers (LocalStack locally); `service`-role workers — outbox relay, image worker (S3 ObjectCreated), catalog cache-invalidation consumer, cart product-event consumer, reservation reaper, payment reconciler, saga recovery poller, notification consumer (order-confirmation email) |
| Observability | `ecs-logging` + `python-json-logger`, Prometheus `/metrics` |
| Testing | pytest + pytest-asyncio, `httpx.AsyncClient`, Testcontainers-Postgres |
| Lint | Ruff (line-length 120) + Ruff-format; Spectral for OpenAPI |
| Deploy | Multi-stage Docker image → ECR; ECS Fargate target documented in [DEPLOYMENT.md](docs/DEPLOYMENT.md) (IaC not yet authored) |

## Quickstart (local)

```bash
cp .env.example .env          # DATABASE_URL is required (app fails fast if unset)
make install                  # pip install -r requirements.txt && pip install -e ".[dev]"
make compose-up               # Postgres + Valkey + LocalStack (S3/SNS/SQS) + Keycloak + Mailpit
                              # + one-shot migrate/bus-setup/s3-setup
                              # + app + relay + image-worker + cache-worker + cart-consumer
                              # + notification-consumer + reaper + payment-reconciler
                              # + saga-recovery + retention-prune
make seed                     # demo state: 5 users, 11 products (5+6 across two
                              # merchants) with images + stock (9x25, one sold-out, one low).
                              # Idempotent; `make seed-reset` wipes + re-seeds (fresh user subs)
                              # demo.suspended signs in but is refused 403 on /v1/me
make run                      # uvicorn main:app --reload
make lint                     # ruff check + ruff format --check + import-linter
make test                     # pytest tests/unit/ (coverage reported, not gated)
make typecheck                # basedpyright (advisory only — baseline carries pre-existing errors)
make loadtest                 # k6 checkout load test vs the running stack (fails when p95 >= 300 ms)
make hooks                    # install pre-commit (Ruff + Ruff-format + Spectral)
```

### Load testing

`make loadtest` runs [k6](https://k6.io) against a running stack
(`make compose-up && make seed && make run`) and drives the real checkout flow.
Setup provisions **one ephemeral Keycloak user per VU** (via the dev realm's
Admin API, deleted on teardown) so each VU races its own cart — sharing the
demo consumer's cart across VUs would measure the harness, not the service —
warms their JIT identity rows, and restocks the first seeded product so every
iteration lands on the happy path. Its pass/fail line is the ticket-16 SLO —
**checkout p95 < 300 ms** — plus a `checks > 99%` guard, so the test fails
(exit code non-zero) when the service can't hold the SLO. Tune with `K6_RATE`
(arrivals/s), `K6_DURATION`, `K6_VUS` (users created & VU cap), `BASE_URL`,
`KEYCLOAK_URL`. Requires `k6` on PATH (`winget install k6 --source winget` /
`brew install k6`).

### Multi-replica proof (concurrency under real scale-out)

`make compose-up-multi` runs the **same compose file** with `--scale app=2
--scale relay=2 --scale notification-consumer=2` — the documented one-command
multi-replica topology (port ranges in `docker-compose.yml` keep host mappings
collision-free). Then `make multi-loadtest` runs
[`loadtest/multi_replica.js`](loadtest/multi_replica.js), which deliberately
re-adds the contention the regular load test removes: **one shared user, one
shared cart, and two concurrent checkouts firing the same `Idempotency-Key`**
(`http.batch()`) so both app replicas see the same `(user_id, key)` at the same
instant. The composite `UNIQUE(user_id, idempotency_key)` must arbitrate — the
winner inserts, the loser rolls back and replays the winner's stored response —
and the run **fails when `proof_failures > 0`** (any non-201 racer or mismatched
order ids). This is "exactly one order per idempotency key" as an observation,
not an architecture-diagram claim. The nightly CI loadtest job brings the stack
up with 2 app replicas too, but it runs `checkout.js` (the latency SLO), **not**
this correctness proof — run `make multi-loadtest` to exercise it. See
`.github/workflows/ci.yml` for the shared-runner limitation the p95 gate carries.

## Environment variables

All config is typed on `AppSettings` (`src/shared/config/setting.py`) — code
never reads `os.environ` directly. `DATABASE_URL` is required; everything else
has a local default. See [`.env.example`](.env.example) for the full list.

### Demo seeding

`make seed` (the `catalog-seed` compose one-shot, profile-gated under
`profiles: ["seed"]`) populates demo users, products, images and stock into the
**running** stack. It is never triggered implicitly by `compose up`. Seeding is
**enabled by default**: the seeder refuses to run only when `SEED_DEMO_DATA=0`
(a kill switch for shared/staging databases — the compose service sets `1`, and
`.env.example` ships `1`). Demo accounts (usernames `demo.consumer`,
`demo.merchant`, `demo.merchant2`, `demo.admin`) are created via the Keycloak
Admin API with known passwords — dev/local only. A fifth account,
`demo.suspended` (BCR-005), is **enabled in Keycloak but disabled in the local
mirror** — the same state an admin disable leaves — so it signs in and is then
refused `403` on `/v1/me`: the suspended-account edge case, reachable with no
manual intervention.
