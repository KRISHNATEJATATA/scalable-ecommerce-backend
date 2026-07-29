# Runbook — Backup & Recovery

Operational recovery for the e-commerce backend. Stateful data lives in **RDS
Postgres** (source of truth) and **S3** (uploads). Valkey holds only ephemeral
state and is disposable.

## Objectives

| Metric | Target |
|---|---|
| **RPO** (max data loss) | **5 minutes** — via RDS Point-in-Time Recovery |
| **RTO** (max downtime)  | **1 hour**  — restore + migrate + shift traffic |

## Backups

- **RDS**: automated backups + PITR enabled (retention ≥ 7 days); transaction
  logs give ~5-min RPO. Manual snapshot before every schema migration.
- **S3**: versioning enabled on the uploads bucket; cross-region replication for
  DR. Objects are immutable once written (store keys, never overwrite).
- **Valkey**: **not** backed up — rate-limit counters, idempotency keys, and
  event-dedup keys are ephemeral and self-heal. Treat as cache. (Revocation is a
  short access-token TTL owned by Keycloak — there is no app-side token denylist.)

## Recovery procedures

### 1. RDS Point-in-Time Recovery (data corruption / bad deploy)

```bash
aws rds restore-db-instance-to-point-in-time \
  --source-db-instance-identifier ecommerce-prod \
  --target-db-instance-identifier ecommerce-restore \
  --restore-time <ISO-8601-timestamp>
```

Repoint the app (`DATABASE_URL`) at the restored instance, or promote it. Verify
`/v1/ready` returns 200 before shifting ALB traffic.

### 2. Alembic rollback (bad migration)

```bash
# Run as a one-off ECS task (sync psycopg driver)
alembic downgrade -1        # or: alembic downgrade <revision>
```

Prefer restoring the pre-migration snapshot if the downgrade is destructive or
not cleanly reversible.

**`d4e5f6a7b8c9` aborted with "already hold multiple committed reservations".**
Not a bug in the migration — it refused to widen the reservation uniqueness guard
because the data already violates it. Those order lines had stock deducted twice
(two `committed` reservations for one `(order_id, sku)`), which no migration can
safely undo. The error lists them. For each: decide refund or restock, then leave
one `committed` row and set the surplus to `released`, handing their units back
(`UPDATE inventory.inventory SET reserved = reserved - <qty> ...` only if the
surplus row was `held`; a `committed` row already left `reserved`, so correct
`on_hand` instead). Re-run `upgrade head`. Nothing is left half-applied — the
migration aborts before any DDL.

### 3. Full DR restore (region loss)

1. Restore RDS from the latest cross-region snapshot / PITR.
2. Confirm the S3 uploads bucket replica is current.
3. `terraform apply` the stack in the DR region (image from ECR replica).
4. Run the Alembic one-off task to `upgrade head` (usually a no-op).
5. Cut DNS/ALB over; verify `/v1/health` and `/v1/ready`.

### 4. DLQ replay (poison messages)

A consumer queue routes a message to its per-subscription DLQ after `maxReceiveCount`
receives. Consumers are idempotent, so replay is safe once the underlying fault is fixed.

```bash
# Move messages from the DLQ back to the source queue (SQS-native redrive)
aws sqs start-message-move-task \
  --source-arn arn:aws:sqs:<region>:<acct>:<consumer>-dlq \
  --destination-arn arn:aws:sqs:<region>:<acct>:<consumer>
```

Watch the CloudWatch alarm on the DLQ's `ApproximateNumberOfMessagesVisible` return to 0.
If a message is genuinely un-processable, inspect the payload, fix the consumer/data, then
redrive — never delete blindly.

### 5. Outbox stuck (relay down / lagging)

Symptom: `outbox lag` metric (age of oldest `published_at IS NULL` row) climbing. The relay is
publish-then-mark, so events are not lost — they ship once the relay recovers. Restart the
`service`-role relay task; if lag persists, scale relay replicas (safe — `FOR UPDATE SKIP
LOCKED` prevents double-claim).

### 6. Image worker (secure upload pipeline)

The `service`-role **image worker** (`python -m src.catalog.adapters.image_worker`) drains the
**`image-uploads`** SQS queue that S3 `ObjectCreated` notifications (scoped to the `uploads/`
prefix) land in. For each object it sniffs the real bytes (`python-magic`), rejects a type that
doesn't match what was claimed at presign, re-encodes to WebP (stripping EXIF), writes
thumbnails under `public/`, and flips `catalog.products.image_status` from `pending` to `ready`
(or `failed`). Processed objects under `public/` are world-readable (CloudFront/OAC in prod);
raw `uploads/` stay private.

**Queues & DLQ.** `image-uploads` has a redrive policy (`maxReceiveCount=5`) → **`image-uploads-dlq`**.
A message that repeatedly raises (e.g. S3 fetch error, worker bug) lands on the DLQ — replay it
with the SQS redrive in **§4** once the fault is fixed (the worker is idempotent + token-guarded,
so replay is safe). A *validation* failure (spoofed/oversized bytes) is **not** a poison message:
it terminally sets `image_status='failed'` and the message is acked normally.

**Stale-event safety.** `mark_image_ready/failed` are conditioned on the product's
`image_upload_token`, so a late event for a **superseded** upload updates zero rows and is
logged as `superseded (stale event)` — it can never overwrite newer image state. When the flip
*does* land, a `ProductUpdated` outbox row is written in the same transaction, so the read-cache
is invalidated (via the relay → `catalog-cache` consumer) and the new `image_url`/status is served.

**Failure recovery.**

| Symptom | Likely cause | Action |
|---|---|---|
| Products stuck `pending` | worker down, or `image-uploads` not draining | check the worker task is running + healthy; inspect queue depth |
| `image_status='failed'` | spoofed/oversized/corrupt upload | expected — the merchant re-presigns + re-uploads a valid image |
| `image-uploads-dlq` non-empty | repeated processing errors | inspect a DLQ message, fix the fault, redrive (§4) |
| `image_url` null on a `ready`-looking image | `public/` not world-readable | re-run `make s3-setup` (ensures the public-read bucket policy) |

**Monitoring.** Alarm on `image-uploads-dlq` `ApproximateNumberOfMessagesVisible > 0`; watch
`image-uploads` queue depth + oldest-message age (worker liveness) and the worker task health
check. To reprocess a specific product, the merchant simply re-presigns — there is no manual
re-enqueue path (the presigned upload is the only trusted entry point).

### 7. Catalog cache worker (read-cache invalidation)

The `service`-role **cache worker** (`python -m src.catalog.adapters.cache_worker`) drains the
**`catalog-cache`** SQS queue subscribed to `ProductUpdated` + `ProductDeleted`. For each event
it **invalidates** the product's Valkey read-cache entry: it deletes both the cached payload
(`product:{id}`) and any in-flight fill lock (`product:lock:{id}`) in one atomic step. Deleting
the lock is what stops a cache fill that began *before* this update from writing its now-stale
read back afterwards — the filler's guarded store no-ops once its lock is gone (a lock we already
hold, so there is no separate expiring generation counter to race). The consumer is idempotent
(dedupe on `event_id`) — re-delivering an event just re-invalidates an already-absent key.

**Event-loss safety (processing lease).** A message is claimed with a **short processing lease**
carrying a unique per-worker token (`CONSUMER_LEASE_TTL_SECONDS`, default 60s — keep it **≤** the
queue's SQS visibility timeout, i.e. set visibility ≥ the lease), and only **on success** is the
lease upgraded — *only if we still own the token* — to a long-lived completion marker
(`CONSUMER_DEDUP_TTL_SECONDS`). If the worker **crashes mid-handle**, the short lease expires and
SQS redelivers the message for reprocessing — the invalidation is never silently dropped. A
handler error releases the lease (only if still ours) immediately so redrive is instant. The
token makes the lease owner-safe: a worker whose lease already expired can never overwrite or
delete a lease a *different* worker has since claimed. `bus_bootstrap` sets the local queue's
visibility timeout from `CONSUMER_LEASE_TTL_SECONDS`; keep the same invariant in Terraform.

**Staleness bound.** Invalidation is eventual: bounded by the outbox relay poll interval + the
queue latency + the entry TTL (`PRODUCT_CACHE_TTL_SECONDS` + jitter) as the ultimate backstop. A
brief read of a just-updated product may serve the prior value until the event drains — this is
the accepted cache-aside trade-off (the write path is never blocked on cache).

| Symptom | Likely cause | Action |
|---|---|---|
| Product reads serve stale data | cache worker down / `catalog-cache` not draining | check the worker task is running + healthy; inspect queue depth; TTL still bounds staleness |
| `catalog-cache-dlq` non-empty | repeated handler errors (Valkey unreachable) | inspect a DLQ message, fix Valkey connectivity, redrive (§4) — invalidation is idempotent, replay is safe |
| Cache never populates | `PRODUCT_CACHE_ENABLED=false` or Valkey down | app degrades to DB-only reads (correct, just slower); restore Valkey |

**Monitoring.** Alarm on `catalog-cache-dlq` `ApproximateNumberOfMessagesVisible > 0`; watch the
`catalog-cache` queue depth + oldest-message age (worker liveness). Losing the worker degrades
performance (more DB reads, staleness bounded by TTL) but is **not** a correctness incident.

### 8. Reservation reaper (stock leaked by a stalled checkout)

The `service`-role **reaper** (`python -m src.inventory.adapters.reaper`, or a scheduled
`--once` task) releases every reservation still `held` past its `expires_at`. It exists because a
hold bumps `inventory.reserved` immediately: if the checkout saga dies before committing or
compensating, those units stay counted as reserved forever — a **phantom oversell-block**, stock
on the shelf that nobody can buy. Each pass claims a batch with `FOR UPDATE SKIP LOCKED`, gives
the stock back, flips the rows to `released` and writes `StockReleased` outbox rows — all in one
transaction, so N replicas are safe and a crash mid-pass simply re-runs.

**Not a data-loss risk.** The reaper only *frees* stock; the failure mode of it being down is
under-selling, not overselling. The one dangerous knob is the TTL: if
`RESERVATION_TTL_SECONDS` drops below the checkout saga's step timeouts, the reaper will reclaim
stock from a slow-but-alive checkout and that checkout fails at payment confirmation.

| Symptom | Likely cause | Action |
|---|---|---|
| `available` stuck at 0 while `on_hand` is healthy | reaper down; expired holds never released | check the reaper task/schedule is running; run one manual sweep (below) |
| Expired-hold backlog (query below) climbing fast | checkouts dying mid-saga upstream | investigate the saga/payment step — the reaper is treating a symptom |
| `inventory_oversell_blocked_total` spiking | genuine contention on a hot SKU, or a saga retry storm | expected under contention; confirm stock levels before assuming a bug |
| Paid orders' stock returns to the pool | `commit_reservation` not called on payment success | fix the saga's confirm step — the reaper is doing its job |

```bash
# Manual one-shot sweep (same image, service role)
aws ecs run-task --cluster ecommerce --task-definition ecommerce-reaper \
  --overrides '{"containerOverrides":[{"name":"app","command":["python","-m","src.inventory.adapters.reaper","--once"]}]}'
```

```sql
-- What is currently held, and how much is past its TTL?
SELECT status, count(*), sum(qty) FROM inventory.reservations GROUP BY status;
-- ↓ THE reaper alert: this is the backlog. Healthy = hovers near zero.
SELECT count(*) FROM inventory.reservations WHERE status = 'held' AND expires_at <= now();
```

**Monitoring.** `inventory_oversell_blocked_total` (rejection rate) and
`inventory_reservation_conflict_total` (order lines retried with a changed quantity — a
caller bug, deliberately kept out of the oversell counter) are on the API's `/metrics`.
The reaper runs in its own process, so its `inventory_reaper_released_total` is exported
separately — a scrape port (`WORKER_METRICS_PORT`) when it loops, a Pushgateway
(`METRICS_PUSHGATEWAY_URL`) when it runs `--once`.

That counter measures volume, **not liveness**: a reaper that never starts increments
nothing and pushes nothing, so a flat line can't be told apart from a healthy idle one.
Reaper liveness is alerted on the expired-hold backlog above — a sustained non-zero count
means it is down or falling behind. Both are shipped as rules, not just as a query to run
by hand: `ops/prometheus/inventory-reaper-alerts.yaml` (`InventoryReaperBacklog`,
`InventoryReaperDown`, `InventoryReaperNotRunning`), fed by the postgres_exporter query in
`ops/prometheus/postgres-exporter-queries.yaml` — which scrapes Postgres, so it keeps
reporting when the reaper is dead.

## Post-incident
- Re-enable automated backups on the promoted instance.
- Rotate any exposed secrets (JWT keys, DB creds) via Secrets Manager.
- Note the incident in `.github/memory.md` (Recent zone) if it yields a lesson.
