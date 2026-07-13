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
- **Valkey**: **not** backed up — rate-limit counters, idempotency keys, and the
  `jti` denylist are ephemeral and self-heal. Treat as cache.

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

### 3. Full DR restore (region loss)

1. Restore RDS from the latest cross-region snapshot / PITR.
2. Confirm the S3 uploads bucket replica is current.
3. `terraform apply` the stack in the DR region (image from ECR replica).
4. Run the Alembic one-off task to `upgrade head` (usually a no-op).
5. Cut DNS/ALB over; verify `/v1/health` and `/v1/ready`.

## Post-incident

- Re-enable automated backups on the promoted instance.
- Rotate any exposed secrets (JWT keys, DB creds) via Secrets Manager.
- Note the incident in `.github/memory.md` (Recent zone) if it yields a lesson.
