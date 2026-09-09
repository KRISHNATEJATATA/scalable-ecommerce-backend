# Bug Audit — tracking table

Repo-wide audit ran 2026-09-09 (orchestrator + 3 parallel module auditors, then a
post-fix final audit). Every confirmed bug went: **ticket on `main` → branch →
fixing subagent → independent verification subagent → ticket updated**. Fix
branches are NOT merged to `main` (main carries tickets + docs only).

| Ticket  | Severity | Bug | Branch | Fix | Verification | Status |
| ------- | -------- | --- | ------ | --- | ------------ | ------ |
| BUG-001 | Medium   | `SEED_DEMO_DATA` fails open (default `True` inverts the documented opt-in gate) | `bug/BUG-001-seed-demo-data-fails-open` | 94e0b6f | PASS | Fixed |
| BUG-002 | Low      | ReservationReaper idle sleep ignores `stop`; failure log claims nonexistent "backoff" | `bug/BUG-002-reaper-stop-aware-sleep-and-log` | 3cdaccd | PASS | Fixed |
| BUG-003 | Medium   | SqsConsumer drains batches serially — batch outruns SQS visibility; false DLQ risk | `bug/BUG-003-sqs-consumer-serial-batch-visibility` | 59fc460 + fd6b6a2 | PASS (dead validator flagged → removed → re-verified PASS) | Fixed |
| BUG-004 | Medium   | Admin `sub` path params unvalidated → Keycloak Admin-API URL traversal/query injection | `bug/BUG-004-admin-sub-path-traversal` | 328ca03 | PASS | Fixed |
| BUG-005 | Medium   | Caller-fixable Keycloak 400s surface as HTTP 500 on admin user creation | `bug/BUG-005-keycloak-400-as-500` | e3c8ef3 | PASS | Fixed |
| BUG-006 | Low      | SagaRecovery idle sleep ignores `stop` + false "backoff" log (same class as BUG-002, found during BUG-002 verification) | `bug/BUG-006-saga-recovery-stop-aware-sleep-and-log` | aabefa8 | PASS | Fixed |
| BUG-007 | High*    | Half-open breaker wedges on a non-transient probe fault (payment path 503 until restart; latent with stub gateway) | `bug/BUG-007-breaker-half-open-wedge` | 90f1b03 | PASS | Fixed |
| BUG-008 | Medium   | `InventoryReaperDown` alert references nonexistent job → permanent false alarm | `bug/BUG-008-reaper-alert-job-label-mismatch` | e95970f | PASS | Fixed |
| BUG-009 | Low      | `catalog_seed --reset` deletes Keycloak users before wiping products → duplicate-set divergence after mid-reset crash | `bug/BUG-009-seed-reset-ordering-window` | f05f950 | PASS | Fixed |

\* High if triggered; trigger currently latent (stub gateway never raises).

Found during the cycle and fixed inside the parent ticket's branch (no
separate ticket): the dead `_require_consumer_lease_ttl_positive` validator
introduced by the BUG-003 fix (removed in fd6b6a2 after verification flagged
it, re-verified PASS).

Potential issues (not bugs, deliberately not fixed): see
[POTENTIAL.md](POTENTIAL.md). Per-bug detail: the BUG-*.md files in this
directory.
