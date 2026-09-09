# HANDOFF — repository bug audit & fix cycle (2026-09-09)

## 1. Executive summary

- **Audited:** the entire repository — all 6 modules (catalog, inventory,
  orders, payments, identity, cart), the event bus + 6 background workers,
  shared infra (config, auth/JWKS, errors, pagination, cache, idempotency,
  middleware, app wiring), scripts, migrations, ops configs, and tests — via
  one orchestrator direct pass + 3 parallel audit agents, plus a **final
  post-fix audit** over the whole tree again.
- **Confirmed bugs:** 9 (BUG-001..009).
- **Fixed & independently verified:** 9 of 9 — every fix by a dedicated
  subagent, every verification by a *different* subagent, all **PASS** on the
  first or second verification round (BUG-003 needed one follow-up commit).
- **Unresolved/unfixed bugs:** 0 confirmed; 13 potential issues documented in
  [docs/bugs/POTENTIAL.md](bugs/POTENTIAL.md) (design trade-offs/latent
  traps, deliberately not fixed).
- **Blocked:** none. All suites ran for real (Testcontainers-Postgres; one
  pre-existing environmental skip: `libmagic`-gated image-processing tests).
- **Overall status:** `main` is healthy and carries only ticket/docs commits;
  all 9 fixes live on dedicated branches, **not merged** (per instructions).
  Baseline suite: 498 passed, 1 skipped → final branches carry 499–501
  depending on their own added tests.

## 2. Repository state

- **Current branch:** `main` @ `59d584c` (+ the BUG-009 ticket commit on top →
  final `main` head is the BUG-009 "Fixed" ticket commit).
- **`main` contains:** 9 ticket files under `docs/bugs/` (each opened while
  `Open`, later updated to `Fixed` with fix/verification/test/validation
  sections), `docs/bugs/README.md` (tracking table), `docs/bugs/POTENTIAL.md`,
  and this `HANDOFF.md`. **No implementation fixes are on `main`.**
- **Pre-existing working-tree changes (NOT ours, untouched):**
  `M .gitignore`, `M pyrightconfig.json`, untracked `docs/frontend-handoff.md`,
  `software-engineer-interview-preparation.md`. Every fix/verify agent was
  instructed to never stage these; verified clean after each commit.
- **Environment/setup:** Python 3.13 venv at `.venv/`; Docker Desktop required
  (Testcontainers-Postgres) — at least one verifier had to start it. Commands
  via `Makefile` (`make test`, `make lint`, `make typecheck`).

## 3. Complete bug inventory

Full per-bug detail (repro, root cause, fix, verification, tests, validation)
is in each ticket — authoritative and complete:
[docs/bugs/BUG-001](bugs/BUG-001-seed-demo-data-fails-open.md) ·
[BUG-002](bugs/BUG-002-reaper-stop-aware-sleep-and-log.md) ·
[BUG-003](bugs/BUG-003-sqs-consumer-serial-batch-visibility.md) ·
[BUG-004](bugs/BUG-004-admin-sub-path-traversal.md) ·
[BUG-005](bugs/BUG-005-keycloak-400-as-500.md) ·
[BUG-006](bugs/BUG-006-saga-recovery-stop-aware-sleep-and-log.md) ·
[BUG-007](bugs/BUG-007-breaker-half-open-wedge.md) ·
[BUG-008](bugs/BUG-008-reaper-alert-job-label-mismatch.md) ·
[BUG-009](bugs/BUG-009-seed-reset-ordering-window.md).
One-paragraph digest:

| Ticket | What was broken → what the fix did (branch commit) |
|---|---|
| BUG-001 | `seed_demo_data` defaulted `True` (fail-open) → default `False`, docs synced, 2 tests (`94e0b6f`) |
| BUG-002 | Reaper ignored `stop` in idle sleep + false "backoff" log → stop-aware `wait_for` mirroring reconciler, wording fixed, 2 tests (`3cdaccd`) |
| BUG-003 | Serial SQS batch outran visibility (10×50ms took 0.615s) → concurrent per-message `_process_and_ack` with per-message error boundaries incl. delete failures; sizing contract documented; 3 tests (`59fc460`, +`fd6b6a2` dead-validator removal) |
| BUG-004 | Unvalidated `{sub}` path params → anchored-UUID `SubPath` pattern on 3 admin routes; test asserts zero adapter calls for evil payloads (`328ca03`) |
| BUG-005 | Keycloak 400 → raw 500 on admin create → email regex + 255 bound at schema, `KeycloakInvalidRequestError` 400 arm in `_translate`, OpenAPI synced, 3 tests (`e3c8ef3`) |
| BUG-006 | Same stop/log defects in `SagaRecovery.run` → mirrored BUG-002 fix, 2 tests (`aabefa8`) |
| BUG-007 | Half-open breaker wedge on non-transient probe fault → `record_success()` before re-raise (mirrors admin `_guarded`), 1 test (`90f1b03`) |
| BUG-008 | Alert selected nonexistent `job="reservation-reaper"` → selectors `{job="workers", instance=~"reaper:9100"}`, config-guard test (`e95970f`) |
| BUG-009 | `--reset` deleted Keycloak users pre-wipe → reordered wipe-first (order-only diff), 2 order-assertion tests (`f05f950`) |

Every verification included an **independent reproduction** and, for 7 of 9
bugs, execution of the new tests against the **pre-fix code** to prove they
fail there. All verifiers re-ran the full suite themselves.

## 4. Unresolved / blocked work

- **None blocked.** Nothing failed verification in the final state.
- **Potential issues:** 13 documented in [docs/bugs/POTENTIAL.md](bugs/POTENTIAL.md) —
  the two most decision-worthy: (1) checkout *replay* clears a rebuilt cart
  (silent data-loss edge), (2) the saga's "cancelled while payment was
  completing" path logs "reconciliation required" but no code reconciles
  cancelled-orders-with-succeeded-payments.
- **Areas needing manual verification:** none for correctness (all verified by
  execution); `make seed`/`--reset` were verified at logic level + order tests,
  not against a live Keycloak. Spectral/promtool not installed on this host —
  OpenAPI/prom configs were parse-validated only.
- **Missing coverage (pre-existing):** worker run-loops now covered for
  reaper/saga-recovery; `run_recovery`'s setting-reuse (POTENTIAL #11)
  untested; no HTTP-level regression test pins BUG-005's 400-handler path
  (verified manually by the verifier).

## 5. Validation summary (commands actually executed, final branch of each)

```
pytest tests/unit -q                      main baseline 498 passed, 1 skipped
pytest tests/unit -q                      each fix branch: 499–501 passed, 1 skipped (all own tests green)
ruff check src tests (+scripts where relevant)   PASS on every branch
ruff format --check ...                   PASS on every branch
basedpyright src (changed files per branch)      zero NEW diagnostics (pre-existing baseline only)
py_compile scripts/catalog_seed.py        OK
yaml.safe_load ops/prometheus/*.yaml      OK (promtool/Spectral not installed — parse-only)
Independent per-bug repros                9/9 reproduced pre-fix, gone post-fix (verbatim outputs in ticket Verification sections)
```

## 6. Branch map

| Branch | Purpose | Head | Ready to merge? |
|---|---|---|---|
| `bug/BUG-001-seed-demo-data-fails-open` | BUG-001 fix + tests | `94e0b6f` | Yes — reviewed PASS |
| `bug/BUG-002-reaper-stop-aware-sleep-and-log` | BUG-002 fix + tests | `3cdaccd` | Yes |
| `bug/BUG-003-sqs-consumer-serial-batch-visibility` | BUG-003 fix + tests | `fd6b6a2` (2 commits) | Yes |
| `bug/BUG-004-admin-sub-path-traversal` | BUG-004 fix + test | `328ca03` | Yes |
| `bug/BUG-005-keycloak-400-as-500` | BUG-005 fix + tests (+OpenAPI) | `e3c8ef3` | Yes |
| `bug/BUG-006-saga-recovery-stop-aware-sleep-and-log` | BUG-006 fix + tests | `aabefa8` | Yes |
| `bug/BUG-007-breaker-half-open-wedge` | BUG-007 fix + test | `90f1b03` | Yes |
| `bug/BUG-008-reaper-alert-job-label-mismatch` | BUG-008 fix + guard test | `e95970f` | Yes |
| `bug/BUG-009-seed-reset-ordering-window` | BUG-009 fix + tests | `f05f950` | Yes |

None merged. Old long-lived branches predating this audit
(`add_adapters`, `add_application_layer/DI`, `scaffolding`) were not touched.

## 7. Recommended next steps

1. **Review & merge** the 9 branches (all independently verified; recommend
   merge order 001→009, though they are independent — each was cut from
   successive `main` ticket commits, so rebase/merge is mechanical).
2. After merging, run the full suite once on the merged `main`
   (expect ≈ 498 + 12 new tests = 510 passed, 1 skipped) and
   `make lint`/`make typecheck`.
3. Decide the two open design questions in `docs/bugs/POTENTIAL.md` (#1 replay
   clears current cart; #2 promised-but-missing reconciliation) — either
   ticket them or adjust the docs/log message to match reality.
4. Consider the cheap hardening items in POTENTIAL (#3 `extra=` redaction,
   #7 whitespace names, #10 trusted-proxies comment).
5. If a real payment provider replaces the stub, revisit BUG-007's test and
   POTENTIAL #12's exception wording.
6. Re-add `libmagic` to the host (or CI image) to unskip the 1 skipped test.

## 8. Handoff checklist

- [ ] `git status` — expect only the 4 pre-existing unrelated entries
      (`M .gitignore`, `M pyrightconfig.json`, 2 untracked docs) and no
      others.
- [ ] `git branch` — 9 `bug/BUG-*` branches present, none merged
      (`git log main` shows only `docs(bugs)` commits past `66ef008`).
- [ ] `pytest tests/unit -q` on `main` → 498 passed, 1 skipped.
- [ ] Spot-read `docs/bugs/README.md` (tracking table) vs `git branch` —
      table matches reality.
- [ ] `make lint` green on `main`.
- [ ] Docker engine up before running the suite (Testcontainers).
- [ ] After merging branches: re-run the checklist against the merged `main`.
