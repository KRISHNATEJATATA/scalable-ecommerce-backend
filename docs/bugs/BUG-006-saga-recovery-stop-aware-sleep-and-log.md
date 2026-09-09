# BUG-006: SagaRecovery idle sleep ignores `stop`; failure log claims a backoff that does not exist

## Severity
Low

## Status
Fixed

## Fix Branch
bug/BUG-006-saga-recovery-stop-aware-sleep-and-log (commit aabefa8 — NOT merged to main)

## Fix
`SagaRecovery.run` idle sleep made stop-aware (mirrors the reconciler and the
BUG-002 reaper fix exactly: `wait_for(stop.wait(), timeout=...)` under
`contextlib.suppress(TimeoutError)`; `stop=None` keeps the plain sleep).
Failure log wording corrected to "retrying after interval". New
`tests/unit/test_saga_recovery_worker.py` (2 regression tests).

## Summary
Same defect class as BUG-002 (fixed on its own branch), discovered during
BUG-002's independent verification: `SagaRecovery.run`
(`src/shared/saga_recovery.py`, `run` at ~lines 157-166) sleeps a plain
`asyncio.sleep(poll_interval)` that ignores `stop`, and its failure log says
"retrying after backoff" while the loop retries at a fixed interval.

## Expected Behavior
Stop-aware idle sleep (the reconciler's `wait_for(stop.wait(), timeout=...)`
pattern) and a log line that describes reality.

## Actual Behavior
Reproduction (2026-09-09): with `poll_interval=1.0s` and a no-op `sweep_once`,
`stop.set()` at t+0.1s — `run()` returned **0.901s** after stop (the remainder
of the sleep). Failure path logs
`"saga recovery pass failed; retrying after backoff"` (saga_recovery.py:163)
though no backoff exists.

## Reproduction
Instantiate `SagaRecovery(None, None, None)`, instance-shadow `sweep_once` to
return zeros, run `run(1.0, stop)`, set stop after 0.1s, measure time from
`stop.set()` to `run()` completion. Pre-fix: ≈ interval − 0.1s.

## Root Cause
`saga_recovery.py:159-166` — plain `asyncio.sleep` + inaccurate wording; the
reaper (BUG-002) had the identical defect and the reconciler models the correct
pattern.

## Affected Area
`src/shared/saga_recovery.py` (`SagaRecovery.run`).

## Impact
SIGTERM shutdown latency up to one poll interval in the saga-recovery worker;
misleading operator log line during DB outages. No work loss (each sweep is a
transaction).

## Proposed Fix
Mirror the reconciler/BUG-002 fix: stop-aware wait under
`contextlib.suppress(TimeoutError)` when `stop` is not None (plain sleep
otherwise), and wording "retrying after interval". Regression tests mirroring
`tests/unit/test_reaper_worker.py`.

## Regression Test
Same shape as BUG-002's: stop set mid-sleep returns from `run()` in ≪ interval;
failing sweep logs no "backoff".

## Verification
PASS (independent verification agent, 2026-09-09). Verifier reproduced the bug
against `aabefa8~1` code with its own harness (0.900s stop-latency pre-fix;
0.000s post-fix), confirmed the run loop matches both sibling implementations
and that sweep/settle logic is untouched, and proved both regression tests
fail on pre-fix code.

## Tests
`pytest tests/unit/test_saga_recovery_worker.py -q` → 2 passed.
`pytest tests/unit -q` (full, Testcontainers) → **500 passed, 1 skipped**
(main 498 + 2 new).

## Validation
`ruff check src tests` → pass; `ruff format --check` → clean (235 files);
basedpyright on changed files → 0 diagnostics. Commit `aabefa8` stat = exactly
saga_recovery.py + the new test file; pre-existing unrelated dirty files not
committed.
