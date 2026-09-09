# BUG-002: ReservationReaper idle sleep ignores `stop`; failure log claims a backoff that does not exist

## Severity
Low

## Status
Fixed

## Fix Branch
bug/BUG-002-reaper-stop-aware-sleep-and-log (commit 3cdaccd — NOT merged to main)

## Fix
`ReservationReaper.run` idle sleep made stop-aware (mirrors
`PaymentReconciler.run` exactly: `wait_for(stop.wait(), timeout=...)` under
`contextlib.suppress(TimeoutError)`; `stop=None` keeps the plain sleep).
Failure log wording corrected to "retrying after interval". New
`tests/unit/test_reaper_worker.py` (2 regression tests).

## Summary
Two defects in `ReservationReaper.run` (`src/inventory/adapters/reaper.py`):
the idle `asyncio.sleep(poll_interval)` is not stop-aware, and the failure-path
log line says "retrying after backoff" while the loop retries at a fixed
interval with no backoff.

## Expected Behavior
The codebase's own convention (shared `polling._sleep_unless_stopped`, and
`PaymentReconciler.run`'s `asyncio.wait_for(stop.wait(), timeout=...)` with an
explicit docstring promise "SIGTERM never waits out a full interval"): a stop
signal ends the loop promptly. Log lines must describe what actually happens.

## Actual Behavior
Reproduction (2026-09-09): with `poll_interval=1.0s`, `stop.set()` at t+0.1s,
`run()` returned at t+1.01s — stop was ignored for 0.91s (the remainder of the
sleep). On the failure path the log emits `"reaper pass failed; retrying after
backoff"` (reaper.py:64) although the next attempt is exactly `poll_interval`
later.

## Reproduction
Stop-awareness: build `ReservationReaper` with a no-op `sweep_once`, run
`run(1.0, stop)`, set `stop` 0.1s in, assert the call returns within ~0.3s.
Pre-fix it returns ~1.0s. Log line: run one failing sweep and capture
`logging.getLogger("src.inventory.adapters.reaper")` output.

## Root Cause
`reaper.py:67` — `await asyncio.sleep(poll_interval)` instead of a
stop-aware wait; `reaper.py:64` — inaccurate log wording.

## Affected Area
`src/inventory/adapters/reaper.py` (`run`).

## Impact
SIGTERM shutdown latency up to one full poll interval (default 10s) in a worker
that must honor the ECS stop timeout; if a sweep plus the un-interruptible
sleep exceeds the platform stop timeout the task is SIGKILLed mid-`engine.dispose()`.
No work loss (each sweep is one transaction). The false "backoff" line misleads
operators during DB-outage incident response.

## Proposed Fix
Mirror the reconciler: `await asyncio.wait_for(stop.wait(), timeout=poll_interval)`
wrapped in `contextlib.suppress(TimeoutError)` (stop-None case keeps plain
`asyncio.sleep`), and correct the log line to "retrying after interval" (or add
real backoff — smallest correct change is the wording).

## Regression Test
Unit test with a fake `sweep_once` and a short poll interval: setting `stop`
mid-sleep returns from `run()` in ≪ interval; failing sweep logs a line without
the word "backoff".

## Verification
PASS (independent verification agent, 2026-09-09). Verifier reproduced the bug
against `3cdaccd~1` code in isolation (0.902s stop-latency pre-fix) and the
fixed branch (0.000s), confirmed the diff mirrors the reconciler exactly and
contains no unrelated changes.

## Tests
`pytest tests/unit/test_reaper_worker.py -q` → 2 passed.
`pytest tests/unit -q` (full) → **500 passed, 1 skipped**.

## Validation
`ruff check src tests` → pass. `ruff format --check` → clean (235 files).
Commit `3cdaccd` stat = exactly reaper.py + new test file; pre-existing
unrelated dirty files untouched. Note: `src/shared/saga_recovery.py` has the
same defect class — filed separately as BUG-006.
