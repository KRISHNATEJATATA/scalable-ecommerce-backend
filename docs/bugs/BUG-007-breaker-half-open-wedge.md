# BUG-007: half-open circuit breaker wedges permanently when the probe fails with a non-transient error

## Severity
High (if triggered; latent with the current stub gateway)

## Status
Fixed

## Fix Branch
bug/BUG-007-breaker-half-open-wedge (commit 90f1b03 — NOT merged to main)

## Fix
One line in `_resilient`'s non-transient arm: `record_success()` before the
bare re-raise (mirrors the Keycloak adapter's `_guarded` bracket: a definitive
4xx proves the dependency is up). `BaseException` → `record_abandoned` arm
pre-existed. 1 regression test
(`test_gateway_non_transient_probe_fault_releases_half_open_slot`).

## Summary
`ResilientPaymentGateway._resilient` (src/payments/adapters/resilient_gateway.py:104-108) has a
non-transient `except Exception` arm that does a bare `raise` with **no breaker
call**. If that call was the half-open probe, the breaker's `_half_open_calls`
slot is never released → every future `allow()` returns False → the whole
payment path answers `CircuitOpenError` (503) **until process restart**.

## Expected Behavior
The code's own contract (src/shared/resilience.py:21-23, and
`record_abandoned`'s docstring: "the half-open probe slot **must** be released
or the breaker wedges") and the sibling bracket in
src/identity/adapters/keycloak/admin_client.py:145-148, which records
`record_success()` before the bare re-raise of a non-transient error (a 4xx
answer proves the dependency is up).

## Actual Behavior
Reproduced 2026-09-09 by the final-audit agent against the real module: trip
the breaker with 5 transient faults → force the reset window to elapse → the
half-open probe raises any **non-transient** exception (a programming error
inside the operation, or a future real provider adapter's 4xx) → breaker stuck
half-open; afterwards every healthy call fails fast with CircuitOpenError.

## Reproduction
As above (breaker.threshold=5 transient failures; wait reset_timeout; probe
raises ValueError; assert a subsequent healthy call still raises
CircuitOpenError — pre-fix it does, post-fix it succeeds).

## Root Cause
`_resilient`'s non-transient arm omits `self._breaker.record_success()` before
the bare `raise` (contrast: the transient arm records failure, the Keycloak
bracket records success).

## Affected Area
`src/payments/adapters/resilient_gateway.py` (`_resilient`).

## Impact
A single non-transient fault during a half-open probe permanently 503s the
payment path until restart (checkout down). Latent today because the stub
gateway never raises; becomes live the moment a real provider adapter (or any
raised error inside the wrapped call) exists.

## Proposed Fix
One line: in the non-transient arm, call `self._breaker.record_success()`
before re-raising — exactly mirroring admin_client.py's bracket (a definitive
4xx answer proves the dependency is up; only transient faults count failures).

## Regression Test
Unit test in the resilient-gateway test file (find via grep
`ResilientPaymentGateway` in tests/): trip the breaker with transient failures,
advance past the reset window (patch `time.monotonic`), run a call whose
operation raises a non-transient exception, then run a healthy call — post-fix
it succeeds (pre-fix it raises CircuitOpenError). Also assert the breaker state
returns to `closed`.

## Verification
PASS (independent verification agent, 2026-09-09). Verifier's own harness
against the real gateway: post-fix the probe's ValueError releases the slot
(state → closed, healthy call succeeds); pre-fix (extracted `90f1b03~1`
module) the wedge reproduced — healthy call raised CircuitOpenError.
Bracket confirmed structurally identical to the admin adapter's `_guarded`.

## Tests
`pytest tests/unit/test_resilience.py tests/unit/test_payments.py -q` → 57 passed.
`pytest tests/unit -q` (full) → **499 passed, 1 skipped** (main 498 + 1 new).

## Validation
`ruff check src tests` → pass; `ruff format --check` → clean; basedpyright →
3 errors, all pre-existing baseline (identical set pre/post; zero new).
Commit `90f1b03` stat = exactly the gateway + its test file; pre-existing
dirty files untouched.
