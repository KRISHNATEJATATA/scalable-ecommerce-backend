# BUG-003: SqsConsumer drains batches serially — batch duration outruns the SQS visibility timeout and burns redrive attempts

## Severity
Medium

## Status
Fixed

## Fix Branch
bug/BUG-003-sqs-consumer-serial-batch-visibility (commits 59fc460 + fd6b6a2 — NOT merged to main)

## Fix
`poll_once` processes the received batch concurrently: one per-message
coroutine (processing + delete) with an individual error boundary per message
— one poison message or a failing `delete_message` can no longer disturb or
abort its siblings; `handled` counts only successful deletes; original log
messages preserved; CancelledError still propagates (no
`return_exceptions=True`). Batch duration now ≈ max(handler), not sum.
Sizing contract documented on `consumer_max_messages` / `consumer_lease_ttl_seconds`
(setting.py comments + `.env.example`: visibility ≥ lease + max_messages ×
handler budget; bus_bootstrap's lease×2 visibility satisfies it). 3 regression
tests added to `tests/unit/bus/test_consumer.py` (concurrency timing+overlap,
poison isolation, delete-failure isolation).

## Summary
`SqsConsumer.poll_once` receives up to `consumer_max_messages` (default 10) and
processes them **serially**. SQS starts the visibility clock for *all* messages
at receive time, so a batch whose handlers average more than
`visibility_timeout / batch_size` is still in flight when the tail messages
become visible again — they are redelivered mid-batch, each redelivery counts
against `maxReceiveCount`, and messages whose handlers *succeed* can land in
the DLQ.

## Expected Behavior
The window the code itself documents for the image worker
(`src/catalog/adapters/image_worker.py` — "messages would reappear mid-processing
and burn redrive attempts until they hit the DLQ despite succeeding… One message
per receive keeps the in-flight work inside that ceiling"): in-flight batch work
must stay inside the visibility ceiling, via batch-size 1, per-message visibility
extension, or a bounded batch-duration contract enforced in settings.

## Actual Behavior
Reproduction (2026-09-09): a batch of 10 messages with 50ms handlers completes in
**0.615s ≈ sum(handlers) (0.50s)**, not max(handler) (0.05s) — strictly serial.
`receive_message` is called with no `VisibilityTimeout` parameter and the
consumer never calls `change_message_visibility`; the lease
(`consumer_lease_ttl_seconds`) is claimed only when a message's turn comes, so it
protects correctness (no double-processing) but not the redrive budget. The cart
consumer's handler fans out to *N* carts sequentially per event
(`ValkeyCartRepository.refresh_product`), making slow-handler batches realistic.

## Reproduction
Fake SQS + Valkey driving `SqsConsumer.poll_once` with 10 × 50ms handlers;
assert the call completes in ≈ max(handler). Pre-fix: ≈ sum(handlers).
(Full false-DLQ demonstration: handler ≈ visibility × (maxReceiveCount − 1) —
every lease expires mid-handle, the completion CAS fails, the message bounces
until DLQ despite handler success.)

## Root Cause
`src/shared/bus/consumer.py:197-222` (`poll_once` serial for-loop) with no
visibility management; `src/shared/config/setting.py` enforces no relationship
between `consumer_max_messages`, handler budget, and visibility (unlike
`image_upload_reaper_grace_seconds > image_visibility_timeout_seconds`, which
*is* enforced).

## Affected Area
`src/shared/bus/consumer.py`; consumers wiring it (`cart_consumer`,
`cache_worker`); `src/shared/config/setting.py` (sizing contract).

## Impact
Under slow handlers + multiple replicas: false DLQ deliveries of healthy
messages, alert noise, duplicated redrive processing (safe only because
handlers are idempotent). Secondary: a transient `delete_message` error
abandons the rest of the batch mid-loop while their visibility clocks run.

## Proposed Fix
Smallest correct change: process the received batch concurrently
(`asyncio.gather` over `_process`, since the per-event Valkey lease already
guarantees single processing) **and/or** receive with
`VisibilityTimeout=consumer_lease_ttl_seconds` so the receive window and the
lease share one documented bound; enforce the bound with a settings validator
mirroring the image-visibility one.

## Regression Test
Unit test as in the reproduction (concurrent completion ≈ max(handler), not
sum); settings validator test; a test that a mid-batch `delete_message` failure
does not stop processing/deleting the remaining messages.

## Verification
PASS (independent verification agent, 2026-09-09). Verifier's own repro:
pre-fix 0.628s ≈ sum(handlers); post-fix 0.051–0.066s ≈ max(handler) across
3 runs; handler-ran-exactly-once and all-deleted confirmed; duplicate-in-batch
lease race exercised end-to-end (loser left for redrive, redelivery
dedupe-acked). Verifier confirmed the two bug-reproducing tests fail on
pre-fix code; also flagged (and a follow-up commit fd6b6a2 removed) a dead
validator the fixer had added — re-verified PASS.

## Tests
`pytest tests/unit/bus/test_consumer.py -q` → 11 passed.
`pytest tests/unit -q` (full) → **501 passed, 1 skipped**; timing-sensitive
tests run 3× with zero flakiness.

## Validation
`ruff check src tests` → pass; `ruff format --check` → clean; basedpyright →
no new diagnostics (pre-existing baseline only). Note on scope: the
"settings validator" half of the proposed fix landed as documentation +
the field's existing `gt=0` bound (an added validator was dead code and was
removed after verification flagged it) — the enforceable part of the contract
is handler-time-dependent and cannot be config-expressed; documented per the
ticket's "and/or" alternative.
