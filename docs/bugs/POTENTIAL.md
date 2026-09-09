# Potential issues — documented, not fixed (deliberately)

Findings from the audit that are **not demonstrably incorrect** today
(design trade-offs, latent traps, or requiring a product decision). None
has a fix branch. In ticket order of discovery.

1. **Checkout replay clears the user's *current* cart** —
   `src/orders/application/checkout_saga.py:147-148, 227`. Every replay of a
   stored `201` clears the basket. A stale-key retry after the user rebuilt a
   new cart silently empties it. The clear-on-replay is *documented* behavior
   ("the next replay clears it"), so classified arguable-by-design — but it is
   a real data-loss edge worth a product decision.
2. **Saga: cancelled-while-payment-completing promises reconciliation that no
   code performs** — `checkout_saga.py:358-365` logs "reconciliation required
   (paid payment row attached)"; the payment reconciler only touches
   `pending` payments, nothing scans cancelled orders with succeeded payments.
   Needs a design decision (build the reverse-scan vs change the message).
   Window is ms-scale per recovery sweep but retry-after-blip coincidences are
   routine at scale. (Final-audit finding #4.)
3. **RedactFilter misses secrets passed via logging `extra=`** (reproduced by
   the shared-infra audit; keys ride `extra=` straight to ECS JSON). No
   current `extra=` caller in `src/` — latent trap for the first caller or a
   dependency's logger.
4. **RedactFilter is all-or-nothing** — the word "email" in a benign message
   destroys the whole record/traceback (log-observability loss, trade-off
   documented as "minimal boundary redaction").
5. **Admin directory: exactly-full final page emits `next_cursor` for an empty
   page** — self-terminating; standard offset-pagination behavior; existing
   test codifies the empty trailing page as terminal.
6. **Admin directory cursor replays a raw offset** — a changed `limit`/`search`
   between pages re-partitions (repeat/skip). Conscious ponytail trade-off,
   documented at `admin_client.py`.
7. **Whitespace-only product names pass validation** (`min_length=1`). Low.
8. **`reject_unknown_query_params` reports only the first unknown param** per
   request. UX completeness, not correctness.
9. **MetricsMiddleware position comment inverted** (`src/app.py:143-145` says
   "innermost"; empirically it is the outermost user middleware — which is
   what the intent requires). Doc-only.
10. **`trusted_proxies` default omits 192.168.0.0/16** despite the "loopback +
    private ranges" comment. Cosmetic.
11. **`run_recovery` uses `reservation_reaper_poll_interval_seconds` as the
    saga-recovery poll interval** (saga_recovery.py:190) — a shared-setting
    reuse present before this audit; harmless unless an operator sizes them
    independently.
12. **`KeycloakInvalidRequestError` detail text is email-specific** ("check the
    email address") while `_translate` is shared by all admin ops. Cosmetic
    (BUG-005 fix).
13. **Test-infra gap**: `test_image_processing.py` is skipped on this host
    (`libmagic` not installed) — pre-existing; noted in every suite run.

Also noted by the final audit and accepted as-is: idempotency fast-path
records can serve stale `201` shapes for up to their 24h TTL after a schema-
evolving deploy (partially acknowledged in code comments; eviction/self-heal
mitigates).
