# BUG-009: `catalog_seed --reset` deletes Keycloak users before wiping products — a mid-reset crash leaves state a plain re-seed cannot converge

## Severity
Low (dev-only tool)

## Status
Fixed

## Fix Branch
bug/BUG-009-seed-reset-ordering-window (commit f05f950 — NOT merged to main)

## Fix
Reset path reordered: `wipe_products` runs before the Keycloak user-deletion
loop (pure resequencing — the delete loop is byte-identical, docstring/argparse
help synced). New `tests/unit/test_catalog_seed_reset.py`: 2 tests asserting
the recorded global call order (wipe < deletes < creates; non-reset path
untouched). Convergence traced: any crash post-fix leaves zero live products,
so a plain re-seed recreates the catalog keyed off this-run's anchors — no
duplicate set possible.

## Summary
`scripts/catalog_seed.py:421-429` (`--reset`) deletes the Keycloak demo users
FIRST, then wipes products. If the process dies between the two steps, the
local `identity.users` anchors and every live product remain bound to the old
(now unrecoverable) Keycloak subs. A subsequent plain `make seed` recreates the
users with fresh subs → `get_or_create` mints new anchor rows →
`ensure_product` (keyed on the new `merchant_id` + name) finds nothing →
creates a DUPLICATE 11-product set alongside the still-live old one. Only
another `--reset` repairs the state.

## Expected Behavior
Reset should be ordered so any interruption leaves state a plain re-seed can
converge on: wipe the DB-side products/stock/images first, then delete the
Keycloak users (orphaned keycloak accounts without local products are harmless
— the next seed re-creates users and products keyed off the fresh subs).

## Actual Behavior
Reset order is users-then-products (traced 2026-09-09 by the final audit:
ensure_product → CatalogService.list_products filter by merchant_id+name;
IdentityRepository.get_or_create keyed by oidc_sub).

## Reproduction
Kill the seeder between the Keycloak-delete step and the product wipe (e.g.
SIGKILL), then run `make seed` → duplicate product set; `SELECT count(*) FROM
catalog.products` doubles.

## Root Cause
Step ordering in the reset path.

## Affected Area
`scripts/catalog_seed.py` (reset path only; normal seed path is idempotent and
fine).

## Impact
Dev/demo environments: a failed reset silently diverges (duplicate catalog) on
the next seed; confusing demo state. No prod impact (seeder is opt-in,
profile-gated).

## Proposed Fix
Reorder the reset path: wipe products/images/stock first, delete Keycloak
users last. Add a comment noting the ordering is the crash-convergence
guarantee.

## Regression Test
If practical without a live Keycloak, a unit test asserting the reset
operation's call ORDER (wipe-products before delete-users) against fakes;
otherwise a code-level assertion is acceptable given the tool's dev-only
nature — state clearly what the test covers.

## Verification
PASS (independent verification agent, 2026-09-09). Verifier confirmed the
diff is order-only (pre/post reset block compared step-by-step; delete loop
byte-identical), independently traced the convergence reasoning for all three
crash windows, and empirically ran the new order test against the pre-fix
script (fails: `assert 4 < 0` — wipe after deletes) and post-fix (passes).

## Tests
`pytest tests/unit/test_catalog_seed_reset.py -q` → 2 passed.
`pytest tests/unit -q` (full) → **500 passed, 1 skipped** (main 498 + 2 new).

## Validation
`ruff check src tests scripts/catalog_seed.py` → pass; `ruff format --check`
(incl. script) → clean (236 files); `py_compile` OK; basedpyright on the
script → 2 pre-existing baseline errors only, zero new. Commit `f05f950`
stat = exactly the script + the new test file; pre-existing dirty files
untouched.
