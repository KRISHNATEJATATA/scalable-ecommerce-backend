# BUG-001: `SEED_DEMO_DATA` fails open — demo-seeding opt-in gate is inverted at the default

## Severity
Medium

## Status
Fixed

## Fix Branch
bug/BUG-001-seed-demo-data-fails-open (commit 94e0b6f — NOT merged to main)

## Fix
`AppSettings.seed_demo_data` default flipped `True` → `False` (fails closed);
field comment updated. README's factually wrong parenthetical corrected.
Two regression tests added (`tests/unit/test_phase0_scaffold.py`):
no env var → `is False`; `SEED_DEMO_DATA=1` → `is True` (monkeypatch +
`_env_file=None`, hermetic). Compose's `SEED_DEMO_DATA: "1"` keeps
`make seed` working.

## Summary
`AppSettings.seed_demo_data` defaults to `True`, so the documented "explicit
opt-in" gate on demo seeding only exists when an operator explicitly sets the
variable to `0`. Omitting the variable — the normal case for any half-configured
environment — unlocks seeding, the exact opposite of the documented fail-safe.

## Expected Behavior
Per `.env.example`, `README.md`, and the field's own comment: "The seeder
refuses to run without it, so hardcoded demo credentials + fictional catalog
must be structurally incapable of reaching a real deployment." A missing
`SEED_DEMO_DATA` env var must leave seeding disabled.

## Actual Behavior
With no `SEED_DEMO_DATA` env var, `AppSettings().seed_demo_data` is `True` and
`scripts/catalog_seed.py:489` (`if not settings.seed_demo_data`) proceeds to seed.

## Reproduction
```
python - <<'PY'
import os
os.environ["DATABASE_URL"] = "postgresql+asyncpg://x:x@localhost:5432/x"
os.environ.pop("SEED_DEMO_DATA", None)
from src.shared.config.setting import AppSettings
print(AppSettings(_env_file=None).seed_demo_data)   # -> True (should be False)
PY
```
Verified 2026-09-09: prints `True`.

## Root Cause
`src/shared/config/setting.py:82` — `seed_demo_data: bool = True`. The gate
logic in the seeder is correct; the default is inverted relative to the
documented contract.

## Affected Area
`src/shared/config/setting.py` (field default); `scripts/catalog_seed.py`
(gate consumer); docs: `.env.example`, `README.md`.

## Impact
Demo accounts with known passwords and a fictional catalog can be created in
any environment where the seeder is run without explicit opt-in. Reachability
is bounded (the seeder is a manual compose one-shot under the `seed` profile),
but the config-level invariant the docs promise does not exist.

## Proposed Fix
Flip the field default to `False` and update the field's comment /
`.env.example` wording to match. Compose already sets `SEED_DEMO_DATA: "1"` for
the `catalog-seed` service, so local `make seed` keeps working.

## Regression Test
Unit test: `AppSettings(_env_file=None)` constructed with no env vars has
`seed_demo_data is False`; with `SEED_DEMO_DATA=1` it is `True`.

## Verification
PASS (independent verification agent, 2026-09-09)

## Tests
`pytest tests/unit/test_phase0_scaffold.py -q` → 55 passed.
`pytest tests/unit -q` (full, Testcontainers) → **500 passed, 1 skipped**
(baseline 498+1 + 2 new).

## Validation
Repro re-run by verifier: no env var → `False`; `SEED_DEMO_DATA=1` → `True`;
seeder gate (`scripts/catalog_seed.py:489-496`) verified to refuse with
`SystemExit(2)` when the setting is False. `ruff check src tests` → pass.
`ruff format --check` → clean. `basedpyright` → no new diagnostics
(1 pre-existing baseline error in setting.py, untouched). `git show --stat`
confirms the commit touches exactly its 3 files (pre-existing unrelated dirty
files not committed).
