# BUG-008: `InventoryReaperDown` alert references a Prometheus job that does not exist — permanent false positive

## Severity
Medium

## Status
Fixed

## Fix Branch
bug/BUG-008-reaper-alert-job-label-mismatch (commit e95970f — NOT merged to main)

## Fix
Option (a): `InventoryReaperDown` selectors now
`up{job="workers", instance=~"reaper:9100"} == 0 or absent(...same...)` —
matches the shipped `workers` job's literal target string. Pushgateway rule
annotated dead (no Pushgateway deployed; its `job` is a grouping key, never a
scrape job). New config-guard test
`tests/unit/test_prometheus_alerts_config.py` asserts every `job="…"`/`job=~"…"`
matcher in the alerts file exists as a `job_name` in prometheus.yml (proven to
flag `reservation-reaper` on the pre-fix file). RUNBOOK needed no change
(no job-label prose).

## Summary
`ops/prometheus/inventory-reaper-alerts.yaml:31` evaluates
`up{job="reservation-reaper"} == 0 or absent(up{job="reservation-reaper"})`,
but the shipped `ops/prometheus/prometheus.yml` defines only three jobs:
`api`, `workers`, `postgres-exporter` — there is no `reservation-reaper` job
(the string `reservation-reaper` appears only as a worker_metrics
grouping/Pushgateway label, never as a scrape job label).

## Expected Behavior
The alert is healthy when the reaper worker is up and firing only when it is
down.

## Actual Behavior
With the shipped config (prometheus.yml:13-14 loads the rules file),
`absent(up{job="reservation-reaper"})` is permanently 1 → `InventoryReaperDown`
fires ~5 minutes after every Prometheus boot and never clears, healthy reaper
or not. The real liveness signal (`InventoryReaperBacklog`, which is correct)
drowns in permanent alert noise.

## Reproduction
Load `ops/prometheus/prometheus.yml` (it includes the alerts rule file) against
a healthy stack → the alert fires and never resolves. Static check: grep
`job_name:` in prometheus.yml (3 jobs) vs the selectors in the alerts file.

## Root Cause
Selector/label mismatch between the alerts file and the actual scrape config
(the alerts were written against an intended job layout that was never
provisioned).

## Affected Area
`ops/prometheus/inventory-reaper-alerts.yaml` (and, depending on the chosen
fix, `ops/prometheus/prometheus.yml`); docs/RUNBOOK.md §8 references the alert.

## Impact
Permanent false alarm buries the reaper's genuine liveness protection; on-call
alert fatigue; a real reaper outage is indistinguishable from the noise.

## Proposed Fix
Pick ONE and make the pair consistent:
(a) fix the alert selectors to the shipped config — e.g.
`up{job="workers", instance=~".*reaper.*"} == 0 or absent(up{job="workers", instance=~".*reaper.*"})`;
or (b) split a dedicated `reservation-reaper` job in prometheus.yml scraping
`reaper:9100` so the existing selectors become true. Document whichever shape
RUNBOOK §8 promises. Keep the Pushgateway-based rule (:42) as-is (currently
dead because no Pushgateway is deployed — annotate it).

## Regression Test
Config-level assertion: a unit/CI check that every `job="..."` selector used in
ops/prometheus/*.yaml exists as a `job_name` in prometheus.yml (prevents the
mismatch class from recurring).

## Verification
PASS (independent verification agent, 2026-09-09). Verifier confirmed the
fixed selector against the Prometheus label model (instance == literal target
`reaper:9100`, anchored regex, `absent()` covers never-scraped), audited the
other two alerts for the same bug class (backlog metric name verified against
postgres-exporter-queries.yaml; Pushgateway rule correctly exempt), and proved
the guard test flags `reservation-reaper` on the pre-fix file and passes
post-fix.

## Tests
`pytest tests/unit/test_prometheus_alerts_config.py -q` → 1 passed.
`pytest tests/unit -q` (full) → **499 passed, 1 skipped** (main 498 + 1 new).

## Validation
`ruff check src tests` → pass; `ruff format --check` → clean; all three
ops/prometheus YAML files parse via yaml.safe_load (promtool/Spectral not
installed on this host). Commit `e95970f` stat = exactly the alerts file +
the new test; pre-existing dirty files untouched.
