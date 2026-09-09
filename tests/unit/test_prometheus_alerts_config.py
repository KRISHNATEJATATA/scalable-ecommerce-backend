"""BUG-008 regression guard: alert rules must only reference scrape jobs that exist.

ops/prometheus/inventory-reaper-alerts.yaml once fired InventoryReaperDown on
`up{job="reservation-reaper"}` while ops/prometheus/prometheus.yml only defines
`api`, `workers` and `postgres-exporter` — so `absent(...)` was permanently 1
and the alert fired ~5m after every Prometheus boot. This test parses both
files and asserts every `job="..."` / `job=~"..."` matcher used in alert
expressions names a real `job_name:` in the scrape config.
"""

import re
from pathlib import Path

import yaml

OPS = Path(__file__).resolve().parents[2] / "ops" / "prometheus"
ALERTS_FILE = OPS / "inventory-reaper-alerts.yaml"
PROMETHEUS_FILE = OPS / "prometheus.yml"

JOB_MATCHER = re.compile(r'job=~?"([^"]+)"')

# Metrics that live on the Pushgateway, not in any scrape: their `job` label is
# the pushing client's grouping key (METRICS_PUSHGATEWAY_URL), never a
# prometheus.yml job_name, so those matchers cannot be validated here.
PUSHGATEWAY_METRICS = ("push_time_seconds",)


def _referenced_jobs(expr: str) -> set[str]:
    for metric in PUSHGATEWAY_METRICS:
        expr = re.sub(rf"{metric}\{{[^}}]*\}}", "", expr)
    return set(JOB_MATCHER.findall(expr))


def test_alert_job_selectors_exist_in_scrape_config() -> None:
    prometheus = yaml.safe_load(PROMETHEUS_FILE.read_text(encoding="utf-8"))
    scrape_jobs = {scrape["job_name"] for scrape in prometheus["scrape_configs"]}
    assert {"api", "workers", "postgres-exporter"} <= scrape_jobs

    alerts = yaml.safe_load(ALERTS_FILE.read_text(encoding="utf-8"))
    referenced: dict[str, list[str]] = {}
    for group in alerts["groups"]:
        for rule in group["rules"]:
            for job in _referenced_jobs(rule.get("expr", "")):
                referenced.setdefault(job, []).append(rule["alert"])

    unknown = {job: alerts_ for job, alerts_ in referenced.items() if job not in scrape_jobs}
    assert not unknown, f"alerts reference jobs prometheus.yml never scrapes: {unknown}"
