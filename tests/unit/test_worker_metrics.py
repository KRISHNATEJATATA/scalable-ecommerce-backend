"""Worker metrics export (`src/shared/observability/worker_metrics.py`).

The point of this module is that worker counters reach Prometheus at all, so the
checks are: off by default (no port bound, no network call from a test run), on
when configured, and a dead Pushgateway never takes the job down with it.
"""

import urllib.request

import pytest
from prometheus_client import Counter

from src.shared.config.setting import AppSettings
from src.shared.observability.worker_metrics import push_worker_metrics, serve_worker_metrics

_probe = Counter("test_worker_probe_total", "Probe counter for the worker exporter test.")


def _settings(**overrides) -> AppSettings:
    return AppSettings(secret_key="x" * 32, **overrides)


def test_exporter_is_off_unless_configured(monkeypatch):
    """Unset means unset: no port bound, no request made."""
    monkeypatch.setattr(
        "src.shared.observability.worker_metrics.start_http_server",
        lambda *a, **k: pytest.fail("started a server without WORKER_METRICS_PORT"),
    )
    monkeypatch.setattr(
        "src.shared.observability.worker_metrics.push_to_gateway",
        lambda *a, **k: pytest.fail("pushed without METRICS_PUSHGATEWAY_URL"),
    )

    serve_worker_metrics(_settings(), job="test")
    push_worker_metrics(_settings(), job="test")


def test_configured_port_serves_this_process_counters():
    """A looping worker's counters become scrapeable."""
    port = 9187
    _probe.inc()
    serve_worker_metrics(_settings(worker_metrics_port=port), job="test")

    with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as response:  # noqa: S310
        body = response.read().decode()

    assert "test_worker_probe_total" in body


def test_a_failing_push_does_not_break_the_job(monkeypatch):
    """Telemetry is not worth re-running a reaper sweep over."""

    def _boom(*_args, **_kwargs):
        raise OSError("pushgateway down")

    monkeypatch.setattr("src.shared.observability.worker_metrics.push_to_gateway", _boom)

    push_worker_metrics(_settings(metrics_pushgateway_url="http://nowhere:9091"), job="test")
