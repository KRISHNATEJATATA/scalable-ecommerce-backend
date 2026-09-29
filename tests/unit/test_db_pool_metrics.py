"""Pool-status gauges (``src/shared/observability/db_pool_metrics.py``).

The point of the module is that connection pressure is *visible* before it is
an outage, so the checks are: the gauges exist with the configured capacity the
budget formula counts, the live values are real (an idle pool reads zero, a
checked-out connection shows up), and re-registering a label (engine
re-creation in every test and lifespan) swaps instead of raising on duplicate
timeseries.
"""

from prometheus_client import generate_latest

from src.shared.clients.postgres_client import create_engine, create_probe_engine
from src.shared.config.setting import AppSettings
from src.shared.observability.db_pool_metrics import register_pool_metrics


def _settings(**overrides) -> AppSettings:
    return AppSettings(**overrides)


def test_pool_gauges_report_configured_capacity():
    create_engine(_settings())  # api pool: 5 + 10
    create_probe_engine(_settings())

    body = generate_latest().decode()

    assert 'db_pool_connections_capacity{pool="api"} 15.0' in body
    assert 'db_pool_connections_capacity{pool="probe"} 1.0' in body
    # Idle pool, nothing opened: the live gauges read zero — size() would
    # report the *configured* 5 and raw overflow() would read -5, so these
    # assertions pin the derived semantics (open = out + in, overflow >= 0).
    assert 'db_pool_connections_checked_out{pool="api"} 0.0' in body
    assert 'db_pool_connections_open{pool="api"} 0.0' in body
    assert 'db_pool_connections_overflow{pool="api"} 0.0' in body


def test_worker_pool_registers_under_its_own_label():
    create_engine(_settings(), worker=True)  # worker pool: 2 + 0

    body = generate_latest().decode()

    assert 'db_pool_connections_capacity{pool="worker"} 2.0' in body


def test_reregistration_swaps_instead_of_duplicating():
    first = create_engine(_settings())
    register_pool_metrics("api", first, capacity=15)  # same label again
    create_engine(_settings())  # and engine re-creation registers afresh

    body = generate_latest().decode()

    assert body.count('db_pool_connections_capacity{pool="api"}') == 1


async def test_checked_out_connection_shows_up(async_engine):
    """A real checkout against real Postgres moves the gauge — the whole point."""
    register_pool_metrics("checkout-probe", async_engine, capacity=5)

    async with async_engine.connect():
        body = generate_latest().decode()
        assert 'db_pool_connections_checked_out{pool="checkout-probe"} 1.0' in body
        assert 'db_pool_connections_open{pool="checkout-probe"} 1.0' in body
