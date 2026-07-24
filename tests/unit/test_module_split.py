"""Module-split smoke tests: schema-per-module models, constraints, and per-module Alembic chains.

Requires a real Postgres reachable via ``DATABASE_URL`` (Testcontainers-managed
in CI; see ``tests/unit/conftest.py`` if one is added later). Skips cleanly
when no Postgres is reachable, so this file doesn't fail collection in
environments without a DB.

ponytail: the skip-if-unreachable fixture is a stand-in until ticket 17
(crown-jewel-risk-tests) wires a shared Testcontainers-Postgres fixture for
the whole suite (see .scratch/distributed-ecommerce-backend/issues/17-crown-
jewel-risk-tests.md) — that ticket is the one that makes this DB coverage
mandatory in CI instead of best-effort.
"""

import subprocess
import sys
from pathlib import Path

import pytest
import sqlalchemy
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from src.catalog.adapters.db.models import SCHEMA as CATALOG_SCHEMA
from src.catalog.adapters.db.models import Product
from src.identity.adapters.db.models import SCHEMA as IDENTITY_SCHEMA
from src.inventory.adapters.db.models import SCHEMA as INVENTORY_SCHEMA
from src.inventory.adapters.db.models import Inventory
from src.shared.config.setting import get_settings

REPO_ROOT = Path(__file__).resolve().parents[2]

MODULES = ["identity", "catalog", "inventory", "orders", "payments"]


def _sync_url() -> str:
    return str(get_settings().database_url).replace("+asyncpg", "+psycopg2")


@pytest.fixture(scope="module")
def engine():
    try:
        eng = sqlalchemy.create_engine(_sync_url())
        with eng.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as exc:  # pragma: no cover - environment-dependent
        pytest.skip(f"Postgres not reachable: {exc}")
    return eng


@pytest.fixture(scope="module", autouse=True)
def migrated(engine):
    """Run every module's own Alembic chain to head before the tests in this file."""
    for module in MODULES:
        subprocess.run(
            [sys.executable, "-m", "alembic", "-c", f"src/{module}/alembic.ini", "upgrade", "head"],
            cwd=REPO_ROOT,
            check=True,
        )
    yield


def test_inventory_check_constraints_reject_bad_rows(engine):
    with engine.connect() as conn:
        with pytest.raises(IntegrityError):
            with conn.begin():
                conn.execute(
                    text(
                        f"INSERT INTO {INVENTORY_SCHEMA}.inventory (sku, on_hand, reserved, version) "
                        "VALUES ('ck-test-neg', -1, 0, 1)"
                    )
                )
        with pytest.raises(IntegrityError):
            with conn.begin():
                conn.execute(
                    text(
                        f"INSERT INTO {INVENTORY_SCHEMA}.inventory (sku, on_hand, reserved, version) "
                        "VALUES ('ck-test-over', 1, 5, 1)"
                    )
                )


def test_catalog_price_check_constraint_rejects_non_positive(engine):
    with engine.connect() as conn:
        with pytest.raises(IntegrityError):
            with conn.begin():
                conn.execute(
                    text(
                        f"INSERT INTO {CATALOG_SCHEMA}.products (id, merchant_id, name, price, version_id) "
                        "VALUES (gen_random_uuid(), gen_random_uuid(), 'bad', 0, 1)"
                    )
                )


def test_identity_email_unique_constraint(engine):
    with engine.connect() as conn:
        conn.execute(
            text(
                f"INSERT INTO {IDENTITY_SCHEMA}.users (id, oidc_sub, email, is_active) "
                "VALUES (gen_random_uuid(), 'dup-sub-1', 'dup@example.com', true)"
            )
        )
        conn.commit()
        with pytest.raises(IntegrityError):
            with conn.begin():
                conn.execute(
                    text(
                        f"INSERT INTO {IDENTITY_SCHEMA}.users (id, oidc_sub, email, is_active) "
                        "VALUES (gen_random_uuid(), 'dup-sub-2', 'dup@example.com', true)"
                    )
                )
        conn.execute(text(f"DELETE FROM {IDENTITY_SCHEMA}.users WHERE oidc_sub = 'dup-sub-1'"))
        conn.commit()


def test_product_and_inventory_use_distinct_optimistic_lock_mechanisms():
    """Product: ORM-managed version_id_col. Inventory: manual CAS `version` column."""
    assert Product.__mapper__.version_id_col is Product.__table__.c.version_id
    assert Inventory.__mapper__.version_id_col is None
    assert "version" in Inventory.__table__.c
