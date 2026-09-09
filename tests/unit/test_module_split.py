"""Module-split smoke tests: schema-per-module models, constraints, and per-module Alembic chains.

Runs against the shared session-scoped Testcontainers-Postgres from
``tests/unit/conftest.py`` — real Postgres, never SQLite, and **no
environment-dependent skip**: if Docker is unavailable, the suite fails loudly
instead of silently skipping the database checks (the old stand-in fixture
skipped whenever a localhost Postgres was unreachable, letting constraint
regressions land green on machines without a DB).

The shared ``_migrated`` fixture has already run every module's Alembic chain to
head; ``sessionmaker_factory`` (via ``async_engine``) truncates all tables before
each test, so these inserts start from a clean schema.
"""

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


def _sync_url() -> str:
    return str(get_settings().database_url).replace("+asyncpg", "+psycopg2")


@pytest.fixture
async def engine(_migrated, sessionmaker_factory):
    """A sync engine on the same Testcontainers database the async fixtures use.

    Depends on ``sessionmaker_factory`` so the per-test truncate has run first;
    migrations are owned by the shared session-scoped ``_migrated`` fixture (this
    file used to run the chains itself against a possibly-absent localhost DB).
    """
    eng = sqlalchemy.create_engine(_sync_url())
    yield eng
    eng.dispose()


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


def test_identity_email_is_not_unique_but_sub_is(engine):
    """A recycled Keycloak email must be insertable as a *new* principal.

    Keycloak enforces email uniqueness only among current accounts, so a freed
    address is reusable. A `UNIQUE(email)` here would force a recreated account
    to either fail JIT provisioning or take over the previous holder's row (and
    with it their orders/products). `UNIQUE(oidc_sub)` is the real key.
    """
    with engine.connect() as conn:
        for sub in ("dup-sub-1", "dup-sub-2"):
            conn.execute(
                text(
                    f"INSERT INTO {IDENTITY_SCHEMA}.users (id, oidc_sub, email, is_active) "
                    "VALUES (gen_random_uuid(), :sub, 'dup@example.com', true)"
                ),
                {"sub": sub},
            )
        conn.commit()

        with pytest.raises(IntegrityError):  # sub, however, is still unique
            with conn.begin():
                conn.execute(
                    text(
                        f"INSERT INTO {IDENTITY_SCHEMA}.users (id, oidc_sub, email, is_active) "
                        "VALUES (gen_random_uuid(), 'dup-sub-1', 'other@example.com', true)"
                    )
                )
        conn.execute(text(f"DELETE FROM {IDENTITY_SCHEMA}.users WHERE email = 'dup@example.com'"))
        conn.commit()


def test_product_and_inventory_use_distinct_optimistic_lock_mechanisms():
    """Product: ORM-managed version_id_col. Inventory: manual CAS `version` column."""
    assert Product.__mapper__.version_id_col is Product.__table__.c.version_id
    assert Inventory.__mapper__.version_id_col is None
    assert "version" in Inventory.__table__.c
