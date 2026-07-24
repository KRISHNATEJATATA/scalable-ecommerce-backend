# GENERATED FILE — do not edit by hand.
# Source: scripts/alembic_env.py.tmpl — edit that, then run:
#     python scripts/generate_alembic_env.py
"""Alembic environment for the ``identity`` schema — one sync chain per module.

Deliberately separate from every other module's chain (own alembic.ini, own
migrations/, own ``alembic_version`` table living in this schema): see
Alembic stays sync (psycopg2) even though the app is async everywhere else.
"""

import re
from logging.config import fileConfig

from alembic import context
from sqlalchemy import create_engine, pool, text

from src.identity.adapters.db.models import SCHEMA, Base
from src.shared.config.setting import get_settings

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def _include_name(name: str, type_: str, parent_names: dict) -> bool:
    # Autogenerate must only ever see this module's own schema — otherwise it
    # proposes dropping sibling modules' tables (incl. their alembic_version).
    if type_ == "schema":
        return name == SCHEMA
    return True


def _sync_url() -> str:
    # App uses the async asyncpg driver; Alembic migrations stay sync (psycopg2).
    return re.sub(r"\+asyncpg", "+psycopg2", str(get_settings().database_url))


def run_migrations_offline() -> None:
    context.configure(
        url=_sync_url(),
        target_metadata=target_metadata,
        version_table_schema=SCHEMA,
        include_schemas=True,
        include_name=_include_name,
        literal_binds=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    engine = create_engine(_sync_url(), poolclass=pool.NullPool)
    with engine.connect() as connection:
        # The version table lives in this module's own schema; the schema
        # must exist before Alembic can create/read it. Each migration's
        # upgrade() also issues this statement for the module's own tables.
        connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}"))
        connection.commit()
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            version_table_schema=SCHEMA,
            include_schemas=True,
            include_name=_include_name,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
