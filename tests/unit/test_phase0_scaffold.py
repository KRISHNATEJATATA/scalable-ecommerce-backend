"""Phase 0 smoke test: settings load + fail-fast, and every src package imports."""

import importlib

import pytest
from pydantic import ValidationError

from src.config.setting import AppSettings

SRC_MODULES = [
    "src",
    "src.admin",
    "src.clients",
    "src.clients.valkey_client",
    "src.clients.s3_client",
    "src.config",
    "src.config.setting",
    "src.errors",
    "src.middleware",
    "src.middleware.security",
    "src.models",
    "src.models.base",
    "src.repositories",
    "src.routes",
    "src.services",
    "src.container",
]


def test_settings_fail_fast_when_database_url_missing(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    with pytest.raises(ValidationError):
        AppSettings(_env_file=None)


def test_settings_load_when_database_url_present(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://u:p@localhost:5432/db")
    s = AppSettings(_env_file=None)
    assert s.jwt_algorithm == "RS256"
    assert s.environment == "local"


@pytest.mark.parametrize("module", SRC_MODULES)
def test_every_src_package_imports(module):
    importlib.import_module(module)
