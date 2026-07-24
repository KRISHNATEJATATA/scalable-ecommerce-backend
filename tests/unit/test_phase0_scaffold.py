"""Phase 0 smoke test: settings load + fail-fast, and every src package imports."""

import importlib

import pytest
from pydantic import ValidationError

from src.shared.config.setting import AppSettings

_MODULES = ["catalog", "inventory", "orders", "payments", "identity", "cart"]
_LAYERS = ["api", "application", "domain", "ports", "adapters"]
_DB_MODULES = ["catalog", "inventory", "orders", "payments", "identity"]  # cart has no DB schema (Valkey-only)

SRC_MODULES = [
    "src",
    "src.shared",
    "src.shared.clients",
    "src.shared.clients.valkey_client",
    "src.shared.clients.s3_client",
    "src.shared.config",
    "src.shared.config.setting",
    "src.shared.errors",
    "src.shared.middleware",
    "src.shared.middleware.security",
    "src.shared.db",
    "src.shared.db.mixins",
    "src.shared.container",
    *[f"src.{m}.{layer}" for m in _MODULES for layer in _LAYERS],
    *[f"src.{m}.adapters.db.models" for m in _DB_MODULES],
    "src.cart.adapters.valkey",
]


def test_settings_fail_fast_when_database_url_missing(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    with pytest.raises(ValidationError):
        AppSettings(_env_file=None)


def test_settings_load_when_database_url_present(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://user:pass@localhost:5432/db")
    s = AppSettings(_env_file=None)
    assert s.jwt_algorithm == "RS256"
    assert s.keycloak_realm == "ecommerce"
    assert s.api_v1_prefix == "/v1"
    assert s.environment == "local"


@pytest.mark.parametrize("module", SRC_MODULES)
def test_every_src_package_imports(module):
    importlib.import_module(module)
