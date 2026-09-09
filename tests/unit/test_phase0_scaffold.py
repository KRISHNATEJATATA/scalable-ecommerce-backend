"""Phase 0 smoke test: settings load + fail-fast, and every src package imports."""

import importlib

import pytest
from pydantic import ValidationError

from src.shared.config.setting import AppSettings

_DB_URL_SCHEME = "postgresql+asyncpg"
_DSN = f"{_DB_URL_SCHEME}://u:p@localhost:5432/db"

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


def test_reaper_grace_must_outlive_the_image_visibility_timeout(monkeypatch):
    """An upload whose event is still in flight must never be reapable: the grace is
    what keeps the sweep from clearing the token that flip is guarded on."""

    monkeypatch.setenv("DATABASE_URL", _DSN)
    with pytest.raises(ValidationError):
        AppSettings(_env_file=None, image_upload_reaper_grace_seconds=300, image_visibility_timeout_seconds=300)


def test_shipped_reaper_defaults_satisfy_the_invariant(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", _DSN)
    s = AppSettings(_env_file=None)
    assert s.image_upload_reaper_grace_seconds > s.image_visibility_timeout_seconds


def test_demo_seeding_enabled_by_default(monkeypatch):
    """Deliberate contract: seeding is ON unless explicitly
    switched off — the seeder is a manual, profile-gated compose one-shot, never
    part of `compose up`."""

    monkeypatch.setenv("DATABASE_URL", _DSN)
    monkeypatch.delenv("SEED_DEMO_DATA", raising=False)
    s = AppSettings(_env_file=None)
    assert s.seed_demo_data is True


def test_demo_seeding_kill_switch(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", _DSN)
    monkeypatch.setenv("SEED_DEMO_DATA", "0")
    s = AppSettings(_env_file=None)
    assert s.seed_demo_data is False
