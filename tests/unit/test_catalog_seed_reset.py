"""regression: ``--reset`` wipes DB-side products BEFORE deleting Keycloak users.

The ordering is the crash-convergence guarantee a mid-reset crash must leave
at worst orphaned Keycloak users and no live local products — state a plain
``make seed`` converges on — never live products stranded on unrecoverable subs
(which would duplicate the catalog on the next seed).

Pure fakes: the orchestrator's collaborators are monkeypatched with recorders
and only the call ORDER is asserted — no DB, no Keycloak, no S3.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import MagicMock

import scripts.catalog_seed as seed


def _install_fakes(monkeypatch: Any, events: list[str]) -> None:
    """Patch every collaborator ``run()`` touches, recording the ordering events."""

    class _FakeAdmin:
        """Keycloak stand-in: demo users start existing; delete/create are recorded."""

        def __init__(self, settings: Any) -> None:
            self._users = {username: f"sub-{username}" for username, *_ in seed.USERS}

        async def find_sub_by_username(self, username: str) -> str | None:
            return self._users.get(username)

        async def delete_user(self, sub: str) -> None:
            username = sub.removeprefix("sub-")
            self._users.pop(username, None)
            events.append(f"delete:{username}")

        async def create_user_with_password(
            self, username: str, email: str, password: str, *, first_name: str, last_name: str
        ) -> str:
            sub = f"new-{username}"
            self._users[username] = sub
            events.append(f"create:{username}")
            return sub

        async def has_realm_role(self, sub: str, role: str) -> bool:
            return True  # role granting is not under test

    async def _noop(*_args: Any, **_kwargs: Any) -> None:
        return None

    async def _fake_wipe_products(sessionmaker: Any, settings: Any) -> int:
        events.append("wipe")
        return len(seed.PRODUCTS)

    async def _fake_ensure_anchor(sessionmaker: Any, sub: str, email: str) -> str:
        return f"anchor-{sub}"  # distinct per sub; satisfies the same-anchor guard

    async def _fake_ensure_product(
        sessionmaker: Any, settings: Any, anchor: Any, spec: tuple[str, str, Any, str, str, int]
    ) -> tuple[Any, bool]:
        return MagicMock(id=f"product-{spec[0]}"), True

    async def _fake_ensure_image(store: Any, sessionmaker: Any, settings: Any, product_id: Any, image_path: Any) -> str:
        return "already-ready"

    @asynccontextmanager
    async def _fake_s3_client(settings: Any) -> Any:
        yield MagicMock()

    monkeypatch.setattr(seed, "get_settings", lambda: MagicMock(s3_bucket="demo-bucket"))
    monkeypatch.setattr(seed, "wait_for_keycloak", _noop)
    monkeypatch.setattr(seed, "create_engine", lambda settings, worker=True: MagicMock(dispose=_noop))
    monkeypatch.setattr(seed, "create_sessionmaker", lambda engine: MagicMock())
    monkeypatch.setattr(seed, "KeycloakIdentityAdmin", _FakeAdmin)
    monkeypatch.setattr(seed, "wipe_products", _fake_wipe_products)
    monkeypatch.setattr(seed, "ensure_anchor", _fake_ensure_anchor)
    monkeypatch.setattr(seed, "s3_client", _fake_s3_client)
    monkeypatch.setattr(seed, "ImageStore", lambda s3, bucket: MagicMock())
    monkeypatch.setattr(seed, "ensure_product", _fake_ensure_product)
    monkeypatch.setattr(seed, "ensure_image", _fake_ensure_image)
    monkeypatch.setattr(seed, "ensure_stock", _noop)


async def test_reset_wipes_products_before_deleting_keycloak_users(monkeypatch: Any) -> None:
    events: list[str] = []
    _install_fakes(monkeypatch, events)

    await seed.run(reset=True)

    deletes = [e for e in events if e.startswith("delete:")]
    creates = [e for e in events if e.startswith("create:")]
    # The bug's regression: the DB-side wipe must precede EVERY Keycloak deletion.
    assert events.index("wipe") < events.index(deletes[0])
    # And the deletions must precede the re-seed's user creation (fresh subs this run).
    assert events.index(deletes[-1]) < events.index(creates[0])
    assert len(deletes) == len(seed.USERS)
    assert len(creates) == len(seed.USERS)


async def test_plain_seed_never_wipes_or_deletes(monkeypatch: Any) -> None:
    events: list[str] = []
    _install_fakes(monkeypatch, events)

    await seed.run(reset=False)

    assert "wipe" not in events
    assert not [e for e in events if e.startswith("delete:")]
    assert not [e for e in events if e.startswith("create:")]  # users already exist → idempotent
