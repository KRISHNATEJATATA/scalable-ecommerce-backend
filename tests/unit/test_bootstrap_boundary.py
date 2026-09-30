"""Boundary check import-linter cannot express: what a route may take from the composition root.

``api.routes -> bootstrap.container`` is exempt from the independence contract (routes need
the DI providers). But the container also *imports* every module's adapters and services, so
``from src.bootstrap.container import InventoryRepository`` in ``orders/api/routes.py`` would be a
cross-module reach that lint cannot see. Routes may only import names the container **defines**
(providers, ``*Dep`` aliases, glue classes) — never ones it merely re-exports.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parents[2] / "src"
CONTAINER = "src.bootstrap.container"
ROUTES = sorted(SRC.glob("*/api/routes.py"))


def _defined_names(tree: ast.Module) -> set[str]:
    names: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
            names.add(node.name)
        elif isinstance(node, ast.Assign):
            names.update(t.id for t in node.targets if isinstance(t, ast.Name))
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names.add(node.target.id)
    return names


DEFINED_BY_CONTAINER = _defined_names(ast.parse((SRC / "bootstrap" / "container.py").read_text(encoding="utf-8")))


def _absolute_module(node: ast.ImportFrom, package: list[str]) -> str:
    """Resolve ``from ..x import y`` against the importing file's package, as the interpreter would."""
    if node.level == 0:
        return node.module or ""
    base = package[: len(package) - (node.level - 1)]
    return ".".join([*base, *([node.module] if node.module else [])])


def test_routes_are_discovered() -> None:
    assert ROUTES, "no src/*/api/routes.py found — the glob is stale"


@pytest.mark.parametrize(
    ("statement", "resolved"),
    [
        ("from src.bootstrap.container import x", "src.bootstrap.container"),
        ("from ...bootstrap.container import x", "src.bootstrap.container"),
        ("from ... import bootstrap", "src"),
        ("from .schemas import x", "src.orders.api.schemas"),
        ("from ..application import x", "src.orders.application"),
    ],
)
def test_relative_imports_resolve_to_absolute_modules(statement: str, resolved: str) -> None:
    node = ast.parse(statement).body[0]
    assert isinstance(node, ast.ImportFrom)
    assert _absolute_module(node, ["src", "orders", "api"]) == resolved


@pytest.mark.parametrize("routes", ROUTES, ids=lambda p: p.parent.parent.name)
def test_routes_take_only_container_defined_names_from_bootstrap(routes: Path) -> None:
    package = ["src", routes.parent.parent.name, "api"]
    tree = ast.parse(routes.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            module = _absolute_module(node, package)
            if module.startswith("src.bootstrap"):
                assert module == CONTAINER, f"{routes}: routes may import only {CONTAINER}, not {module}"
                leaked = {alias.name for alias in node.names} - DEFINED_BY_CONTAINER
                assert not leaked, f"{routes}: {sorted(leaked)} are re-exported internals, not container providers"
            elif module == "src":
                # `from ... import bootstrap` reaches the composition root without naming it in `module`.
                assert not any(a.name == "bootstrap" for a in node.names), f"{routes}: import {CONTAINER} explicitly"
        elif isinstance(node, ast.Import):
            assert not any(a.name.startswith("src.bootstrap") for a in node.names), f"{routes}: use `from ... import`"
