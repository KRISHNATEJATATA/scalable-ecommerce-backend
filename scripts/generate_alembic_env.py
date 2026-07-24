"""Regenerate each module's Alembic ``env.py`` from one template.

The 5 per-module ``env.py`` files are identical except for the module name
This script is the single place to change the shared body; the generated
files stay boring and reviewable in git.

Usage::

    python scripts/generate_alembic_env.py
"""

from pathlib import Path
from string import Template

REPO_ROOT = Path(__file__).resolve().parents[1]
TEMPLATE_PATH = REPO_ROOT / "scripts" / "alembic_env.py.tmpl"
MODULES = ["identity", "catalog", "inventory", "orders", "payments"]


def main() -> None:
    template = Template(TEMPLATE_PATH.read_text(encoding="utf-8"))
    for module in MODULES:
        out_path = REPO_ROOT / "src" / module / "migrations" / "env.py"
        out_path.write_text(template.substitute(module=module), encoding="utf-8")
        print(f"wrote {out_path.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()
