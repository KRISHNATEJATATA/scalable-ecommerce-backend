"""CI gate: fail the build if any pytest test was skipped (reads the JUnit XML).

A skipped crown-jewel test is a silent green: the DB/Valkey/LocalStack paths
this suite owns must run for real on the runner, never skip on a
missing dependency. The allowlist below is deliberately **EMPTY** — add a name
only with a written justification and a tracking ticket, never to make a red
build pass.

Usage: ``python scripts/ci_no_skips.py [junit.xml]``
"""

from __future__ import annotations

import sys
import xml.etree.ElementTree as ET

#: Empty on purpose. A skip in CI must be loud (this gate) or explicitly
#: re-enabled by fixing its dependency.
ALLOWED: set[str] = set()


def main(path: str) -> int:
    cases = [case for case in ET.parse(path).iter("testcase")]
    if not cases:
        # Zero testcases means the run never actually executed anything (a
        # catastrophic collection or a mis-pointed pytest) — silence here would
        # read as green, so fail loudly instead.
        print(f"FAIL: {path} contains no testcases — the suite did not run")
        return 1
    skipped: list[tuple[str, str]] = []
    for case in cases:
        for skip in case.iter("skipped"):
            name = f"{case.get('classname', '')}.{case.get('name', '')}"
            if name not in ALLOWED:
                skipped.append((name, (skip.get("message") or "").strip()[:160]))
    if skipped:
        print(f"FAIL: {len(skipped)} skipped test(s) — the allowlist is empty; run them for real:")
        for name, message in skipped:
            print(f"  - {name}: {message}")
        return 1
    print(f"ok: no skipped tests ({len(cases)} testcases)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "test-results/unit.xml"))
