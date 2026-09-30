"""Which schemas publish events.

The set mirrors the modules that own an ``outbox`` table, so it is composition
knowledge: it lives here, not in the ``src.shared`` kernel. Cart is Valkey-only
and has no outbox. The kernel (relay, lag poller) receives this list as an
argument and only checks that each name is a plain SQL identifier.
"""

from __future__ import annotations

# The relay f-strings these into schema-qualified SQL, so the list MUST stay a
# trusted constant (never user input).
OUTBOX_SCHEMAS: tuple[str, ...] = ("identity", "catalog", "inventory", "orders", "payments")
