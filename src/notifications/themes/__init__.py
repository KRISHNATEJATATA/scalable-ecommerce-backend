"""Email themes for the notification worker — the startup-decided copy for every email type.

One theme is a directory of ``string.Template`` files — stdlib substitution, no
template-engine dependency — carrying a triple (subject / text body / HTML body)
per :class:`~src.notifications.domain.email.EmailType`:

* ``order_confirmation``: ``subject.txt`` / ``body.txt`` / ``body.html``;
* ``refund_issued``: ``refund_subject.txt`` / ``refund_body.txt`` / ``refund_body.html``.

The worker loads ONE theme at boot and the loader requires **all six files**
(fail-fast: a missing dir or file is a boot failure, the same contract as the
sender selection) — ONE uniform contract, so a theme never silently falls back
to packaged copy for a type its owner never branded. That is why adding an
email type is a coordinated, breaking change for a frontend-mounted theme dir
(the frontend ships the new files before/with the worker upgrade).

* default (``notification_email_theme_dir`` empty): the packaged
  ``ecommerce-minimal`` theme next to this module — the shared backend stays
  minimal and unbranded (its multi-frontend contract);
* a frontend brands the mail by mounting its own theme dir into the
  notification-consumer container and pointing the setting at it — decided at
  container start, never mid-run (the same switch the Keycloak realm's
  ``emailTheme`` uses for the verify/set-up mail).

The confirmation's items list is variable-length, which a ``string.Template``
cannot loop — the caller renders the lines per format (plain text / ``<li>``)
and passes them as the single ``$items`` placeholder. Item lines stay
data-shaped (validated ``OrderPlaced`` wire values); the theme owns the wrapper
copy and subject. The refund email carries only ``$order_id`` and ``$amount``
(a ``PaymentRefunded`` has no item lines and never names a provider reference).
"""

from __future__ import annotations

from pathlib import Path
from string import Template

from src.notifications.domain.email import EmailType

# The packaged default, resolved from this file's location — correct both in a
# repo checkout (dev) and in the image (``COPY . .`` bakes the whole repo).
_DEFAULT_THEME_DIR = Path(__file__).resolve().parent / "ecommerce-minimal"

# The theme-dir contract: per email type, the three files carrying its copy.
# Adding a member here is a breaking change for every mounted theme dir —
# see the module docstring before extending.
_TYPE_FILES: dict[str, tuple[str, str, str]] = {
    EmailType.ORDER_CONFIRMATION.value: ("subject.txt", "body.txt", "body.html"),
    EmailType.REFUND_ISSUED.value: ("refund_subject.txt", "refund_body.txt", "refund_body.html"),
}


class EmailTheme:
    """One loaded theme: per email type, its subject + text + HTML templates."""

    def __init__(self, name: str, templates: dict[str, tuple[Template, Template, Template]]) -> None:
        self.name = name
        self._templates = templates

    @classmethod
    def from_settings(cls, theme_dir: str) -> EmailTheme:
        """Load the configured theme (or the packaged default when unset). Fail-fast."""
        path = Path(theme_dir) if theme_dir else _DEFAULT_THEME_DIR
        name = path.name
        templates: dict[str, tuple[Template, Template, Template]] = {}
        for email_type, (subject_file, body_file, html_file) in _TYPE_FILES.items():
            loaded: dict[str, Template] = {}
            for is_subject, file_name in ((True, subject_file), (False, body_file), (False, html_file)):
                file_path = path / file_name
                if not file_path.is_file():
                    raise RuntimeError(f"email theme {name!r} is missing {file_name} (dir: {path})")
                text = file_path.read_text(encoding="utf-8")
                if is_subject:
                    # The trailing newline is file hygiene (text files end with
                    # one), not subject content — the SMTP Subject header must
                    # not carry it. A body's trailing newline IS content: the
                    # body ends with one. Flagged by slot, not by filename.
                    text = text.strip()
                loaded[file_name] = Template(text)
            templates[email_type] = (loaded[subject_file], loaded[body_file], loaded[html_file])
        return cls(name, templates)

    def render_order_confirmation(
        self, order_id: str, total: str, items_lines_text: str, items_lines_html: str
    ) -> tuple[str, str, str]:
        """Render the confirmation's subject + text body + HTML body.

        Values arrive as the validated event's wire shapes (ids and prices as
        strings) plus the caller-rendered item lines per format; they are
        substituted verbatim so the email mirrors the order.
        """
        subject, body_text, body_html = self._templates[EmailType.ORDER_CONFIRMATION.value]
        values = {"order_id": order_id, "total": total}
        return (
            subject.substitute(order_id=order_id),
            body_text.substitute(**values, items=items_lines_text),
            body_html.substitute(**values, items=items_lines_html),
        )

    def render_refund_issued(self, order_id: str, amount: str) -> tuple[str, str, str]:
        """Render the refund-issued email's subject + text body + HTML body.

        The wire shapes only: the order and the refunded amount (no item lines,
        no provider reference — customer-safe copy is the theme's contract).
        """
        subject, body_text, body_html = self._templates[EmailType.REFUND_ISSUED.value]
        values = {"order_id": order_id, "amount": amount}
        return (
            subject.substitute(**values),
            body_text.substitute(**values),
            body_html.substitute(**values),
        )
