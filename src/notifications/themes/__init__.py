"""Email themes for the notification worker — the startup-decided confirmation copy.

One theme is a directory of ``string.Template`` files (``subject.txt``,
``body.txt``, ``body.html``) — stdlib substitution, no template-engine
dependency. The worker loads ONE theme at boot (fail-fast: a missing dir or
file is a boot failure, the same contract as the sender selection) and renders
every confirmation from it:

* default (``notification_email_theme_dir`` empty): the packaged
  ``ecommerce-minimal`` theme next to this module — the shared backend stays
  minimal and unbranded (its multi-frontend contract);
* a frontend brands the mail by mounting its own theme dir into the
  notification-consumer container and pointing the setting at it — decided at
  container start, never mid-run (the same switch the Keycloak realm's
  ``emailTheme`` uses for the verify/set-up mail).

The items list is variable-length, which a ``string.Template`` cannot loop —
the caller renders the lines per format (plain text / ``<li>``) and passes them
as the single ``$items`` placeholder. Item lines stay data-shaped (validated
``OrderPlaced`` wire values); the theme owns the wrapper copy and subject.
"""

from __future__ import annotations

from pathlib import Path
from string import Template

# The packaged default, resolved from this file's location — correct both in a
# repo checkout (dev) and in the image (``COPY . .`` bakes the whole repo).
_DEFAULT_THEME_DIR = Path(__file__).resolve().parent / "ecommerce-minimal"

_THEME_FILES = ("subject.txt", "body.txt", "body.html")


class EmailTheme:
    """One loaded theme: the confirmation's subject + text + HTML templates."""

    def __init__(self, name: str, subject: Template, body_text: Template, body_html: Template) -> None:
        self.name = name
        self._subject = subject
        self._body_text = body_text
        self._body_html = body_html

    @classmethod
    def from_settings(cls, theme_dir: str) -> EmailTheme:
        """Load the configured theme (or the packaged default when unset). Fail-fast."""
        path = Path(theme_dir) if theme_dir else _DEFAULT_THEME_DIR
        name = path.name
        templates: dict[str, Template] = {}
        for file_name in _THEME_FILES:
            file_path = path / file_name
            if not file_path.is_file():
                raise RuntimeError(f"email theme {name!r} is missing {file_name} (dir: {path})")
            text = file_path.read_text(encoding="utf-8")
            if file_name == "subject.txt":
                # The trailing newline is file hygiene (text files end with one),
                # not subject content — the SMTP Subject header must not carry it.
                # body.txt's trailing newline IS content: the body ends with one.
                text = text.strip()
            templates[file_name] = Template(text)
        return cls(name, templates["subject.txt"], templates["body.txt"], templates["body.html"])

    def render_order_confirmation(
        self, order_id: str, total: str, items_lines_text: str, items_lines_html: str
    ) -> tuple[str, str, str]:
        """Render the confirmation's subject + text body + HTML body.

        Values arrive as the validated event's wire shapes (ids and prices as
        strings) plus the caller-rendered item lines per format; they are
        substituted verbatim so the email mirrors the order.
        """
        values = {"order_id": order_id, "total": total}
        subject = self._subject.substitute(order_id=order_id)
        body_text = self._body_text.substitute(**values, items=items_lines_text)
        body_html = self._body_html.substitute(**values, items=items_lines_html)
        return subject, body_text, body_html
