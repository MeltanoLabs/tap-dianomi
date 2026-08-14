"""Ensure each Python module docstring ends with a Meltano copyright notice.

Copyright (c) 2026 Meltano.
"""

from __future__ import annotations

import datetime
import re
import sys

HOLDER = "Meltano"
NOTICE_RE = re.compile(r"Copyright \(c\) \d{4}(?:-\d{4})? " + re.escape(HOLDER) + r"\.")


def notice() -> str:
    """Return the copyright notice for the current year."""
    return f"Copyright (c) {datetime.date.today().year} {HOLDER}."


def add_notice(text: str) -> str:
    """Return text with the copyright notice in the module docstring."""
    if NOTICE_RE.search(text):
        return text  # A notice exists. Keep the year unchanged.
    stripped = text.lstrip()
    for quote in ('"""', "'''"):
        if stripped.startswith(quote):
            lead = text[: len(text) - len(stripped)]
            close = stripped.find(quote, len(quote))
            if close == -1:
                break
            body = stripped[len(quote) : close].rstrip("\n")
            after = stripped[close:]  # Closing quote plus the rest of the file.
            return f"{lead}{quote}{body}\n\n{notice()}\n{after}"
    # No module docstring. Add one that holds the notice.
    return f'"""{notice()}"""\n\n{text}'


def main(paths: list[str]) -> int:
    """Add the notice to each file. Return 1 if a file changed."""
    changed = False
    for path in paths:
        with open(path, encoding="utf-8") as fh:
            src = fh.read()
        out = add_notice(src)
        if out != src:
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(out)
            print(f"added copyright notice to {path}")
            changed = True
    return 1 if changed else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
