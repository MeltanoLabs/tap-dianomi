"""Dianomi entry point.

Copyright (c) 2026 Meltano.
"""

from __future__ import annotations

from tap_dianomi.tap import TapDianomi

TapDianomi.cli()
