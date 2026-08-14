# Copyright (c) 2026 Meltano.

"""Dianomi entry point."""

from __future__ import annotations

from tap_dianomi.tap import TapDianomi

TapDianomi.cli()
