"""Dianomi tap class."""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone

from singer_sdk import Tap
from singer_sdk import typing as th

from tap_dianomi import streams

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override


class TapDianomi(Tap):
    """Singer tap for Dianomi."""

    name = "tap-dianomi"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key",
            th.StringType,
            required=True,
            secret=True,
            title="API Key",
            description="API key provided by your Dianomi account manager",
        ),
        th.Property(
            "email",
            th.StringType,
            required=True,
            title="Email",
            description="Email address associated with your Dianomi account",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            title="Start Date",
            description="The earliest record date to sync (ISO 8601 format)",
            default=(datetime.now(tz=timezone.utc) - timedelta(days=365)).date().isoformat(),
        ),
        th.Property(
            "partner_id",
            th.IntegerType,
            title="Partner ID",
            description=(
                "Publisher Partner ID to filter results. "
                "Found in the left sidebar of the Dianomi web interface."
            ),
        ),
        th.Property(
            "client_id",
            th.IntegerType,
            title="Client ID",
            description="Advertiser Client ID to filter results.",
        ),
    ).to_dict()

    @override
    def discover_streams(self) -> list[streams.DianomiStream]:
        """Return a list of discovered streams.

        Returns:
            A list of all available Dianomi reporting streams.
        """
        return [
            streams.ActionsByAdVariantStream(self),
            streams.ActionsByPublisherStream(self),
            streams.AggregateCampaignPerformanceByDayStream(self),
            streams.PerformanceByAdVariantByDayStream(self),
            streams.PerformanceByCampaignWithActionsStream(self),
        ]


if __name__ == "__main__":
    TapDianomi.cli()
