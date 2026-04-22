"""Dianomi tap class."""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone

import requests
from singer_sdk import Tap
from singer_sdk import typing as th

from tap_dianomi.client import DianomiStream
from tap_dianomi.streams import (
    STREAM_PRIMARY_KEYS,
    STREAM_REPLICATION_KEYS,
    ByDayDianomiStream,
)

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override


class TapDianomi(Tap):
    """Singer tap for Dianomi."""

    name = "tap-dianomi"
    dynamic_catalog = True

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key",
            th.StringType,
            required=True,
            secret=True,
            title="API Key",
            description="Dianomi API key",
        ),
        th.Property(
            "email",
            th.StringType,
            required=True,
            title="Email",
            description="Dianomi account email address",
        ),
        th.Property(
            "client_id",
            th.IntegerType,
            title="Client ID",
            description="Advertiser Client ID to filter results by - required for advertisers",
        ),
        th.Property(
            "partner_id",
            th.IntegerType,
            title="Partner ID",
            description="Publisher partner ID to filter results by - required for publishers",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            title="Start Date",
            description=(
                "The earliest record date to sync (ISO 8601 format) - defaults to the last year"
            ),
            default=(datetime.now(tz=timezone.utc) - timedelta(days=365)).date().isoformat(),
        ),
    ).to_dict()

    @override
    def discover_streams(self):
        headers = {
            "X-Auth-Key": self.config["api_key"],
            "X-Auth-Email": self.config["email"],
            "X-Auth-Client-Id": (client_id := self.config.get("client_id")) and str(client_id),
        }

        response = requests.get(
            "https://my.dianomi.com/cgi-bin/genienav.pl",
            headers=headers,
            timeout=300,
        )
        response.raise_for_status()

        available_stats: dict[str, dict] = response.json()

        for stream_name, stat in available_stats.items():
            stream_cls = DianomiStream

            if stream_name not in STREAM_REPLICATION_KEYS:
                self.logger.warning(
                    "No replication key defined for %s",
                    stream_name,
                )

            if STREAM_REPLICATION_KEYS.get(stream_name) == "date":
                stream_cls = ByDayDianomiStream

            stream = stream_cls(self, name=stream_name, stat_id=stat["stat_id"])

            if stream_name not in STREAM_PRIMARY_KEYS:
                self.logger.warning(
                    "No replication key defined for %s",
                    stream_name,
                )

            stream.primary_keys = STREAM_PRIMARY_KEYS.get(stream_name, ())

            yield stream


if __name__ == "__main__":
    TapDianomi.cli()
