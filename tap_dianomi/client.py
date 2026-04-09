"""REST client handling, including DianomiStream base class."""

from __future__ import annotations

import sys
from datetime import date, datetime, timezone
from typing import TYPE_CHECKING, Any, ClassVar

from singer_sdk.streams import RESTStream

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Iterable

    import requests
    from singer_sdk.helpers.types import Context


class DianomiStream(RESTStream):
    """Dianomi stream class."""

    url_base = "https://my.dianomi.com"
    path = "/cgi-bin/selfserve/stat.pl"

    stat_id: ClassVar[int]

    _DATE_FORMAT = "%Y%m%d"

    @property
    @override
    def http_headers(self) -> dict:
        """Return the HTTP headers needed for Dianomi authentication."""
        return {
            "X-Auth-Key": self.config.get("api_key", ""),
            "X-Auth-Email": self.config.get("email", ""),
        }

    @override
    def get_new_paginator(self) -> None:
        """Return None - Dianomi reporting endpoints do not paginate."""
        return None

    def _to_api_date(self, value: date | datetime | str) -> str:
        """Convert a date value to the yyyymmdd format expected by the Dianomi API.

        Args:
            value: A date, datetime, or ISO-format date string.

        Returns:
            Date string in yyyymmdd format.
        """
        if isinstance(value, str):
            if len(value) == 8 and value.isdigit():
                return value
            # Strip time component if present (e.g. "2024-01-01T00:00:00+00:00")
            value = datetime.fromisoformat(value.split("T")[0])
        if isinstance(value, datetime):
            return value.strftime(self._DATE_FORMAT)
        return value.strftime(self._DATE_FORMAT)

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Return URL parameters for the Dianomi stats endpoint.

        Builds a date range from the last synced date (or configured start_date)
        to today, and appends the stat_id and optional filtering parameters.

        Args:
            context: The stream context.
            next_page_token: Unused - Dianomi does not paginate.

        Returns:
            A dictionary of URL query parameters.
        """
        start_value = self.get_starting_replication_key_value(context)
        start_dt: date | datetime | str | None = start_value or self.config.get("start_date")
        end_dt = datetime.now(tz=timezone.utc)

        params: dict[str, Any] = {
            "stat_id": self.stat_id,
            "format": "json",
            "date1": self._to_api_date(start_dt) if start_dt else self._to_api_date(end_dt),
            "date2": self._to_api_date(end_dt),
        }

        if partner_id := self.config.get("partner_id"):
            params["partner_id"] = partner_id

        if client_id := self.config.get("client_id"):
            params["client_id"] = client_id

        return params

    @override
    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the API response and yield individual records.

        Handles both a top-level JSON array and a JSON object with a
        nested data array (under keys "data", "results", "records", or "rows").

        Args:
            response: The HTTP response object.

        Yields:
            Each record as a dictionary.
        """
        data = response.json()
        if isinstance(data, list):
            yield from data
            return
        if isinstance(data, dict):
            for key in ("data", "results", "records", "rows"):
                if key in data and isinstance(data[key], list):
                    yield from data[key]
                    return

    @override
    def post_process(
        self,
        row: dict,
        context: Context | None = None,
    ) -> dict | None:
        """Normalise yyyymmdd date strings to ISO format (yyyy-mm-dd).

        Args:
            row: An individual record from the stream.
            context: The stream context.

        Returns:
            The updated record dictionary.
        """
        for key in ("date", "start_date", "end_date"):
            raw = row.get(key)
            if isinstance(raw, str) and len(raw) == 8 and raw.isdigit():
                try:
                    row[key] = datetime.strptime(raw, self._DATE_FORMAT).date().isoformat()
                except ValueError:
                    pass
        return row
