"""REST client handling, including DianomiStream base class."""

from __future__ import annotations

import contextlib
import sys
from datetime import date, datetime, timezone
from functools import cached_property
from typing import TYPE_CHECKING, Any, ClassVar

from singer_sdk import typing as th
from singer_sdk.streams import RESTStream

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Iterable

    import requests
    from singer_sdk.helpers.types import Context


_DIANOMI_TYPE_MAP: dict[str, type[th.JSONTypeHelper]] = {
    "string": th.StringType,
    "number": th.NumberType,
}

_DATE_FORMAT = "%Y%m%d"
_DATE_FORMAT_LEN = 8


class DianomiStream(RESTStream):
    """Dianomi stream class."""

    url_base = "https://my.dianomi.com"
    path = "/cgi-bin/selfserve/stat.pl"

    stat_id: ClassVar[int]

    @property
    @override
    def http_headers(self) -> dict:
        """Return the HTTP headers required for Dianomi authentication."""
        return {
            "X-Auth-Key": self.config.get("api_key", ""),
            "X-Auth-Email": self.config.get("email", ""),
        }

    @override
    def get_new_paginator(self) -> None:
        """Dianomi reporting endpoints do not paginate."""

    def _to_api_date(self, value: date | datetime | str) -> str:
        """Convert a date value to the yyyymmdd format expected by the Dianomi API.

        Args:
            value: A date, datetime, or ISO-format date string.

        Returns:
            Date string in yyyymmdd format.
        """
        if isinstance(value, str):
            if len(value) == _DATE_FORMAT_LEN and value.isdigit():
                return value
            value = datetime.fromisoformat(value.split("T")[0])
        if isinstance(value, datetime):
            return value.strftime(_DATE_FORMAT)
        return value.strftime(_DATE_FORMAT)

    def _base_params(self) -> dict[str, Any]:
        """Return common query parameters shared by schema and data requests."""
        params: dict[str, Any] = {"stat_id": self.stat_id, "format": "json"}
        if client_id := self.config.get("client_id"):
            params["client_id"] = client_id
        if partner_id := self.config.get("partner_id"):
            params["partner_id"] = partner_id
        return params

    @cached_property
    def _cols(self) -> list[dict]:
        """Fetch column definitions from the API for this stream's stat_id.

        Omits date parameters so the API returns column metadata only,
        without requiring a valid date range.

        Returns:
            A list of column definition dicts with ``label`` and ``type`` keys.
        """
        response = self.requests_session.get(
            f"{self.url_base}{self.path}",
            headers=self.http_headers,
            params=self._base_params(),
        )
        response.raise_for_status()
        try:
            return response.json().get("cols", [])
        except Exception:  # noqa: BLE001
            return []

    @override
    @cached_property
    def schema(self) -> dict:
        """Dynamically build the stream schema from the API's column definitions.

        Returns:
            A JSON Schema dict derived from the ``cols`` metadata returned by
            the Dianomi stats endpoint.
        """
        return th.PropertiesList(
            *[
                th.Property(
                    col["label"],
                    _DIANOMI_TYPE_MAP.get(col["type"], th.StringType),
                )
                for col in self._cols
            ]
        ).to_dict()

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Return URL parameters for the Dianomi stats endpoint.

        Builds a date range from the last synced date (or configured
        start_date) to today, merged with common stat and filter params.

        Args:
            context: The stream context.
            next_page_token: Unused - Dianomi does not paginate.

        Returns:
            A dictionary of URL query parameters.
        """
        start_value = self.get_starting_replication_key_value(context)
        start_dt: date | datetime | str | None = start_value or self.config.get("start_date")
        end_dt = datetime.now(tz=timezone.utc)

        return {
            **self._base_params(),
            "date1": self._to_api_date(start_dt) if start_dt else self._to_api_date(end_dt),
            "date2": self._to_api_date(end_dt),
        }

    @override
    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the API's cols/rows response and yield individual records.

        Handles both object rows (dicts) and array rows (lists), zipping
        array rows with the column labels returned alongside.

        Args:
            response: The HTTP response object.

        Yields:
            Each record as a dictionary keyed by column label.
        """
        try:
            data = response.json()
        except Exception:  # noqa: BLE001
            return

        cols = [c["label"] for c in data.get("cols", [])]
        for row in data.get("rows", []):
            if isinstance(row, list):
                yield dict(zip(cols, row, strict=False))
            elif isinstance(row, dict):
                yield row

    @override
    def post_process(
        self,
        row: dict,
        context: Context | None = None,
    ) -> dict | None:
        """Normalise any yyyymmdd date strings in the record to ISO format.

        Args:
            row: An individual record from the stream.
            context: The stream context.

        Returns:
            The updated record dictionary.
        """
        for key, raw in row.items():
            if isinstance(raw, str) and len(raw) == _DATE_FORMAT_LEN and raw.isdigit():
                with contextlib.suppress(ValueError):
                    row[key] = datetime.strptime(raw, _DATE_FORMAT).date().isoformat()  # noqa: DTZ007
        return row
