"""REST client handling, including DianomiStream base class."""

from __future__ import annotations

import contextlib
import sys
from datetime import date, datetime, timezone
from functools import cached_property
from typing import Any, ClassVar

from singer_sdk import typing as th
from singer_sdk.exceptions import FatalAPIError
from singer_sdk.streams import RESTStream

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override


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
    def http_headers(self):
        return {
            "X-Auth-Key": self.config["api_key"],
            "X-Auth-Email": self.config["email"],
        }

    @override
    def get_new_paginator(self):
        return None

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
        self.validate_response(response)

        return response.json().get("cols", [])

    @override
    @cached_property
    def schema(self):
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
    def get_url_params(self, context, next_page_token):
        start_value = self.get_starting_replication_key_value(context)
        start_dt: date | datetime | str | None = start_value or self.config.get("start_date")
        end_dt = datetime.now(tz=timezone.utc)

        return {
            **self._base_params(),
            "date1": self._to_api_date(start_dt) if start_dt else self._to_api_date(end_dt),
            "date2": self._to_api_date(end_dt),
        }

    @override
    def validate_response(self, response):
        super().validate_response(response)

        if response.text == "failed - no permission to access this stat":
            raise FatalAPIError(response.text)

    @override
    def parse_response(self, response):
        data = response.json()

        cols = [c["label"] for c in data["cols"]]
        rows = (r["c"] for r in data["rows"])

        for row in rows:
            record = {c: r["v"] for c, r in zip(cols, row, strict=True)}
            yield record

    @override
    def post_process(self, row, context=None):
        for key, raw in row.items():
            if isinstance(raw, str) and len(raw) == _DATE_FORMAT_LEN and raw.isdigit():
                with contextlib.suppress(ValueError):
                    row[key] = datetime.strptime(raw, _DATE_FORMAT).date().isoformat()  # noqa: DTZ007
        return row
