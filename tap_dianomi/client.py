"""REST client handling, including DianomiStream base class."""

from __future__ import annotations

import contextlib
import sys
from datetime import datetime, timezone
from functools import cached_property
from typing import ClassVar

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

    @override
    @property
    def http_headers(self):
        return {
            "X-Auth-Key": self.config["api_key"],
            "X-Auth-Email": self.config["email"],
        }

    @override
    def get_new_paginator(self):
        return None

    @cached_property
    def _base_params(self):
        params = {
            "format": "json",
            "stat_id": self.stat_id,
        }

        if client_id := self.config.get("client_id"):
            params["client_id"] = client_id

        if partner_id := self.config.get("partner_id"):
            params["partner_id"] = partner_id

        return params

    @override
    @cached_property
    def schema(self):
        response = self.requests_session.get(
            self.url_base + self.path,
            headers=self.http_headers,
            params=self._base_params,
        )

        response.raise_for_status()

        if response.text == "failed - no permission to access this stat":
            self.logger.warning("Cloud not discover schema for '%s', %s", self.name, response.text)
            return {}

        cols = response.json()["cols"]

        return th.PropertiesList(
            *[
                th.Property(
                    col["label"],
                    _DIANOMI_TYPE_MAP.get(col["type"], th.StringType),
                )
                for col in cols
            ]
        ).to_dict()

    @cached_property
    def _start_date(self):
        return datetime.fromisoformat(self.config["start_date"])

    @override
    def get_url_params(self, context, next_page_token):
        start_dt = self.get_starting_timestamp(context) or self._start_date
        end_dt = datetime.now(tz=timezone.utc)

        return {
            **self._base_params,
            "date1": start_dt.strftime(_DATE_FORMAT),
            "date2": end_dt.strftime(_DATE_FORMAT),
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
