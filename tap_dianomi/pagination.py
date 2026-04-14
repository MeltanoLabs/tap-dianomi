"""Pagination classes for tap-dianomi."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from singer_sdk.pagination import BaseAPIPaginator
from typing_extensions import override


class DateRangePaginator(BaseAPIPaginator[tuple[date, date]]):
    """Date range paginator."""

    @override
    def __init__(self, start_value: date, *, days: int) -> None:
        super().__init__((start_value, start_value + timedelta(days=days)))
        self.days = days

    @override
    def has_more(self, response):
        return max(self.current_value) < datetime.now(tz=timezone.utc).date()

    @override
    def get_next(self, response):
        return tuple(d + timedelta(days=self.days + 1) for d in self.current_value)

    @override
    def continue_if_empty(self, response):
        return True
