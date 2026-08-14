"""Stream type classes for tap-dianomi."""

from __future__ import annotations

import sys
from functools import cached_property

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

from singer_sdk import typing as th

from tap_dianomi.client import DianomiStream
from tap_dianomi.pagination import DateRangePaginator

STREAM_PRIMARY_KEYS = {
    "ad_performance_variant_viewable_tz_2": ("campaign_id", "variant_id"),
    "activity_pub_impressions_tz": ("Publisher_ID",),
    "product_performance_variant_actions_tz": ("Campaign_Id",),
    "activity_pub_impressions_tz_by_day": ("date", "Publisher_ID"),
    "activity_pub_impressions_tz_per_campaign_by_day": ("date", "campaign_id", "Publisher_ID"),
    "activity_pub_impressions_tz_per_campaign_by_day_em": ("date", "campaign_id", "Publisher_ID"),
    "performance_campaign_day_tz": ("date", "campaign_name"),
    "ad_performance_variant_day_ex_tz": ("date", "variant_id"),
    "ad_performance_variant_day_ex_tz_em": ("date", "variant_id"),
}

STREAM_REPLICATION_KEYS = {
    "ad_performance_variant_viewable_tz_2": None,
    "activity_pub_impressions_tz": None,
    "product_performance_variant_actions_tz": None,
    "activity_pub_impressions_tz_by_day": "date",
    "activity_pub_impressions_tz_per_campaign_by_day": "date",
    "activity_pub_impressions_tz_per_campaign_by_day_em": "date",
    "performance_campaign_day_tz": "date",
    "ad_performance_variant_day_ex_tz": "date",
    "ad_performance_variant_day_ex_tz_em": "date",
}


class ByDayDianomiStream(DianomiStream):
    """Base for day-level streams with date-range pagination."""

    replication_key = "date"
    is_timestamp_replication_key = True
    is_sorted = True

    @override
    @cached_property
    def schema(self):
        schema = super().schema
        schema["properties"][self.replication_key] = th.DateType().to_dict()
        return schema

    @override
    def get_new_paginator(self):
        start_date = self.get_starting_timestamp(self.context).date()
        return DateRangePaginator(start_date, days=7)

    @override
    def get_url_params(self, context, next_page_token):
        params = super().get_url_params(context, next_page_token)
        start, end = next_page_token
        return params | {
            "date1": ByDayDianomiStream.to_api_date(start),
            "date2": ByDayDianomiStream.to_api_date(end),
        }
