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


class _ByDayDianomiStream(DianomiStream):
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
    @cached_property
    def primary_keys(self):
        return ("date",)

    @override
    def get_new_paginator(self):
        start_date = self.get_starting_timestamp(self.context).date()
        return DateRangePaginator(start_date, days=7)

    @override
    def get_url_params(self, context, next_page_token):
        params = super().get_url_params(context, next_page_token)

        start, end = next_page_token

        return params | {
            "date1": _ByDayDianomiStream.to_api_date(start),
            "date2": _ByDayDianomiStream.to_api_date(end),
        }


class ActionsByAdVariantStream(DianomiStream):
    """Actions by ad variant with estimated viewed impressions."""

    name = "actions_by_ad_variant"
    stat_id = 2465
    primary_keys = ("campaign_id", "variant_id")


class ActionsByPublisherStream(DianomiStream):
    """Actions aggregated by publisher."""

    name = "actions_by_publisher"
    stat_id = 2371
    primary_keys = ("Publisher_ID",)


class ActionsByPublisherByDayStream(_ByDayDianomiStream):
    """Actions aggregated by publisher, broken down by day."""

    name = "actions_by_publisher_by_day"
    stat_id = 2595

    @override
    @cached_property
    def primary_keys(self):
        return (
            *super().primary_keys,
            "Publisher_ID",
        )


class ActionsByPublisherPerCampaignByDayStream(_ByDayDianomiStream):
    """Actions aggregated by publisher and campaign, broken down by day."""

    name = "actions_by_publisher_per_campaign_by_day"
    stat_id = 2596

    @override
    @cached_property
    def primary_keys(self):
        return (
            *super().primary_keys,
            "campaign_id",
            "Publisher_ID",
        )


class AggregateCampaignPerformanceByDayStream(_ByDayDianomiStream):
    """Aggregate campaign performance broken down by day."""

    name = "aggregate_campaign_performance_by_day"
    stat_id = 2583

    @override
    @cached_property
    def primary_keys(self):
        return (
            *super().primary_keys,
            "campaign_name",
        )


class PerformanceByAdVariantByDayStream(_ByDayDianomiStream):
    """Expanded performance metrics per ad variant broken down by day."""

    name = "performance_by_ad_variant_by_day"
    stat_id = 2377

    @override
    @cached_property
    def primary_keys(self):
        return (
            *super().primary_keys,
            "variant_id",
        )


class PerformanceByCampaignWithActionsStream(DianomiStream):
    """Campaign-level performance including action metrics."""

    name = "performance_by_campaign_with_actions"
    stat_id = 2380
    primary_keys = ("Campaign_Id",)
