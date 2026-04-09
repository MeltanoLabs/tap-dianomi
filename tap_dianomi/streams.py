"""Stream type classes for tap-dianomi."""

from __future__ import annotations

from tap_dianomi.client import DianomiStream


class ActionsByAdVariantStream(DianomiStream):
    """Actions by ad variant with estimated viewed impressions."""

    name = "actions_by_ad_variant"
    stat_id = 2485
    primary_keys = ("campaign_id", "variant_id")


class ActionsByPublisherStream(DianomiStream):
    """Actions aggregated by publisher."""

    name = "actions_by_publisher"
    stat_id = 2371
    primary_keys = ("Publisher_ID",)


class AggregateCampaignPerformanceByDayStream(DianomiStream):
    """Aggregate campaign performance broken down by day."""

    name = "aggregate_campaign_performance_by_day"
    stat_id = 2583
    primary_keys = ("date", "campaign_name")
    replication_key = "date"
    is_sorted = True


class PerformanceByAdVariantByDayStream(DianomiStream):
    """Expanded performance metrics per ad variant broken down by day."""

    name = "performance_by_ad_variant_by_day"
    stat_id = 2377
    primary_keys = ("date", "variant_id")
    replication_key = "date"
    is_sorted = True


class PerformanceByCampaignWithActionsStream(DianomiStream):
    """Campaign-level performance including action metrics."""

    name = "performance_by_campaign_with_actions"
    stat_id = 2380
    primary_keys = ("Campaign_Id",)
