"""Stream type classes for tap-dianomi."""

from __future__ import annotations

from typing import ClassVar

from singer_sdk import typing as th

from tap_dianomi.client import DianomiStream


class DataBySmartadByDayStream(DianomiStream):
    """Daily performance metrics broken down by smartad."""

    name = "data_by_smartad_by_day"
    primary_keys: ClassVar[tuple[str, ...]] = ("date", "id")
    replication_key = "date"
    is_sorted = True
    stat_id = 1393

    schema = th.PropertiesList(
        th.Property("date", th.DateType, description="Report date (yyyy-mm-dd)"),
        th.Property("id", th.IntegerType, description="Smartad identifier"),
        th.Property("description", th.StringType, description="Smartad description"),
        th.Property("impressions", th.IntegerType, description="Number of ad impressions"),
        th.Property("clicks", th.IntegerType, description="Number of ad clicks"),
        th.Property("ctr", th.NumberType, description="Click-through rate"),
        th.Property("revenue", th.NumberType, description="Revenue generated"),
        th.Property("ecpm", th.NumberType, description="Effective cost per thousand impressions"),
    ).to_dict()


class PerformanceByCountryStream(DianomiStream):
    """Daily performance metrics broken down by country."""

    name = "performance_by_country"
    primary_keys: ClassVar[tuple[str, ...]] = ("date", "country")
    replication_key = "date"
    is_sorted = True
    stat_id = 1418

    schema = th.PropertiesList(
        th.Property("date", th.DateType, description="Report date (yyyy-mm-dd)"),
        th.Property("country", th.StringType, description="Country name or ISO code"),
        th.Property("impressions", th.IntegerType, description="Number of ad impressions"),
        th.Property("clicks", th.IntegerType, description="Number of ad clicks"),
        th.Property("revenue", th.NumberType, description="Revenue generated"),
        th.Property("cpm", th.NumberType, description="Cost per thousand impressions"),
    ).to_dict()


class PartnerRevenueOverTimeStream(DianomiStream):
    """Aggregated partner revenue over a date range."""

    name = "partner_revenue_over_time"
    primary_keys: ClassVar[tuple[str, ...]] = ("start_date", "end_date")
    replication_key = "end_date"
    is_sorted = True
    stat_id = 1388

    schema = th.PropertiesList(
        th.Property("start_date", th.DateType, description="Start date of the reporting period"),
        th.Property("end_date", th.DateType, description="End date of the reporting period"),
        th.Property("currency_symbol", th.StringType, description="Currency symbol for revenue values"),
        th.Property("revenue", th.NumberType, description="Total revenue for the period"),
    ).to_dict()


class RevenueByDeviceStream(DianomiStream):
    """Daily revenue metrics broken down by device type."""

    name = "revenue_by_device"
    primary_keys: ClassVar[tuple[str, ...]] = ("date", "device_type")
    replication_key = "date"
    is_sorted = True
    stat_id = 1560

    schema = th.PropertiesList(
        th.Property("date", th.DateType, description="Report date (yyyy-mm-dd)"),
        th.Property("device_type", th.StringType, description="Device type (e.g. desktop, mobile, tablet)"),
        th.Property("impressions", th.IntegerType, description="Number of ad impressions"),
        th.Property("clicks", th.IntegerType, description="Number of ad clicks"),
        th.Property("ctr", th.NumberType, description="Click-through rate"),
        th.Property("revenue", th.NumberType, description="Revenue generated"),
        th.Property("ecpm", th.NumberType, description="Effective cost per thousand impressions"),
    ).to_dict()


class RevenueByProductTypeStream(DianomiStream):
    """Daily revenue metrics broken down by product type."""

    name = "revenue_by_product_type"
    primary_keys: ClassVar[tuple[str, ...]] = ("date", "id")
    replication_key = "date"
    is_sorted = True
    stat_id = 1398

    schema = th.PropertiesList(
        th.Property("date", th.DateType, description="Report date (yyyy-mm-dd)"),
        th.Property("id", th.IntegerType, description="Product type identifier"),
        th.Property("product_type", th.StringType, description="Product type name"),
        th.Property("revenue", th.NumberType, description="Revenue generated"),
    ).to_dict()


class SmartadPerformanceStream(DianomiStream):
    """Aggregated performance metrics per smartad."""

    name = "smartad_performance"
    primary_keys: ClassVar[tuple[str, ...]] = ("date", "id")
    replication_key = "date"
    is_sorted = True
    stat_id = 1505

    schema = th.PropertiesList(
        th.Property("date", th.DateType, description="Report date (yyyy-mm-dd)"),
        th.Property("id", th.IntegerType, description="Smartad identifier"),
        th.Property("description", th.StringType, description="Smartad description"),
        th.Property("impressions", th.IntegerType, description="Number of ad impressions"),
        th.Property("clicks", th.IntegerType, description="Number of ad clicks"),
        th.Property("ctr", th.NumberType, description="Click-through rate"),
        th.Property("revenue", th.NumberType, description="Revenue generated"),
        th.Property("ecpm", th.NumberType, description="Effective cost per thousand impressions"),
    ).to_dict()
