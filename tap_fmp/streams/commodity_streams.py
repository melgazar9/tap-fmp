"""Commodity stream types classes for tap-fmp."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_fmp.client import (
    FmpSurrogateKeyStream,
    SymbolPartitionStream,
    SymbolPartitionTimeSliceStream,
)
from tap_fmp.mixins import (
    BaseSymbolPartitionMixin,
    CommodityConfigMixin,
    ChartLightMixin,
    ChartFullMixin,
    Prices1minMixin,
    Prices5minMixin,
    Prices1HrMixin,
)


class CommoditiesListStream(CommodityConfigMixin, FmpSurrogateKeyStream):
    """Stream for Commodities List API."""

    name = "commodities_list"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("name", th.StringType),
        th.Property("exchange", th.StringType),
        th.Property("trade_month", th.StringType),
        th.Property("currency", th.StringType),
    ).to_dict()

    def create_record_from_item(self, item: str) -> dict:
        """Create a record dict from a commodity symbol."""
        return {
            "symbol": item,
            "name": None,
            "exchange": None,
            "trade_month": None,
            "currency": None,
        }

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/commodities-list"


class CommodityPartitionStream(SymbolPartitionStream):
    """Base class for commodity streams that need commodity symbol partitioning."""

    @property
    def partitions(self):
        return self._tap.get_cached_commodities()


class TimestampProcessingMixin:
    """Mixin for processing timestamp/date fields in commodity records."""

    def post_process(self, record: dict, context: Context | None = None) -> dict:
        from datetime import datetime

        if "date" not in record and "timestamp" in record:
            try:
                timestamp_val = int(record["timestamp"])
                record["date"] = datetime.fromtimestamp(timestamp_val).isoformat()
            except (ValueError, TypeError, OSError):
                pass
        elif "date" in record and isinstance(record["date"], str):
            if " " in record["date"] and "T" not in record["date"]:
                record["date"] = record["date"].replace(" ", "T")

        return super().post_process(record, context)


class CommoditySymbolPartitionMixin(TimestampProcessingMixin, BaseSymbolPartitionMixin):
    """Mixin for commodity streams that provides symbol partitioning."""

    @property
    def selection_config_section(self) -> str:
        return "commodities"

    @property
    def selection_field_name(self) -> str:
        return "select_commodities"

    def get_cached_symbols(self) -> list[dict]:
        return self._tap.get_cached_commodities()


class CommodityPriceMixin(
    TimestampProcessingMixin,
    BaseSymbolPartitionMixin,
    FmpSurrogateKeyStream,
):
    """Mixin for commodity quote streams that need surrogate keys."""

    @property
    def selection_config_section(self) -> str:
        return "commodities"

    @property
    def selection_field_name(self) -> str:
        return "select_commodities"

    def get_cached_symbols(self) -> list[dict]:
        return self._tap.get_cached_commodities()


class CommoditiesQuoteStream(CommodityPriceMixin, CommodityPartitionStream):
    """Stream for Commodities Quote API."""

    name = "commodities_quotes"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("name", th.StringType),
        th.Property("price", th.NumberType),
        th.Property("change_percentage", th.NumberType),
        th.Property("change", th.NumberType),
        th.Property("volume", th.IntegerType),
        th.Property("day_low", th.NumberType),
        th.Property("day_high", th.NumberType),
        th.Property("year_high", th.NumberType),
        th.Property("year_low", th.NumberType),
        th.Property("market_cap", th.NumberType),
        th.Property("price_avg50", th.NumberType),
        th.Property("price_avg200", th.NumberType),
        th.Property("exchange", th.StringType),
        th.Property("open", th.NumberType),
        th.Property("previous_close", th.NumberType),
        th.Property("timestamp", th.IntegerType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/quote"


class CommoditiesQuoteShortStream(CommodityPriceMixin, CommodityPartitionStream):
    name = "commodities_quote_short"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("price", th.NumberType),
        th.Property("change", th.NumberType),
        th.Property("volume", th.IntegerType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/quote-short"


class AllCommoditiesQuotesStream(FmpSurrogateKeyStream):
    """Stream for All Commodities Quotes API."""

    name = "all_commodities_quotes"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("price", th.NumberType),
        th.Property("change", th.NumberType),
        th.Property("volume", th.IntegerType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/batch-commodity-quotes"


class CommoditiesLightChartStream(
    CommoditySymbolPartitionMixin,
    ChartLightMixin,
    SymbolPartitionTimeSliceStream,
):
    name = "commodities_light_chart"


class CommoditiesFullChartStream(
    CommoditySymbolPartitionMixin,
    ChartFullMixin,
    SymbolPartitionTimeSliceStream,
):
    name = "commodities_full_chart"


class Commodities1minStream(
    CommoditySymbolPartitionMixin,
    Prices1minMixin,
    SymbolPartitionTimeSliceStream,
):
    name = "commodities_1min"


class Commodities5minStream(
    CommoditySymbolPartitionMixin,
    Prices5minMixin,
    SymbolPartitionTimeSliceStream,
):
    name = "commodities_5min"


class Commodities1HrStream(
    CommoditySymbolPartitionMixin,
    Prices1HrMixin,
    SymbolPartitionTimeSliceStream,
):
    name = "commodities_1h"
