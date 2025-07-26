"""Commodity stream types classes for tap-fmp."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.helpers.types import Context
from tap_fmp.streams.chart_streams import (
    ChartLightStream,
    PriceVolumeStream,
    Prices1minStream,
    Prices5minStream,
    Prices1HrStream,
)

from tap_fmp.client import FmpRestStream, SymbolPartitionStream


class CommoditiesListStream(FmpRestStream):
    """Stream for Commodities List API."""

    name = "commodities_list"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("name", th.StringType),
        th.Property("exchange", th.StringType),
        th.Property("trade_month", th.StringType),
        th.Property("currency", th.StringType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/commodities-list"


class CommodityPartitionStream(SymbolPartitionStream):
    """Base class for commodity streams that need commodity symbol partitioning."""

    @property
    def partitions(self):
        return self._tap.get_cached_commodities()


class CommodityPriceMixin(FmpRestStream):
    @property
    def partitions(self):
        query_params_symbol = self.query_params.get("symbol")
        other_params_symbols = self.config.get("other_params", {}).get("symbols")

        assert not (query_params_symbol and other_params_symbols), (
            f"Cannot specify symbol configurations in both query_params and "
            f"other_params for stream {self.name}."
        )

        if query_params_symbol:
            return (
                [{"symbol": query_params_symbol}]
                if isinstance(query_params_symbol, str)
                else query_params_symbol
            )
        elif other_params_symbols:
            return (
                [{"symbol": symbol} for symbol in other_params_symbols]
                if isinstance(other_params_symbols, list)
                else other_params_symbols
            )
        else:
            return [
                {"symbol": c.get("symbol")} for c in self._tap.get_cached_commodities()
            ]


class CommoditiesQuoteStream(CommodityPriceMixin, CommodityPartitionStream):
    """Stream for Commodities Quote API."""

    name = "commodities_quotes"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True

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
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("price", th.NumberType),
        th.Property("change", th.NumberType),
        th.Property("volume", th.IntegerType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/quote-short"


class AllCommoditiesQuotesStream(FmpRestStream):
    """Stream for All Commodities Quotes API."""

    name = "all_commodities_quotes"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("price", th.NumberType),
        th.Property("change", th.NumberType),
        th.Property("volume", th.IntegerType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/batch-commodity-quotes"


class CommoditiesLightChartStream(CommodityPriceMixin, ChartLightStream):
    name = "commodities_light_chart"


class CommoditiesFullChartStream(CommodityPriceMixin, PriceVolumeStream):
    name = "commodities_full_chart"


class Commodities1minStream(CommodityPriceMixin, Prices1minStream):
    name = "commodities_1min"


class Commodities5minStream(CommodityPriceMixin, Prices5minStream):
    name = "commodities_5min"


class Commodities1HrStream(CommodityPriceMixin, Prices1HrStream):
    name = "commodities_1hr"
