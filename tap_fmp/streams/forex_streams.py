"""Forex stream types classes for tap-fmp."""

from __future__ import annotations

import typing as t
from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_fmp.client import FmpRestStream, SymbolPartitionStream
from tap_fmp.streams.chart_streams import ChartLightStream, PriceVolumeStream, Prices1minStream, Prices5minStream, Prices1HrStream


class ForexPairsStream(FmpRestStream):
    name = "forex_pairs"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True
    
    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("from_currency", th.StringType),
        th.Property("to_currency", th.StringType),
        th.Property("from_name", th.StringType),
        th.Property("to_name", th.StringType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/forex-list"


class ForexPartitionStream(SymbolPartitionStream):
    @property
    def partitions(self):
        return [{"symbol": forex_json.get("symbol")} for forex_json in self._tap.get_cached_forex_pairs()]


class ForexSymbolPartitionMixin(FmpRestStream):
    @property
    def partitions(self):
        query_params_symbol = self.query_params.get("symbol")
        other_params_symbols = self.config.get("other_params", {}).get("symbols")

        assert not (query_params_symbol and other_params_symbols), (
            f"Cannot specify symbol configurations in both query_params and "
            f"other_params for stream {self.name}."
        )

        if query_params_symbol:
            return [{"symbol": query_params_symbol}] if isinstance(query_params_symbol, str) else query_params_symbol
        elif other_params_symbols:
            return [{"symbol": symbol} for symbol in other_params_symbols] if isinstance(other_params_symbols, list) else other_params_symbols
        else:
            return [{"symbol": forex_json.get("symbol")} for forex_json in self._tap.get_cached_forex_pairs()]


class ForexQuoteStream(ForexSymbolPartitionMixin, ForexPartitionStream):
    """Stream for Forex Quote API."""
    
    name = "forex_quotes"
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
        """Get URL for the request."""
        return f"{self.url_base}/stable/quote"


class ForexQuoteShortStream(ForexSymbolPartitionMixin, ForexPartitionStream):
    """Stream for Forex Short Quote API."""
    
    name = "forex_quotes_short"
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


class BatchForexQuotesStream(FmpRestStream):
    """Stream for Batch Forex Quotes API."""
    
    name = "batch_forex_quotes"
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
        return f"{self.url_base}/stable/batch-forex-quotes"


class ForexLightChartStream(ForexSymbolPartitionMixin, ChartLightStream):
    """Stream for Historical Forex Light Chart API."""

    name = "forex_light_chart"

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/historical-price-eod/light"


class ForexFullChartStream(ForexSymbolPartitionMixin, PriceVolumeStream):
    """Stream for Historical Forex Full Chart API."""

    name = "forex_full_chart"

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/historical-price-eod/full"


class Forex1minStream(ForexSymbolPartitionMixin, Prices1minStream):
    """Stream for 1-Minute Interval Forex Chart API."""

    name = "forex_1min"

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/historical-chart/1min"


class Forex5minStream(ForexSymbolPartitionMixin, Prices5minStream):
    """Stream for 5-Minute Interval Forex Chart API."""

    name = "forex_5min"

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/historical-chart/5min"


class Forex1HrStream(ForexSymbolPartitionMixin, Prices1HrStream):
    """Stream for 1-Hour Interval Forex Chart API."""

    name = "forex_1hr"

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/historical-chart/1hour"