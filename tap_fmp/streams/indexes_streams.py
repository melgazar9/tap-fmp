"""Index streams."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_fmp.client import (
    FmpRestStream,
    SymbolPartitionStream,
    SymbolPartitionTimeSliceStream,
)


class IndexListStream(FmpRestStream):
    """Stock Market Indexes List API - Comprehensive list of stock market indexes."""

    name = "index_list"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("name", th.StringType),
        th.Property("exchange", th.StringType),
        th.Property("currency", th.StringType),
    ).to_dict()

    def get_url(self, context: Context | None) -> str:
        return f"{self.url_base}/stable/index-list"


class IndexQuoteStream(SymbolPartitionStream):
    """Index Quote API - Real-time stock index quotes."""

    name = "index_quote"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("name", th.StringType),
        th.Property("price", th.NumberType),
        th.Property("change_percentage", th.NumberType),
        th.Property("change", th.NumberType),
        th.Property("volume", th.NumberType),
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

    def get_url(self, context: Context | None) -> str:
        return f"{self.url_base}/stable/quote"


class IndexShortQuoteStream(SymbolPartitionStream):
    """Index Short Quote API - Concise stock index quotes."""

    name = "index_short_quote"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("price", th.NumberType),
        th.Property("change", th.NumberType),
        th.Property("volume", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context | None) -> str:
        return f"{self.url_base}/stable/quote-short"


class AllIndexQuotesStream(FmpRestStream):
    """All Index Quotes API - Real-time quotes for a wide range of stock indexes."""

    name = "all_index_quotes"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("price", th.NumberType),
        th.Property("change", th.NumberType),
        th.Property("volume", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context | None) -> str:
        return f"{self.url_base}/stable/batch-index-quotes"


class HistoricalIndexLightChartStream(SymbolPartitionTimeSliceStream):
    """Historical Index Light Chart API - End-of-day historical prices for stock indexes."""

    name = "historical_index_light_chart"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("date", th.DateType),
        th.Property("price", th.NumberType),
        th.Property("volume", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context | None) -> str:
        return f"{self.url_base}/stable/historical-price-eod/light"


class HistoricalIndexFullChartStream(SymbolPartitionTimeSliceStream):
    """Historical Index Full Chart API - Full historical end-of-day prices for stock indexes."""

    name = "historical_index_full_chart"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("date", th.DateType),
        th.Property("open", th.NumberType),
        th.Property("high", th.NumberType),
        th.Property("low", th.NumberType),
        th.Property("close", th.NumberType),
        th.Property("volume", th.NumberType),
        th.Property("change", th.NumberType),
        th.Property("change_percent", th.NumberType),
        th.Property("vwap", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context | None) -> str:
        return f"{self.url_base}/stable/historical-price-eod/full"


class Index1MinuteIntervalStream(SymbolPartitionTimeSliceStream):
    """1-Minute Interval Index Price API - 1-minute interval intraday data for stock indexes."""

    name = "index_1min"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("date", th.DateTimeType),
        th.Property("open", th.NumberType),
        th.Property("low", th.NumberType),
        th.Property("high", th.NumberType),
        th.Property("close", th.NumberType),
        th.Property("volume", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context | None) -> str:
        return f"{self.url_base}/stable/historical-chart/1min"


class Index5MinuteIntervalStream(Index1MinuteIntervalStream):
    """5-Minute Interval Index Price API - 5-minute interval intraday data for stock indexes."""

    name = "index_5min"

    def get_url(self, context: Context | None) -> str:
        return f"{self.url_base}/stable/historical-chart/5min"


class Index1HourIntervalStream(Index1MinuteIntervalStream):
    """1-Hour Interval Index Price API - 1-hour interval intraday data for stock indexes."""

    name = "index_1hr"

    def get_url(self, context: Context | None) -> str:
        return f"{self.url_base}/stable/historical-chart/1hour"


class SP500ConstituentStream(FmpRestStream):
    """S&P 500 Index API - Detailed data on the S&P 500 index companies."""

    name = "sp500_constituent"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("name", th.StringType),
        th.Property("sector", th.StringType),
        th.Property("sub_sector", th.StringType),
        th.Property("head_quarter", th.StringType),
        th.Property("date_first_added", th.DateType),
        th.Property("cik", th.StringType),
        th.Property("founded", th.StringType),
    ).to_dict()

    def get_url(self, context: Context | None) -> str:
        return f"{self.url_base}/stable/sp500-constituent"


class NasdaqConstituentStream(SP500ConstituentStream):
    """Nasdaq Index API - Comprehensive data for the Nasdaq index companies."""

    name = "nasdaq_constituent"

    def get_url(self, context: Context | None) -> str:
        return f"{self.url_base}/stable/nasdaq-constituent"


class DowJonesConstituentStream(SP500ConstituentStream):
    """Dow Jones API - Data on the Dow Jones Industrial Average companies."""

    name = "dow_jones_constituent"

    def get_url(self, context: Context | None) -> str:
        return f"{self.url_base}/stable/dowjones-constituent"


class HistoricalSP500ConstituentStream(FmpRestStream):
    """Historical S&P 500 API - Historical data for the S&P 500 index changes."""

    name = "historical_sp500_constituent"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("date", th.DateType),
        th.Property("date_added", th.StringType),
        th.Property("added_security", th.StringType),
        th.Property("removed_ticker", th.StringType),
        th.Property("removed_security", th.StringType),
        th.Property("reason", th.StringType),
    ).to_dict()

    def get_url(self, context: Context | None) -> str:
        return f"{self.url_base}/stable/historical-sp500-constituent"


class HistoricalNasdaqConstituentStream(HistoricalSP500ConstituentStream):
    """Historical Nasdaq API - Historical data for the Nasdaq index changes."""

    name = "historical_nasdaq_constituent"

    def get_url(self, context: Context | None) -> str:
        return f"{self.url_base}/stable/historical-nasdaq-constituent"


class HistoricalDowJonesConstituentStream(HistoricalSP500ConstituentStream):
    """Historical Dow Jones API - Historical data for the Dow Jones Industrial Average changes."""

    name = "historical_dow_jones_constituent"

    def get_url(self, context: Context | None) -> str:
        return f"{self.url_base}/stable/historical-dowjones-constituent"
