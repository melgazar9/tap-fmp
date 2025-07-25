"""Crypto stream types classes for tap-fmp."""

from __future__ import annotations

import typing as t
from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_fmp.client import FmpRestStream, SymbolPartitionStream, SymbolPartitionTimeSliceStream
from tap_fmp.streams.chart_streams import Prices1minStream, Prices5minStream, Prices1HrStream


class CryptoListStream(FmpRestStream):
    """Stream for Crypto List API."""
    
    name = "crypto_list"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True
    
    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("name", th.StringType),
        th.Property("exchange", th.StringType),
        th.Property("ico_date", th.StringType),
        th.Property("circulating_supply", th.NumberType),
        th.Property("total_supply", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        """Get URL for the request."""
        return f"{self.url_base}/stable/cryptocurrency-list"


class CryptoSymbolPartitionMixin(SymbolPartitionStream):
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
            return [{"symbol": c.get("symbol")} for c in self._tap.get_cached_crypto_symbols()]


class FullCryptoQuoteStream(CryptoSymbolPartitionMixin):
    """Stream for Full Crypto Quote API."""
    
    name = "crypto_quotes"
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

    def get_url(self, context: Context | None = None) -> str:
        """Get URL for the request."""
        return f"{self.url_base}/stable/quote"


class CryptoQuoteShortStream(CryptoSymbolPartitionMixin):
    """Stream for Crypto Quote Short API."""
    
    name = "crypto_quotes_short"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True
    
    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("price", th.NumberType),
        th.Property("change", th.NumberType),
        th.Property("volume", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        """Get URL for the request."""
        return f"{self.url_base}/stable/quote-short"


class AllCryptoQuotesStream(FmpRestStream):
    """Stream for All Crypto Quotes API."""
    
    name = "all_crypto_quotes"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True
    
    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("price", th.NumberType),
        th.Property("change", th.NumberType),
        th.Property("volume", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        """Get URL for the request."""
        return f"{self.url_base}/stable/batch-crypto-quotes"


class HistoricalCryptoLightChartStream(CryptoSymbolPartitionMixin, SymbolPartitionTimeSliceStream):
    """Stream for Historical Crypto Light Chart API."""
    
    name = "historical_crypto_light_chart"
    primary_keys = ["symbol", "date"]
    
    schema = th.PropertiesList(
        th.Property("symbol", th.StringType, required=True),
        th.Property("date", th.DateType),
        th.Property("price", th.NumberType),
        th.Property("volume", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        """Get URL for the request."""
        return f"{self.url_base}/stable/historical-price-eod/light"


class HistoricalCryptoFullChartStream(CryptoSymbolPartitionMixin, SymbolPartitionTimeSliceStream):
    """Stream for Historical Crypto Full Chart API."""
    
    name = "historical_crypto_full_chart"
    primary_keys = ["symbol", "date"]
    
    schema = th.PropertiesList(
        th.Property("symbol", th.StringType, required=True),
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

    def get_url(self, context: Context | None = None) -> str:
        """Get URL for the request."""
        return f"{self.url_base}/stable/historical-price-eod/full"


class Crypto1minStream(CryptoSymbolPartitionMixin, Prices1minStream):
    """Stream for 1-Minute Interval Crypto Data API."""
    name = "crypto_1min"


class Crypto5minStream(CryptoSymbolPartitionMixin, Prices5minStream):
    """Stream for 5-Minute Interval Crypto Data API."""
    name = "crypto_5min"


class Crypto1HrStream(CryptoSymbolPartitionMixin, Prices1HrStream):
    """Stream for 1-Hour Interval Crypto Data API."""
    name = "crypto_1hr"