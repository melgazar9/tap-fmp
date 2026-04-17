"""Crypto stream types classes for tap-fmp."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_fmp.client import (
    FmpSurrogateKeyStream,
    BaseSymbolPartitionStream,
    BaseSymbolPartitionTimeSliceStream,
)
from tap_fmp.mixins import (
    BaseSymbolPartitionMixin,
    ChartFullMixin,
    ChartLightMixin,
    CryptoConfigMixin,
    Prices1HrMixin,
    Prices1minMixin,
    Prices5minMixin,
)


class CryptoListStream(CryptoConfigMixin, FmpSurrogateKeyStream):
    """Stream for Crypto List API."""

    name = "crypto_list"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("name", th.StringType),
        th.Property("exchange", th.StringType),
        th.Property("ico_date", th.StringType),
        th.Property("circulating_supply", th.NumberType),
        th.Property("total_supply", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/cryptocurrency-list"


class CryptoSymbolPartitionMixin(BaseSymbolPartitionMixin):

    @property
    def selection_config_section(self) -> str:
        return "crypto_symbols"

    @property
    def selection_field_name(self) -> str:
        return "select_crypto_symbols"

    def _partition_symbols(self) -> list[dict]:
        return self._tap.get_cached_crypto_symbols()


class FullCryptoQuoteStream(CryptoSymbolPartitionMixin, BaseSymbolPartitionStream):
    """Stream for Full Crypto Quote API."""

    name = "crypto_quotes"

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


class CryptoQuoteShortStream(CryptoSymbolPartitionMixin, BaseSymbolPartitionStream):
    """Stream for Crypto Quote Short API."""

    name = "crypto_quotes_short"

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


class AllCryptoQuotesStream(FmpSurrogateKeyStream):
    """Stream for All Crypto Quotes API."""

    name = "all_crypto_quotes"

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


class HistoricalCryptoLightChartStream(
    CryptoSymbolPartitionMixin,
    ChartLightMixin,
    BaseSymbolPartitionTimeSliceStream,
):
    """Stream for Historical Crypto Light Chart API."""

    name = "historical_crypto_light_chart"

    def get_url(self, context: Context | None = None) -> str:
        """Get URL for the request."""
        return f"{self.url_base}/stable/historical-price-eod/light"


class HistoricalCryptoFullChartStream(
    CryptoSymbolPartitionMixin, ChartFullMixin, BaseSymbolPartitionTimeSliceStream
):
    """Stream for Historical Crypto Full Chart API."""

    name = "historical_crypto_full_chart"

    def get_url(self, context: Context | None = None) -> str:
        """Get URL for the request."""
        return f"{self.url_base}/stable/historical-price-eod/full"


class Crypto1minStream(
    CryptoSymbolPartitionMixin,
    Prices1minMixin,
    BaseSymbolPartitionTimeSliceStream,
):
    """Stream for 1-Minute Interval Crypto Data API."""

    name = "crypto_1min"


class Crypto5minStream(
    CryptoSymbolPartitionMixin,
    Prices5minMixin,
    BaseSymbolPartitionTimeSliceStream,
):
    """Stream for 5-Minute Interval Crypto Data API."""

    name = "crypto_5min"


class Crypto1HrStream(
    CryptoSymbolPartitionMixin,
    Prices1HrMixin,
    BaseSymbolPartitionTimeSliceStream,
):
    """Stream for 1-Hour Interval Crypto Data API."""

    name = "crypto_1h"
