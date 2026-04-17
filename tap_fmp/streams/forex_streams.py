"""Forex stream types classes for tap-fmp."""

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
    ForexConfigMixin,
    ChartLightMixin,
    ChartFullMixin,
    Prices1minMixin,
    Prices5minMixin,
    Prices1HrMixin,
)


class ForexPairsStream(ForexConfigMixin, FmpSurrogateKeyStream):
    name = "forex_pairs"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("from_currency", th.StringType),
        th.Property("to_currency", th.StringType),
        th.Property("from_name", th.StringType),
        th.Property("to_name", th.StringType),
    ).to_dict()

    def create_record_from_item(self, item: str) -> dict:
        """Create a record dict from a forex symbol.

        Parses standard 6-char ISO pairs (e.g., "EURUSD" -> EUR, USD) or
        dot-separated pairs (e.g., "EUR.USD"). Falls back to None when the
        symbol does not match a known pattern.
        """
        from_ccy = to_ccy = None
        if "." in item:
            parts = item.split(".", 1)
            if len(parts) == 2 and len(parts[0]) == 3 and len(parts[1]) == 3:
                from_ccy, to_ccy = parts[0].upper(), parts[1].upper()
        elif len(item) == 6 and item.isalpha():
            from_ccy, to_ccy = item[:3].upper(), item[3:].upper()
        return {
            "symbol": item,
            "from_currency": from_ccy,
            "to_currency": to_ccy,
            "from_name": None,
            "to_name": None,
        }

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/forex-list"


class ForexSymbolPartitionMixin(BaseSymbolPartitionMixin):

    @property
    def selection_config_section(self) -> str:
        return "forex_pairs"

    @property
    def selection_field_name(self) -> str:
        return "select_forex_pairs"

    def _partition_symbols(self) -> list[dict]:
        return self._tap.get_cached_forex_pairs()


class ForexQuoteStream(ForexSymbolPartitionMixin, BaseSymbolPartitionStream):
    """Stream for Forex Quote API."""

    name = "forex_quotes"

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


class ForexQuoteShortStream(ForexSymbolPartitionMixin, BaseSymbolPartitionStream):
    """Stream for Forex Short Quote API."""

    name = "forex_quotes_short"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("price", th.NumberType),
        th.Property("change", th.NumberType),
        th.Property("volume", th.IntegerType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/quote-short"


class BatchForexQuotesStream(FmpSurrogateKeyStream):
    """Stream for Batch Forex Quotes API."""

    name = "batch_forex_quotes"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("price", th.NumberType),
        th.Property("change", th.NumberType),
        th.Property("volume", th.IntegerType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/batch-forex-quotes"


class ForexLightChartStream(
    ForexSymbolPartitionMixin,
    ChartLightMixin,
    BaseSymbolPartitionTimeSliceStream,
):
    """Stream for Historical Forex Light Chart API."""

    name = "forex_light_chart"

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/historical-price-eod/light"


class ForexFullChartStream(
    ForexSymbolPartitionMixin, ChartFullMixin, BaseSymbolPartitionTimeSliceStream
):
    """Stream for Historical Forex Full Chart API."""

    name = "forex_full_chart"


class Forex1minStream(
    ForexSymbolPartitionMixin,
    Prices1minMixin,
    BaseSymbolPartitionTimeSliceStream,
):
    """Stream for 1-Minute Interval Forex Chart API."""

    name = "forex_1min"


class Forex5minStream(
    ForexSymbolPartitionMixin,
    Prices5minMixin,
    BaseSymbolPartitionTimeSliceStream,
):
    """Stream for 5-Minute Interval Forex Chart API."""

    name = "forex_5min"


class Forex1HrStream(
    ForexSymbolPartitionMixin,
    Prices1HrMixin,
    BaseSymbolPartitionTimeSliceStream,
):
    """Stream for 1-Hour Interval Forex Chart API."""

    name = "forex_1h"
