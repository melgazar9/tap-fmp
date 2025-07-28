"""Technical Indicators stream types classes for tap-fmp."""

from __future__ import annotations

from singer_sdk import typing as th

from singer_sdk.helpers.types import Context

from tap_fmp.client import SymbolPartitionTimeSliceStream


class BaseTechnicalIndicatorStream(SymbolPartitionTimeSliceStream):
    """Base class for technical indicator streams."""

    primary_keys = ["symbol", "date"]
    replication_key = "date"

    schema = th.PropertiesList(
        th.Property("symbol", th.StringType, required=True),
        th.Property("date", th.DateTimeType, required=True),
        th.Property("open", th.NumberType),
        th.Property("high", th.NumberType),
        th.Property("low", th.NumberType),
        th.Property("close", th.NumberType),
        th.Property("volume", th.IntegerType),
    ).to_dict()

    @property
    def partitions(self):
        query_params = self.query_params
        other_params = self.config.get(self.name, {}).get("other_params", {})

        query_symbol = query_params.get("symbol")
        query_timeframe = query_params.get("timeframe")
        query_period_length = query_params.get("periodLength")

        other_symbols = other_params.get("symbols")
        other_timeframes = other_params.get("timeframes")
        other_period_lengths = other_params.get("period_lengths")

        if query_symbol:
            symbols = [query_symbol] if isinstance(query_symbol, str) else query_symbol
        elif other_symbols:
            symbols = other_symbols
        else:
            symbols = [c.get("symbol") for c in self._tap.get_cached_symbols()]

        timeframes = (
            [query_timeframe]
            if query_timeframe
            else other_timeframes
        )

        period_lengths = (
            [query_period_length]
            if query_period_length
            else (other_period_lengths if other_period_lengths else [None])
        )

        if not timeframes:
            timeframes = ["1min", "5min", "15min", "30min", "1hour", "4hour", "1day"]
        if not period_lengths:
            period_lengths = [10]

        partitions = []
        for symbol in symbols:
            for timeframe in timeframes:
                for period_length in period_lengths:
                    partition = {"symbol": symbol}
                    if timeframe:
                        partition["timeframe"] = timeframe
                    if period_length:
                        partition["periodLength"] = period_length
                    partitions.append(partition)
        return partitions

    def get_records(self, context: Context | None):
        self.query_params.update(context)
        return super().get_records(context)


class SimpleMovingAverageStream(BaseTechnicalIndicatorStream):
    """Stream for Simple Moving Average API."""

    name = "simple_moving_average"

    schema = th.PropertiesList(
        th.Property("symbol", th.StringType, required=True),
        th.Property("date", th.DateTimeType, required=True),
        th.Property("open", th.NumberType),
        th.Property("high", th.NumberType),
        th.Property("low", th.NumberType),
        th.Property("close", th.NumberType),
        th.Property("volume", th.IntegerType),
        th.Property("sma", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/technical-indicators/sma"


class ExponentialMovingAverageStream(BaseTechnicalIndicatorStream):
    """Stream for Exponential Moving Average API."""

    name = "exponential_moving_average"

    schema = th.PropertiesList(
        th.Property("symbol", th.StringType, required=True),
        th.Property("date", th.DateTimeType, required=True),
        th.Property("open", th.NumberType),
        th.Property("high", th.NumberType),
        th.Property("low", th.NumberType),
        th.Property("close", th.NumberType),
        th.Property("volume", th.IntegerType),
        th.Property("ema", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/technical-indicators/ema"


class WeightedMovingAverageStream(BaseTechnicalIndicatorStream):
    """Stream for Weighted Moving Average API."""

    name = "weighted_moving_average"

    schema = th.PropertiesList(
        th.Property("symbol", th.StringType, required=True),
        th.Property("date", th.DateTimeType, required=True),
        th.Property("open", th.NumberType),
        th.Property("high", th.NumberType),
        th.Property("low", th.NumberType),
        th.Property("close", th.NumberType),
        th.Property("volume", th.IntegerType),
        th.Property("wma", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/technical-indicators/wma"


class DoubleExponentialMovingAverageStream(BaseTechnicalIndicatorStream):
    """Stream for Double Exponential Moving Average API."""

    name = "double_exponential_moving_average"

    schema = th.PropertiesList(
        th.Property("symbol", th.StringType, required=True),
        th.Property("date", th.DateTimeType, required=True),
        th.Property("open", th.NumberType),
        th.Property("high", th.NumberType),
        th.Property("low", th.NumberType),
        th.Property("close", th.NumberType),
        th.Property("volume", th.IntegerType),
        th.Property("dema", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/technical-indicators/dema"


class TripleExponentialMovingAverageStream(BaseTechnicalIndicatorStream):
    """Stream for Triple Exponential Moving Average API."""

    name = "triple_exponential_moving_average"

    schema = th.PropertiesList(
        th.Property("symbol", th.StringType, required=True),
        th.Property("date", th.DateTimeType, required=True),
        th.Property("open", th.NumberType),
        th.Property("high", th.NumberType),
        th.Property("low", th.NumberType),
        th.Property("close", th.NumberType),
        th.Property("volume", th.IntegerType),
        th.Property("tema", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/technical-indicators/tema"


class RelativeStrengthIndexStream(BaseTechnicalIndicatorStream):
    """Stream for Relative Strength Index API."""

    name = "relative_strength_index"

    schema = th.PropertiesList(
        th.Property("symbol", th.StringType, required=True),
        th.Property("date", th.DateTimeType, required=True),
        th.Property("open", th.NumberType),
        th.Property("high", th.NumberType),
        th.Property("low", th.NumberType),
        th.Property("close", th.NumberType),
        th.Property("volume", th.IntegerType),
        th.Property("rsi", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/technical-indicators/rsi"


class StandardDeviationStream(BaseTechnicalIndicatorStream):
    """Stream for Standard Deviation API."""

    name = "standard_deviation"

    schema = th.PropertiesList(
        th.Property("symbol", th.StringType, required=True),
        th.Property("date", th.DateTimeType, required=True),
        th.Property("open", th.NumberType),
        th.Property("high", th.NumberType),
        th.Property("low", th.NumberType),
        th.Property("close", th.NumberType),
        th.Property("volume", th.IntegerType),
        th.Property("standard_deviation", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/technical-indicators/standarddeviation"


class WilliamsStream(BaseTechnicalIndicatorStream):
    """Stream for Williams %R API."""

    name = "williams"

    schema = th.PropertiesList(
        th.Property("symbol", th.StringType, required=True),
        th.Property("date", th.DateTimeType, required=True),
        th.Property("open", th.NumberType),
        th.Property("high", th.NumberType),
        th.Property("low", th.NumberType),
        th.Property("close", th.NumberType),
        th.Property("volume", th.IntegerType),
        th.Property("williams", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/technical-indicators/williams"


class AverageDirectionalIndexStream(BaseTechnicalIndicatorStream):
    """Stream for Average Directional Index API."""

    name = "average_directional_index"

    schema = th.PropertiesList(
        th.Property("symbol", th.StringType, required=True),
        th.Property("date", th.DateTimeType, required=True),
        th.Property("open", th.NumberType),
        th.Property("high", th.NumberType),
        th.Property("low", th.NumberType),
        th.Property("close", th.NumberType),
        th.Property("volume", th.IntegerType),
        th.Property("adx", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/technical-indicators/adx"
