from tap_fmp.client import SymbolPartitionTimeSliceStream
from singer_sdk.helpers.types import Context
from singer_sdk import typing as th


class ChartLightStream(SymbolPartitionTimeSliceStream):
    name = "chart_light"
    primary_keys = ["symbol", "date"]

    schema = th.PropertiesList(
        th.Property("symbol", th.StringType, required=True),
        th.Property("date", th.DateType),
        th.Property("price", th.NumberType),
        th.Property("volume", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/historical-price-eod/light"


class PriceVolumeStream(SymbolPartitionTimeSliceStream):
    name = "chart_full"
    primary_keys = ["symbol", "date", "open", "high", "low", "close", "volume"]

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

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/historical-price-eod/full"


class UnadjustedPriceStream(SymbolPartitionTimeSliceStream):
    name = "unadjusted_price"
    primary_keys = [
        "symbol",
        "date",
        "adj_open",
        "adj_high",
        "adj_low",
        "adj_close",
        "volume",
    ]

    schema = th.PropertiesList(
        th.Property("symbol", th.StringType, required=True),
        th.Property("date", th.DateType),
        th.Property("adj_open", th.NumberType),
        th.Property("adj_high", th.NumberType),
        th.Property("adj_low", th.NumberType),
        th.Property("adj_close", th.NumberType),
        th.Property("volume", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/historical-price-eod/non-split-adjusted"


class DividendAdjustedPriceStream(SymbolPartitionTimeSliceStream):
    name = "dividend_adjusted_prices"
    primary_keys = [
        "symbol",
        "date",
        "adj_open",
        "adj_high",
        "adj_low",
        "adj_close",
        "volume",
    ]

    schema = th.PropertiesList(
        th.Property("symbol", th.StringType, required=True),
        th.Property("date", th.DateType),
        th.Property("adj_open", th.NumberType),
        th.Property("adj_high", th.NumberType),
        th.Property("adj_low", th.NumberType),
        th.Property("adj_close", th.NumberType),
        th.Property("volume", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/historical-price-eod/dividend-adjusted"


class Prices1minStream(SymbolPartitionTimeSliceStream):
    name = "prices_1min"
    primary_keys = ["symbol", "date"]
    replication_key = "date"

    schema = th.PropertiesList(
        th.Property("symbol", th.StringType, required=True),
        th.Property("date", th.DateTimeType),
        th.Property("open", th.NumberType),
        th.Property("high", th.NumberType),
        th.Property("low", th.NumberType),
        th.Property("close", th.NumberType),
        th.Property("volume", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/historical-chart/1min"


class Prices5minStream(Prices1minStream):
    name = "prices_5min"

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/historical-chart/5min"


class Prices15minStream(Prices1minStream):
    name = "prices_15min"

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/historical-chart/15min"


class Prices30minStream(Prices1minStream):
    name = "prices_30min"

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/historical-chart/30min"


class Prices1HrStream(Prices1minStream):
    name = "prices_1h"

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/historical-chart/1hour"


class Prices4HrStream(Prices1minStream):
    name = "prices_4h"

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/historical-chart/4hour"
