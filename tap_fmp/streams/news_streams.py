"""News stream types classes for tap-fmp."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.helpers.types import Context
from tap_fmp.client import (
    FmpRestStream,
    TimeSliceStream,
    SymbolPartitionTimeSliceStream,
)


class BaseNewsTimeSliceStream(TimeSliceStream):
    replication_key = "published_date"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True
    primary_keys = ["surrogate_key"]
    _paginate = True
    _add_surrogate_key = True
    _max_pages = 100

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("published_date", th.DateTimeType),
        th.Property("publisher", th.StringType),
        th.Property("title", th.StringType),
        th.Property("image", th.StringType),
        th.Property("site", th.StringType),
        th.Property("text", th.StringType),
        th.Property("url", th.StringType),
    ).to_dict()


class BaseSearchNewsSymbolPartitionStream(
    SymbolPartitionTimeSliceStream, TimeSliceStream
):
    replication_key = "published_date"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True
    primary_keys = ["surrogate_key"]
    _paginate = True
    _add_surrogate_key = True
    _max_pages = 100

    def get_records(self, context: Context | None):
        self.query_params.update(context)
        return super().get_records(context)


class FmpArticlesStream(FmpRestStream):
    """Stream for FMP Articles API."""

    name = "fmp_articles"
    _paginate = True
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("title", th.StringType),
        th.Property("date", th.DateTimeType),
        th.Property("content", th.StringType),
        th.Property("tickers", th.StringType),
        th.Property("image", th.StringType),
        th.Property("link", th.StringType),
        th.Property("author", th.StringType),
        th.Property("site", th.StringType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/fmp-articles"


class GeneralNewsStream(BaseNewsTimeSliceStream):
    """Stream for General News API."""

    name = "general_news"

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/news/general-latest"


class PressReleasesStream(BaseNewsTimeSliceStream):
    """Stream for Press Releases API."""

    name = "press_releases"

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/news/press-releases-latest"


class StockNewsStream(BaseNewsTimeSliceStream):
    """Stream for Stock News API."""

    name = "stock_news"

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/news/stock-latest"


class CryptoNewsStream(BaseNewsTimeSliceStream):
    """Stream for Crypto News API."""

    name = "crypto_news"

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/news/crypto-latest"


class ForexNewsStream(BaseNewsTimeSliceStream):
    """Stream for Forex News API."""

    name = "forex_news"

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/news/forex-latest"


class SearchPressReleasesStream(BaseSearchNewsSymbolPartitionStream):
    """Stream for Search Press Releases API."""

    name = "search_press_releases"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("published_date", th.DateTimeType),
        th.Property("publisher", th.StringType),
        th.Property("title", th.StringType),
        th.Property("image", th.StringType),
        th.Property("site", th.StringType),
        th.Property("text", th.StringType),
        th.Property("url", th.StringType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/news/press-releases"


class SearchStockNewsStream(BaseSearchNewsSymbolPartitionStream):
    """Stream for Search Stock News API."""

    name = "search_stock_news"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("published_date", th.DateTimeType),
        th.Property("publisher", th.StringType),
        th.Property("title", th.StringType),
        th.Property("image", th.StringType),
        th.Property("site", th.StringType),
        th.Property("text", th.StringType),
        th.Property("url", th.StringType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/news/stock"


class SearchCryptoNewsStream(BaseSearchNewsSymbolPartitionStream):
    """Stream for Search Crypto News API."""

    name = "search_crypto_news"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("published_date", th.DateTimeType),
        th.Property("publisher", th.StringType),
        th.Property("title", th.StringType),
        th.Property("image", th.StringType),
        th.Property("site", th.StringType),
        th.Property("text", th.StringType),
        th.Property("url", th.StringType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/news/crypto"


class SearchForexNewsStream(BaseSearchNewsSymbolPartitionStream):
    """Stream for Search Forex News API."""

    name = "search_forex_news"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("published_date", th.DateTimeType),
        th.Property("publisher", th.StringType),
        th.Property("title", th.StringType),
        th.Property("image", th.StringType),
        th.Property("site", th.StringType),
        th.Property("text", th.StringType),
        th.Property("url", th.StringType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/news/forex"
