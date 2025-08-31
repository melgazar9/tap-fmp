"""News stream types classes for tap-fmp."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_fmp.client import FmpSurrogateKeyStream, TimeSliceStream
from tap_fmp.mixins import (
    BatchSymbolPartitionMixin,
    CompanyBatchStreamMixin,
    CryptoConfigMixin,
    ForexConfigMixin,
)


class BaseNewsTimeSliceStream(TimeSliceStream, FmpSurrogateKeyStream):
    replication_key = "replication_date"
    replication_method = "INCREMENTAL"
    is_timestamp_replication_key = True
    _paginate = True
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
        th.Property("replication_date", th.DateType, required=True),
    ).to_dict()

    def post_process(self, record: dict, context: Context | None = None) -> dict:
        """
        Inject replication_date key from the API request parameter. The replication_key is published_date, but if
        published_date comes through null or as an empty string we set the replication date to the 'from' parameter.
        """
        if "published_date" in record and len(record["published_date"]):
            record["replication_date"] = record["published_date"]
        elif "from" in self.query_params:
            record["replication_date"] = self.query_params["from"]
        return super().post_process(record, context)


class FmpArticlesStream(FmpSurrogateKeyStream):
    """Stream for FMP Articles API."""

    name = "fmp_articles"
    _paginate = True

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


class PressReleasesLatestStream(BaseNewsTimeSliceStream):
    """Stream for Press Releases Latest API."""

    name = "press_releases_latest"

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/news/press-releases-latest"


class PressReleasesStream(
    BatchSymbolPartitionMixin, CompanyBatchStreamMixin, BaseNewsTimeSliceStream
):
    """Stream for Press Releases API."""

    name = "press_releases"

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/news/press-releases"


class StockNewsLatestStream(BaseNewsTimeSliceStream):
    """Stream for Stock News Latest API."""

    name = "stock_news_latest"

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/news/stock-latest"


class CryptoNewsLatestStream(BaseNewsTimeSliceStream):
    """Stream for Crypto News Latest API."""

    name = "crypto_news_latest"

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/news/crypto-latest"


class ForexNewsLatestStream(BaseNewsTimeSliceStream):
    """Stream for Forex News Latest API."""

    name = "forex_news_latest"

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/news/forex-latest"


class StockNewsStream(
    BatchSymbolPartitionMixin, CompanyBatchStreamMixin, BaseNewsTimeSliceStream
):
    """Stream for Stock News API."""

    name = "stock_news"

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/news/stock"


class CryptoNewsStream(
    BatchSymbolPartitionMixin, CryptoConfigMixin, BaseNewsTimeSliceStream
):
    """Stream for Crypto News API."""

    name = "crypto_news"

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/news/crypto"


class ForexNewsStream(
    BatchSymbolPartitionMixin, ForexConfigMixin, BaseNewsTimeSliceStream
):
    """Stream for Forex News API."""

    name = "forex_news"

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/news/forex"
