"""Market Performance stream types classes for tap-fmp."""

from __future__ import annotations

import typing as t

from singer_sdk import typing as th
from singer_sdk.helpers.types import Context
from tap_fmp.client import FmpRestStream, TimeSliceStream, IncrementalDateStream


class HistoricalMarketPerformanceStream(TimeSliceStream):
    """Stream for Historical Market Performance API."""

    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        self.query_params.update(context)
        return super().get_records(context)


class MarketSectorPerformanceSnapshotStream(IncrementalDateStream):
    """Stream for Market Sector Performance Snapshot API."""

    name = "market_sector_performance_snapshot"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("date", th.DateType),
        th.Property("sector", th.StringType),
        th.Property("exchange", th.StringType),
        th.Property("average_change", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/sector-performance-snapshot"


class IndustryPerformanceSnapshotStream(IncrementalDateStream):
    """Stream for Industry Performance Snapshot API."""

    name = "industry_performance_snapshot"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("date", th.DateType),
        th.Property("industry", th.StringType),
        th.Property("exchange", th.StringType),
        th.Property("average_change", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/industry-performance-snapshot"


class HistoricalSectorPerformanceStream(HistoricalMarketPerformanceStream):
    """Stream for Historical Market Sector Performance API."""

    name = "historical_sector_performance"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("date", th.DateType),
        th.Property("sector", th.StringType),
        th.Property("exchange", th.StringType),
        th.Property("average_change", th.NumberType),
    ).to_dict()

    @property
    def partitions(self):
        return self._tap.get_cached_sectors()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/historical-sector-performance"


class HistoricalIndustryPerformanceStream(HistoricalMarketPerformanceStream):
    """Stream for Historical Industry Performance API."""

    name = "historical_industry_performance"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("date", th.DateType),
        th.Property("industry", th.StringType),
        th.Property("exchange", th.StringType),
        th.Property("average_change", th.NumberType),
    ).to_dict()

    @property
    def partitions(self):
        return self._tap.get_cached_industries()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/historical-industry-performance"


class SectorPeSnapshotStream(IncrementalDateStream):
    """Stream for Sector PE Snapshot API."""

    name = "sector_pe_snapshot"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("date", th.DateType),
        th.Property("sector", th.StringType),
        th.Property("exchange", th.StringType),
        th.Property("pe", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/sector-pe-snapshot"


class IndustryPeSnapshotStream(IncrementalDateStream):
    """Stream for Industry PE Snapshot API."""

    name = "industry_pe_snapshot"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("date", th.DateType),
        th.Property("industry", th.StringType),
        th.Property("exchange", th.StringType),
        th.Property("pe", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/industry-pe-snapshot"


class HistoricalSectorPeStream(HistoricalMarketPerformanceStream):
    """Stream for Historical Sector PE API."""

    name = "historical_sector_pe"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("date", th.DateType),
        th.Property("sector", th.StringType),
        th.Property("exchange", th.StringType),
        th.Property("pe", th.NumberType),
    ).to_dict()

    @property
    def partitions(self):
        return self._tap.get_cached_sectors()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/historical-sector-pe"


class HistoricalIndustryPeStream(HistoricalMarketPerformanceStream):
    """Stream for Historical Industry PE API."""

    name = "historical_industry_pe"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("date", th.DateType),
        th.Property("industry", th.StringType),
        th.Property("exchange", th.StringType),
        th.Property("pe", th.NumberType),
    ).to_dict()

    @property
    def partitions(self):
        return self._tap.get_cached_industries()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/historical-industry-pe"


class BiggestStockGainersStream(FmpRestStream):
    """Stream for Biggest Stock Gainers API."""

    name = "biggest_stock_gainers"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("price", th.NumberType),
        th.Property("name", th.StringType),
        th.Property("change", th.NumberType),
        th.Property("changes_percentage", th.NumberType),
        th.Property("exchange", th.StringType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/biggest-gainers"


class BiggestStockLosersStream(FmpRestStream):
    """Stream for Biggest Stock Losers API."""

    name = "biggest_stock_losers"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("price", th.NumberType),
        th.Property("name", th.StringType),
        th.Property("change", th.NumberType),
        th.Property("changes_percentage", th.NumberType),
        th.Property("exchange", th.StringType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/biggest-losers"


class TopTradedStocksStream(FmpRestStream):
    """Stream for Top Traded Stocks API."""

    name = "top_traded_stocks"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("price", th.NumberType),
        th.Property("name", th.StringType),
        th.Property("change", th.NumberType),
        th.Property("changes_percentage", th.NumberType),
        th.Property("exchange", th.StringType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/most-actives"
