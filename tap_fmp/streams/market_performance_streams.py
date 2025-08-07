"""Market Performance stream types classes for tap-fmp."""

from __future__ import annotations

import typing as t

from singer_sdk import typing as th
from singer_sdk.exceptions import ConfigValidationError
from singer_sdk.helpers.types import Context
from datetime import datetime, timedelta

from tap_fmp.client import FmpRestStream, TimeSliceStream


class PerformanceSnapshotStream(FmpRestStream):
    primary_keys = ["surrogate_key"]
    replication_key = "date"
    replication_method = "INCREMENTAL"
    _add_surrogate_key = True

    def _format_replication_key(self, replication_key_value):
        if isinstance(replication_key_value, str):
            try:
                # Try parsing as date string and return as YYYY-MM-DD
                if 'T' in replication_key_value or 'Z' in replication_key_value:
                    # ISO format with time
                    output_value = datetime.fromisoformat(replication_key_value.replace('Z', '+00:00')).strftime("%Y-%m-%d")
                else:
                    # Already in YYYY-MM-DD format
                    output_value = replication_key_value
                return output_value
            except ValueError:
                pass
        raise ValueError(f"Could not format replication key for stream {self.name}")

    def _get_dates_dict(self):
        if "date" in self.query_params:
            return [{"date": self.query_params.get("date")}]

        query_params = self.config.get(self.name, {}).get("query_params", {})
        other_params = self.config.get(self.name, {}).get("other_params", {})
        date_range = other_params.get("date_range")

        if "date" in query_params and "date_range" in other_params:
            raise ConfigValidationError(
                "Cannot specify both 'date' and 'date_range' in query_params and other_params."
            )

        if not date_range:
            return [{"date": datetime.today().date().strftime("%Y-%m-%d")}]

        assert isinstance(date_range, list), "date_range must be a list"

        if len(date_range) == 1 or date_range[-1] == "today":
            date_range = date_range + [datetime.today().date().strftime("%Y-%m-%d")]

        # Generate all dates in range
        all_dates = []
        current_date = datetime.fromisoformat(date_range[0])
        end_date = datetime.fromisoformat(date_range[-1])
        while current_date <= end_date:
            all_dates.append({"date": current_date.strftime("%Y-%m-%d")})
            current_date += timedelta(days=1)
        
        return all_dates

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        dates_dict = self._get_dates_dict()
        starting_date = self.get_starting_timestamp(context)
        filtered_dates = [d for d in dates_dict if d["date"] >= starting_date]
        
        for date_dict in filtered_dates:
            self.query_params.update(date_dict)
            yield from super().get_records(context)


class HistoricalMarketPerformanceStream(TimeSliceStream):
    """Stream for Historical Market Performance API."""

    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        self.query_params.update(context)
        return super().get_records(context)


class MarketSectorPerformanceSnapshotStream(PerformanceSnapshotStream):
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


class IndustryPerformanceSnapshotStream(PerformanceSnapshotStream):
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


class SectorPeSnapshotStream(PerformanceSnapshotStream):
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


class IndustryPeSnapshotStream(PerformanceSnapshotStream):
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
