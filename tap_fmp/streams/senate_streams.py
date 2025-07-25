"""Senate stream types classes for tap-fmp."""

from __future__ import annotations

import typing as t

from singer_sdk import typing as th
from singer_sdk.helpers.types import Context
from tap_fmp.client import FmpRestStream, SymbolPartitionStream


class BaseSenateStream(FmpRestStream):
    """Base class for Senate/House financial disclosure streams."""

    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True
    _paginate = True
    _max_pages = 100
    
    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("disclosure_date", th.DateType),
        th.Property("transaction_date", th.DateType),
        th.Property("first_name", th.StringType),
        th.Property("last_name", th.StringType),
        th.Property("office", th.StringType),
        th.Property("district", th.StringType),
        th.Property("owner", th.StringType),
        th.Property("asset_description", th.StringType),
        th.Property("asset_type", th.StringType),
        th.Property("type", th.StringType),
        th.Property("amount", th.StringType),
        th.Property("capital_gains_over200_usd", th.StringType),
        th.Property("comment", th.StringType),
        th.Property("link", th.StringType),
    ).to_dict()

class SenateNamePartitionMixin(FmpRestStream):
    """Mixin for Senate streams that need name partitioning."""

    @property
    def partitions(self):
        query_params_name = self.query_params.get("name")
        other_params_names = self.config.get("other_params", {}).get("names")

        assert not (query_params_name and other_params_names), (
            f"Cannot specify name configurations in both query_params and "
            f"other_params for stream {self.name}."
        )

        if query_params_name:
            return [{"name": query_params_name}] if isinstance(query_params_name, str) else query_params_name
        elif other_params_names:
            return [{"name": name} for name in other_params_names] if isinstance(other_params_names, list) else other_params_names
        else:
            default_names = ["Nancy", "Chuck", "Mitch", "Kevin", "Alexandria", "Ted", "Elizabeth", "Bernie"]
            return [{"name": name} for name in default_names]

    def get_records(self, context: Context | None):
        self.query_params.update(context)
        return super().get_records(context)

class LatestSenateDisclosuresStream(BaseSenateStream):
    """Stream for Latest Senate Financial Disclosures API."""
    
    name = "latest_senate_disclosures"

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/senate-latest"


class LatestHouseDisclosuresStream(BaseSenateStream):
    """Stream for Latest House Financial Disclosures API."""
    
    name = "latest_house_disclosures"

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/house-latest"


class SenateTradingActivityStream(SymbolPartitionStream):
    """Stream for Senate Trading Activity API."""
    
    name = "senate_trading_activity"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True
    
    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("disclosure_date", th.DateType),
        th.Property("transaction_date", th.DateType),
        th.Property("first_name", th.StringType),
        th.Property("last_name", th.StringType),
        th.Property("office", th.StringType),
        th.Property("district", th.StringType),
        th.Property("owner", th.StringType),
        th.Property("asset_description", th.StringType),
        th.Property("asset_type", th.StringType),
        th.Property("type", th.StringType),
        th.Property("amount", th.StringType),
        th.Property("capital_gains_over200_usd", th.StringType),
        th.Property("comment", th.StringType),
        th.Property("link", th.StringType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/senate-trades"


class SenateTradesByNameStream(SenateNamePartitionMixin):
    """Stream for Senate Trades By Name API."""
    
    name = "senate_trades_by_name"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True
    
    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("disclosure_date", th.DateType),
        th.Property("transaction_date", th.DateType),
        th.Property("first_name", th.StringType),
        th.Property("last_name", th.StringType),
        th.Property("office", th.StringType),
        th.Property("district", th.StringType),
        th.Property("owner", th.StringType),
        th.Property("asset_description", th.StringType),
        th.Property("asset_type", th.StringType),
        th.Property("type", th.StringType),
        th.Property("amount", th.StringType),
        th.Property("capital_gains_over200_usd", th.StringType),
        th.Property("comment", th.StringType),
        th.Property("link", th.StringType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/senate-trades-by-name"


class HouseTradesStream(SymbolPartitionStream):
    """Stream for U.S. House Trades API."""
    
    name = "house_trades"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True
    
    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("disclosure_date", th.DateType),
        th.Property("transaction_date", th.DateType),
        th.Property("first_name", th.StringType),
        th.Property("last_name", th.StringType),
        th.Property("office", th.StringType),
        th.Property("district", th.StringType),
        th.Property("owner", th.StringType),
        th.Property("asset_description", th.StringType),
        th.Property("asset_type", th.StringType),
        th.Property("type", th.StringType),
        th.Property("amount", th.StringType),
        th.Property("capital_gains_over200_usd", th.StringType),
        th.Property("comment", th.StringType),
        th.Property("link", th.StringType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/house-trades"


class HouseTradesByNameStream(SenateNamePartitionMixin):
    """Stream for House Trades By Name API."""
    
    name = "house_trades_by_name"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True
    
    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("disclosure_date", th.DateType),
        th.Property("transaction_date", th.DateType),
        th.Property("first_name", th.StringType),
        th.Property("last_name", th.StringType),
        th.Property("office", th.StringType),
        th.Property("district", th.StringType),
        th.Property("owner", th.StringType),
        th.Property("asset_description", th.StringType),
        th.Property("asset_type", th.StringType),
        th.Property("type", th.StringType),
        th.Property("amount", th.StringType),
        th.Property("capital_gains_over200_usd", th.StringType),
        th.Property("comment", th.StringType),
        th.Property("link", th.StringType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/house-trades-by-name"