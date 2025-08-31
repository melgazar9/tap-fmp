"""Insider Trades stream types classes for tap-fmp."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_fmp.client import FmpRestStream, SymbolPartitionStream
from datetime import datetime


class LatestInsiderTradingStream(FmpRestStream):
    """Stream for Latest Insider Trading API."""

    name = "latest_insider_trading"
    primary_keys = ["surrogate_key"]
    replication_method = "INCREMENTAL"
    replication_key = "filing_date"
    _paginate = True
    _max_pages = 100
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("filing_date", th.DateType),
        th.Property("transaction_date", th.DateType),
        th.Property("reporting_cik", th.StringType),
        th.Property("company_cik", th.StringType),
        th.Property("transaction_type", th.StringType),
        th.Property("securities_owned", th.NumberType),
        th.Property("reporting_name", th.StringType),
        th.Property("type_of_owner", th.StringType),
        th.Property("acquisition_or_disposition", th.StringType),
        th.Property("direct_or_indirect", th.StringType),
        th.Property("form_type", th.StringType),
        th.Property("securities_transacted", th.NumberType),
        th.Property("price", th.NumberType),
        th.Property("security_name", th.StringType),
        th.Property("url", th.StringType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        """Get URL for the request."""
        return f"{self.url_base}/stable/insider-trading/latest"

    @property
    def partitions(self):
        other_params = self.stream_config.get("other_params")
        if other_params and "dates" in other_params:
            return [{"date": d} for d in other_params["dates"]]
        else:
            return [{"date": datetime.today().date().strftime("%Y-%m-%d")}]


class SearchInsiderTradesStream(SymbolPartitionStream):
    """Stream for Search Insider Trades API."""

    name = "insider_trades_search"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True
    _paginate = True
    _max_pages = 100

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("filing_date", th.DateType),
        th.Property("transaction_date", th.DateType),
        th.Property("reporting_cik", th.StringType),
        th.Property("company_cik", th.StringType),
        th.Property("transaction_type", th.StringType),
        th.Property("securities_owned", th.NumberType),
        th.Property("reporting_name", th.StringType),
        th.Property("type_of_owner", th.StringType),
        th.Property("acquisition_or_disposition", th.StringType),
        th.Property("direct_or_indirect", th.StringType),
        th.Property("form_type", th.StringType),
        th.Property("securities_transacted", th.NumberType),
        th.Property("price", th.NumberType),
        th.Property("security_name", th.StringType),
        th.Property("url", th.StringType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        """Get URL for the request."""
        return f"{self.url_base}/stable/insider-trading/search"


class SearchInsiderTradesByReportingNameStream(FmpRestStream):
    """Stream for Search Insider Trades by Reporting Name API."""

    name = "insider_trades_by_reporting_name_search"
    primary_keys = ["reporting_cik", "reporting_name"]

    schema = th.PropertiesList(
        th.Property("reporting_cik", th.StringType),
        th.Property("reporting_name", th.StringType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        """Get URL for the request."""
        return f"{self.url_base}/stable/insider-trading/reporting-name"


class AllInsiderTransactionTypesStream(FmpRestStream):
    """Stream for All Insider Transaction Types API."""

    name = "all_insider_transaction_types"
    primary_keys = ["transaction_type"]

    schema = th.PropertiesList(
        th.Property("transaction_type", th.StringType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        """Get URL for the request."""
        return f"{self.url_base}/stable/insider-trading-transaction-type"


class InsiderTradeStatisticsStream(SymbolPartitionStream):
    """Stream for Insider Trade Statistics API."""

    name = "insider_trade_statistics"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("cik", th.StringType),
        th.Property("year", th.IntegerType),
        th.Property("quarter", th.IntegerType),
        th.Property("acquired_transactions", th.IntegerType),
        th.Property("disposed_transactions", th.IntegerType),
        th.Property("acquired_disposed_ratio", th.NumberType),
        th.Property("total_acquired", th.NumberType),
        th.Property("total_disposed", th.NumberType),
        th.Property("average_acquired", th.NumberType),
        th.Property("average_disposed", th.NumberType),
        th.Property("total_purchases", th.IntegerType),
        th.Property("total_sales", th.IntegerType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        """Get URL for the request."""
        return f"{self.url_base}/stable/insider-trading/statistics"


class AcquisitionOwnershipStream(SymbolPartitionStream):
    """Stream for Acquisition Ownership API."""

    name = "acquisition_ownership"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("cik", th.StringType),
        th.Property("symbol", th.StringType),
        th.Property("filing_date", th.DateType),
        th.Property("accepted_date", th.DateType),
        th.Property("cusip", th.StringType),
        th.Property("name_of_reporting_person", th.StringType),
        th.Property("citizenship_or_place_of_organization", th.StringType),
        th.Property("sole_voting_power", th.StringType),
        th.Property("shared_voting_power", th.StringType),
        th.Property("sole_dispositive_power", th.StringType),
        th.Property("shared_dispositive_power", th.StringType),
        th.Property("amount_beneficially_owned", th.StringType),
        th.Property("percent_of_class", th.StringType),
        th.Property("type_of_reporting_person", th.StringType),
        th.Property("url", th.StringType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        """Get URL for the request."""
        return f"{self.url_base}/stable/acquisition-of-beneficial-ownership"
