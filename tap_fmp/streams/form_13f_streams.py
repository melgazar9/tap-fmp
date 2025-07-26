"""Form 13F streams."""

from __future__ import annotations

from datetime import datetime
from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_fmp.client import FmpRestStream


class Form13fPartitionStream(FmpRestStream):
    @property
    def partitions(self):
        query_params = self.config.get(self.name, {}).get("query_params", {})
        quarters = query_params.get("quarter")
        years = query_params.get("years")
        if quarters is None or quarters == "*":
            quarters = [1, 2, 3, 4]
        if years is None or years == "*":
            years = [y for y in range(2023, datetime.today().date().year + 1)]
            years = [str(y) for y in years]
        return [
            {"cik": c["cik"], "year": y, "quarter": q}
            for c in self._tap.get_cached_ciks()
            for y in years
            for q in quarters
        ]


class InstitutionalOwnershipFilingsStream(Form13fPartitionStream):
    """Institutional Ownership Filings API - Latest SEC filings related to institutional ownership."""

    name = "institutional_ownership_filings"
    primary_keys = ["surrogate_key"]
    _paginate = True
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("cik", th.StringType),
        th.Property("name", th.StringType),
        th.Property("date", th.DateType),
        th.Property("filing_date", th.DateTimeType),
        th.Property("accepted_date", th.DateTimeType),
        th.Property("form_type", th.StringType),
        th.Property("link", th.StringType),
        th.Property("final_link", th.StringType),
    ).to_dict()

    def get_url(self, context: Context | None) -> str:
        return f"{self.url_base}/stable/institutional-ownership/latest"


class FilingsExtractStream(Form13fPartitionStream):
    """Filings Extract API - Extract detailed data directly from official SEC filings."""

    name = "filings_extract"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("date", th.DateType),
        th.Property("filing_date", th.DateType),
        th.Property("accepted_date", th.DateType),
        th.Property("cik", th.StringType),
        th.Property("security_cusip", th.StringType),
        th.Property("symbol", th.StringType),
        th.Property("name_of_issuer", th.StringType),
        th.Property("shares", th.IntegerType),
        th.Property("title_of_class", th.StringType),
        th.Property("shares_type", th.StringType),
        th.Property("put_call_share", th.StringType),
        th.Property("value", th.NumberType),
        th.Property("link", th.StringType),
        th.Property("final_link", th.StringType),
    ).to_dict()

    def get_url(self, context: Context | None) -> str:
        return f"{self.url_base}/stable/institutional-ownership/extract"


class HolderPerformanceSummaryStream(FmpRestStream):
    """Holder Performance Summary API - Performance insights for institutional investors."""

    name = "holder_performance_summary"
    primary_keys = ["surrogate_key"]
    _paginate = True
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("date", th.DateType),
        th.Property("cik", th.StringType),
        th.Property("investor_name", th.StringType),
        th.Property("portfolio_size", th.IntegerType),
        th.Property("securities_added", th.IntegerType),
        th.Property("securities_removed", th.IntegerType),
        th.Property("market_value", th.NumberType),
        th.Property("previous_market_value", th.NumberType),
        th.Property("change_in_market_value", th.NumberType),
        th.Property("change_in_market_value_percentage", th.NumberType),
        th.Property("average_holding_period", th.IntegerType),
        th.Property("average_holding_period_top10", th.IntegerType),
        th.Property("average_holding_period_top20", th.IntegerType),
        th.Property("turnover", th.NumberType),
        th.Property("turnover_alternate_sell", th.NumberType),
        th.Property("turnover_alternate_buy", th.NumberType),
        th.Property("performance", th.NumberType),
        th.Property("performance_percentage", th.NumberType),
        th.Property("last_performance", th.NumberType),
        th.Property("change_in_performance", th.NumberType),
        th.Property("performance1year", th.NumberType),
        th.Property("performance_percentage1year", th.NumberType),
        th.Property("performance3year", th.NumberType),
        th.Property("performance_percentage3year", th.NumberType),
        th.Property("performance5year", th.NumberType),
        th.Property("performance_percentage5year", th.NumberType),
        th.Property("performance_since_inception", th.NumberType),
        th.Property("performance_since_inception_percentage", th.NumberType),
        th.Property("performance_relative_to_s_p500_percentage", th.NumberType),
        th.Property("performance1year_relative_to_s_p500_percentage", th.NumberType),
        th.Property("performance3year_relative_to_s_p500_percentage", th.NumberType),
        th.Property("performance5year_relative_to_s_p500_percentage", th.NumberType),
        th.Property(
            "performance_since_inception_relative_to_s_p500_percentage", th.NumberType
        ),
    ).to_dict()

    def get_url(self, context: Context | None) -> str:
        return (
            f"{self.url_base}/stable/institutional-ownership/holder-performance-summary"
        )

    @property
    def partitions(self):
        return [{"cik": c["cik"]} for c in self._tap.get_cached_ciks()]


class HolderIndustryBreakdownStream(Form13fPartitionStream):
    """Holders Industry Breakdown API - Industry distribution of institutional holdings."""

    name = "holder_industry_breakdown"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("date", th.DateType),
        th.Property("cik", th.StringType),
        th.Property("investor_name", th.StringType),
        th.Property("industry_title", th.StringType),
        th.Property("weight", th.NumberType),
        th.Property("last_weight", th.NumberType),
        th.Property("change_in_weight", th.NumberType),
        th.Property("change_in_weight_percentage", th.NumberType),
        th.Property("performance", th.NumberType),
        th.Property("performance_percentage", th.NumberType),
        th.Property("last_performance", th.NumberType),
        th.Property("change_in_performance", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context | None) -> str:
        return (
            f"{self.url_base}/stable/institutional-ownership/holder-industry-breakdown"
        )


class PositionsSummaryStream(FmpRestStream):
    """Positions Summary API - Comprehensive snapshot of institutional holdings by symbol."""

    name = "positions_summary"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("cik", th.StringType),
        th.Property("date", th.DateType),
        th.Property("investors_holding", th.IntegerType),
        th.Property("last_investors_holding", th.IntegerType),
        th.Property("investors_holding_change", th.IntegerType),
        th.Property("number_of13_fshares", th.NumberType),
        th.Property("last_number_of13_fshares", th.NumberType),
        th.Property("number_of13_fshares_change", th.NumberType),
        th.Property("total_invested", th.NumberType),
        th.Property("last_total_invested", th.NumberType),
        th.Property("total_invested_change", th.NumberType),
        th.Property("ownership_percent", th.NumberType),
        th.Property("last_ownership_percent", th.NumberType),
        th.Property("ownership_percent_change", th.NumberType),
        th.Property("new_positions", th.IntegerType),
        th.Property("last_new_positions", th.IntegerType),
        th.Property("new_positions_change", th.IntegerType),
        th.Property("increased_positions", th.IntegerType),
        th.Property("last_increased_positions", th.IntegerType),
        th.Property("increased_positions_change", th.IntegerType),
        th.Property("closed_positions", th.IntegerType),
        th.Property("last_closed_positions", th.IntegerType),
        th.Property("closed_positions_change", th.IntegerType),
        th.Property("reduced_positions", th.IntegerType),
        th.Property("last_reduced_positions", th.IntegerType),
        th.Property("reduced_positions_change", th.IntegerType),
        th.Property("total_calls", th.NumberType),
        th.Property("last_total_calls", th.NumberType),
        th.Property("total_calls_change", th.NumberType),
        th.Property("total_puts", th.NumberType),
        th.Property("last_total_puts", th.NumberType),
        th.Property("total_puts_change", th.NumberType),
        th.Property("put_call_ratio", th.NumberType),
        th.Property("last_put_call_ratio", th.NumberType),
        th.Property("put_call_ratio_change", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context | None) -> str:
        return (
            f"{self.url_base}/stable/institutional-ownership/symbol-positions-summary"
        )

    @property
    def partitions(self):
        query_params = self.config.get(self.name, {}).get("query_params", {})
        years = query_params.get("years")
        quarters = query_params.get("quarter")
        if years is None or years == "*":
            years = [y for y in range(2023, datetime.today().date().year + 1)]
            years = [str(y) for y in years]
        if quarters is None or quarters == "*":
            quarters = [1, 2, 3, 4]
        return [
            {"symbol": s["symbol"], "quarter": q, "year": y}
            for s in self._tap.get_cached_symbols()
            for y in years
            for q in quarters
        ]


class IndustryPerformanceSummaryStream(FmpRestStream):
    """Industry Performance Summary API - Financial performance overview by industry."""

    name = "industry_performance_summary"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("industry_title", th.StringType),
        th.Property("industry_value", th.NumberType),
        th.Property("date", th.DateType),
    ).to_dict()

    def get_url(self, context: Context | None) -> str:
        return f"{self.url_base}/stable/institutional-ownership/industry-summary"

    @property
    def partitions(self):
        query_params = self.config.get(self.name, {}).get("query_params", {})
        years = query_params.get("years")
        quarters = query_params.get("quarter")
        if years is None or years == "*":
            years = [y for y in range(2023, datetime.today().date().year + 1)]
            years = [str(y) for y in years]
        if quarters is None or quarters == "*":
            quarters = [1, 2, 3, 4]
        return [{"quarter": q, "year": y} for y in years for q in quarters]
