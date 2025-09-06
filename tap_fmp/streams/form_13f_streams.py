"""Form 13F streams."""

from __future__ import annotations

import typing as t
from datetime import datetime
from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_fmp.client import FmpRestStream, FmpSurrogateKeyStream
from tap_fmp.helpers import safe_int


class Form13fCikPartitionStream(FmpRestStream):
    @property
    def partitions(self):
        query_params = self.stream_config.get("query_params", {})
        other_params = self.stream_config.get("other_params", {})

        quarters = (
            [query_params["quarter"]]
            if "quarter" in query_params
            else other_params.get("quarters")
        )

        years = (
            [query_params["year"]]
            if "year" in query_params
            else other_params.get("years")
        )

        if not quarters or quarters == ["*"]:
            quarters = [1, 2, 3, 4]

        if not years or years == ["*"]:
            current_year = datetime.today().year
            years = [y for y in range(2020, current_year + 1)]

        return [
            {"cik": c["cik"], "year": y, "quarter": q}
            for c in self._tap.get_cached_ciks()
            for y in years
            for q in quarters
        ]

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        self.query_params.update(context)
        return super().get_records(context)


class Form13fSymbolPartitionStream(FmpRestStream):
    @property
    def partitions(self):
        query_params = self.stream_config.get("query_params", {})
        other_params = self.stream_config.get("other_params", {})

        quarters = (
            [query_params.get("quarter")]
            if "quarter" in query_params
            else other_params.get("quarters")
        )

        years = (
            [query_params.get("year")]
            if "year" in query_params
            else other_params.get("years")
        )

        if not quarters or quarters == ["*"]:
            quarters = [1, 2, 3, 4]

        if not years or years == ["*"]:
            current_year = datetime.today().year
            years = [y for y in range(2020, current_year + 1)]

        return [
            {"symbol": s["symbol"], "year": y, "quarter": q}
            for s in self._tap.get_cached_company_symbols()
            for y in years
            for q in quarters
        ]

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        self.query_params.update(context)
        return super().get_records(context)


class InstitutionalOwnershipFilingsStream(FmpRestStream):
    """Institutional Ownership Filings API - Latest SEC filings related to institutional ownership."""

    name = "institutional_ownership_latest_filings"
    primary_keys = ["surrogate_key"]
    _paginate = True
    _add_surrogate_key = True
    _max_pages = 100

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


class FilingsExtractStream(Form13fCikPartitionStream):
    """Filings Extract API - Extract detailed data directly from official SEC filings."""

    name = "institutional_ownership_extract"
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


class Form13fFilingDates(FmpSurrogateKeyStream):
    """Form 13F Filing Dates API - List of available SEC filings for institutional ownership."""

    name = "form_13f_filing_dates"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("cik", th.StringType),
        th.Property("date", th.DateType),
        th.Property("year", th.IntegerType),
        th.Property("quarter", th.IntegerType),
    ).to_dict()

    @property
    def partitions(self):
        return [{"cik": c["cik"]} for c in self._tap.get_cached_ciks()]

    def get_url(self, context):
        return f"{self.url_base}/stable/institutional-ownership/dates"

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        self.query_params.update(context)
        return super().get_records(context)

    def post_process(self, record: dict, context: Context | None = None) -> dict:
        record["cik"] = context.get("cik")
        return super().post_process(record, context)


class Form13fFilingExtractsWithAnalytics(Form13fSymbolPartitionStream):
    name = "form_13f_filings_extracts_with_analytics"

    primary_keys = ["surrogate_key"]
    _paginate = True
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("date", th.DateType),
        th.Property("cik", th.StringType),
        th.Property("filing_date", th.DateType),
        th.Property("investor_name", th.StringType),
        th.Property("symbol", th.StringType),
        th.Property("security_name", th.StringType),
        th.Property("type_of_security", th.StringType),
        th.Property("security_cusip", th.StringType),
        th.Property("shares_type", th.StringType),
        th.Property("put_call_share", th.StringType),
        th.Property("investment_discretion", th.StringType),
        th.Property("industry_title", th.StringType),
        th.Property("weight", th.NumberType),
        th.Property("last_weight", th.NumberType),
        th.Property("change_in_weight", th.NumberType),
        th.Property("change_in_weight_percentage", th.NumberType),
        th.Property("market_value", th.NumberType),
        th.Property("last_market_value", th.NumberType),
        th.Property("change_in_market_value", th.NumberType),
        th.Property("change_in_market_value_percentage", th.NumberType),
        th.Property("shares_number", th.IntegerType),
        th.Property("last_shares_number", th.IntegerType),
        th.Property("change_in_shares_number", th.IntegerType),
        th.Property("change_in_shares_number_percentage", th.NumberType),
        th.Property("quarter_end_price", th.NumberType),
        th.Property("avg_price_paid", th.NumberType),
        th.Property("is_new", th.BooleanType),
        th.Property("is_sold_out", th.BooleanType),
        th.Property("ownership", th.NumberType),
        th.Property("last_ownership", th.NumberType),
        th.Property("change_in_ownership", th.NumberType),
        th.Property("change_in_ownership_percentage", th.NumberType),
        th.Property("holding_period", th.IntegerType),
        th.Property("first_added", th.DateType),
        th.Property("performance", th.NumberType),
        th.Property("performance_percentage", th.NumberType),
        th.Property("last_performance", th.NumberType),
        th.Property("change_in_performance", th.NumberType),
        th.Property("is_counted_for_performance", th.BooleanType),
        th.Property("year", th.IntegerType),
        th.Property("quarter", th.IntegerType),
    ).to_dict()

    def get_url(self, context: Context | None) -> str:
        return (
            f"{self.url_base}/stable/institutional-ownership/extract-analytics/holder"
        )

    def post_process(self, record: dict, context: Context | None = None) -> dict:
        if "year" in record:
            record["year"] = int(record["year"])
        return super().post_process(record, context)


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
        th.Property("performance_relative_to_sp500_percentage", th.NumberType),
        th.Property("performance1year_relative_to_sp500_percentage", th.NumberType),
        th.Property("performance3year_relative_to_sp500_percentage", th.NumberType),
        th.Property("performance5year_relative_to_sp500_percentage", th.NumberType),
        th.Property(
            "performance_since_inception_relative_to_sp500_percentage", th.NumberType
        ),
    ).to_dict()

    def get_url(self, context: Context | None) -> str:
        return (
            f"{self.url_base}/stable/institutional-ownership/holder-performance-summary"
        )

    @property
    def partitions(self):
        return [{"cik": c["cik"]} for c in self._tap.get_cached_ciks()]

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        self.query_params.update(context)
        return super().get_records(context)


class HolderIndustryBreakdownStream(Form13fCikPartitionStream):
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


class PositionsSummaryStream(Form13fSymbolPartitionStream):
    """Positions Summary API - Comprehensive snapshot of institutional holdings by symbol."""

    name = "positions_summary"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("cik", th.StringType),
        th.Property("date", th.DateType),
        th.Property("year", th.IntegerType),
        th.Property("quarter", th.IntegerType),
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

    def post_process(self, record: dict, context: Context | None = None) -> dict:
        """Add year and quarter from context and convert to integers."""
        if context and "year" in context:
            record["year"] = safe_int(context["year"])
        if context and "quarter" in context:
            record["quarter"] = safe_int(context["quarter"])
        return super().post_process(record, context)


class IndustryPerformanceSummaryStream(FmpRestStream):
    """Industry Performance Summary API - Financial performance overview by industry."""

    name = "industry_performance_summary"
    primary_keys = ["industry_title", "industry_value", "date"]

    schema = th.PropertiesList(
        th.Property("industry_title", th.StringType),
        th.Property("industry_value", th.NumberType),
        th.Property("date", th.DateType),
    ).to_dict()

    def get_url(self, context: Context | None) -> str:
        return f"{self.url_base}/stable/institutional-ownership/industry-summary"

    @property
    def partitions(self):
        query_params = self.stream_config.get("query_params", {})
        other_params = self.stream_config.get("other_params", {})

        if ("year" in query_params or "quarter" in query_params) and (
            "years" in other_params or "quarters" in other_params
        ):
            raise ValueError(
                "Configuration error: Specify year/quarter in query_params OR years/quarters in other_params, not both."
            )

        if "year" in query_params:
            years = [query_params.get("year")]
        else:
            years = other_params.get("years")

        if not years or years == "*" or years == ["*"]:
            years = [str(y) for y in range(2023, datetime.today().year + 1)]
        elif isinstance(years, str):
            years = [years]

        if "quarter" in query_params:
            quarters = [query_params.get("quarter")]
        else:
            quarters = other_params.get("quarters")

        if not quarters or quarters == "*" or quarters == ["*"]:
            quarters = [1, 2, 3, 4]
        elif isinstance(quarters, (str, int)):
            quarters = [int(quarters)]

        return [{"year": y, "quarter": q} for y in years for q in quarters]

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        if context:
            self.query_params.update(context)
        return super().get_records(context)
