"""Bulk stream types classes for tap-fmp."""

from __future__ import annotations

import typing as t
from singer_sdk import typing as th
from singer_sdk.helpers.types import Context
from tap_fmp.client import FmpSurrogateKeyStream, IncrementalDateStream
from datetime import datetime
from singer_sdk.exceptions import ConfigValidationError
from decimal import Decimal


class BaseBulkStream(FmpSurrogateKeyStream):
    _expect_csv = True

    _decimal_fields = None
    _float_fields = None
    _integer_fields = None
    _date_fields = None
    _datetime_fields = None

    def post_process(self, record: dict, context: Context | None = None) -> dict:
        field_converters = [
            (self._decimal_fields, lambda v: Decimal(str(v))),
            (self._float_fields, float),
            (self._integer_fields, int),
            (self._date_fields, lambda x: datetime.fromisoformat(x).date()),
            (self._datetime_fields, lambda x: datetime.fromisoformat(x)),
        ]

        for fields, converter in field_converters:
            if fields:
                assert isinstance(fields, (tuple, list))
                for col in fields:
                    value = record.get(col)
                    if value in (None, ""):
                        record[col] = None
                    else:
                        record[col] = converter(value)
        return super().post_process(record, context)


class PaginatedBulkStream(BaseBulkStream):
    """Base class for bulk streams that need partitioning by part parameter."""

    replication_key = "_part"
    replication_method = "INCREMENTAL"
    _paginate = True
    _paginate_key = "part"

    def _get_parts(self):
        if "part" in self.query_params:
            return [self.query_params.get("part")]

        other_params = self.stream_config.get("other_params", {})
        parts = other_params.get("parts")

        if "part" in self.query_params and "parts" in other_params:
            raise ConfigValidationError(
                "Cannot specify both 'part' in query_params and 'parts' in other_params."
            )

        if not parts or parts == "*" or parts == ["*"]:
            return None

        if isinstance(parts, (str, int)):
            parts = [parts]

        parts = [int(part) for part in parts]

        if self.replication_method == "INCREMENTAL":
            state = self.get_context_state(context=None)
            if state.get("replication_key_value"):
                min_part = int(state["replication_key_value"])
                parts = [part for part in parts if part >= min_part]

        return parts

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        parts = self._get_parts()
        if parts:
            for part in parts:
                self.query_params.update({"part": part})
                yield from super().get_records(context)
        else:
            if self.replication_method == "INCREMENTAL":
                state = self.get_context_state(context)
                if state.get("replication_key_value"):
                    starting_part = int(state["replication_key_value"])
                    self.query_params["part"] = starting_part
            yield from super().get_records(context)

    def post_process(self, row: dict, context: Context | None = None) -> dict:
        row["_part"] = self.query_params["part"]
        return super().post_process(row, context)


class IncrementalYearPeriodStream(BaseBulkStream):
    replication_key = "date"
    replication_method = "INCREMENTAL"

    def _get_year_range(self):
        if "year" in self.query_params:
            return [self.query_params.get("year")]

        other_params = self.stream_config.get("other_params", {})
        year_range = other_params.get("year_range")

        if "year" in self.query_params and "year_range" in other_params:
            raise ConfigValidationError(
                "Cannot specify both 'year' and 'year_range' in query_params and other_params."
            )

        if not year_range:
            return [y for y in range(1990, datetime.today().year + 1)]

        assert isinstance(year_range, list), "date_range must be a list"

        if len(year_range) == 1 or year_range[-1] == "current_year":
            year_range = [y for y in range(year_range[0], datetime.today().year + 1)]
        return year_range

    def _get_periods(self):
        other_params = self.stream_config.get("other_params", {})

        if "period" in self.query_params and "periods" in other_params:
            raise ConfigValidationError(
                "Cannot specify both 'period' and 'periods' in query_params and other_params."
            )

        if "period" in self.query_params:
            return [self.query_params.get("period")]

        periods = other_params.get("periods")

        if not periods or periods == "*" or periods == ["*"]:
            return ["Q1", "Q2", "Q3", "Q4", "FY"]

        assert isinstance(periods, list), "date_range must be a list"

        return periods

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        year_range = self._get_year_range()
        starting_date = self.get_starting_timestamp(context)

        filtered_years = [
            y
            for y in year_range
            if y >= max(datetime.fromisoformat(starting_date).year, 1990)
        ]

        periods = self._get_periods()

        updated_query_params = [
            {"year": y, "period": p} for y in filtered_years for p in periods
        ]

        for query_dict in updated_query_params:
            self.query_params.update(query_dict)
            yield from super().get_records(context)


class TtmBulkStream(BaseBulkStream):
    def post_process(self, row: dict, context: Context = None) -> dict:
        row = {
            (
                (k[:-3].rstrip("_") + "_ttm")
                if (k.lower().endswith("ttm") and not k.lower().endswith("_ttm"))
                else k
            ): v
            for k, v in row.items()
        }
        return super().post_process(row, context)


class CompanyProfileBulkStream(PaginatedBulkStream):
    """Stream for Company Profile Bulk API."""

    name = "company_profile_bulk"

    _decimal_fields = [
        "price",
        "market_cap",
        "last_dividend",
        "change",
    ]

    _float_fields = [
        "beta",
        "change_percentage",
        "volume",
        "average_volume",
    ]

    _integer_fields = ["full_time_employees"]
    _date_fields = ["filing_date", "ipo_date"]

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("price", th.NumberType),
        th.Property("market_cap", th.NumberType),
        th.Property("filing_date", th.DateType),
        th.Property("beta", th.NumberType),
        th.Property("last_dividend", th.NumberType),
        th.Property("range", th.StringType),
        th.Property("change", th.NumberType),
        th.Property("change_percentage", th.NumberType),
        th.Property("volume", th.NumberType),
        th.Property("average_volume", th.NumberType),
        th.Property("company_name", th.StringType),
        th.Property("currency", th.StringType),
        th.Property("cik", th.StringType),
        th.Property("isin", th.StringType),
        th.Property("cusip", th.StringType),
        th.Property("exchange_full_name", th.StringType),
        th.Property("exchange", th.StringType),
        th.Property("industry", th.StringType),
        th.Property("website", th.StringType),
        th.Property("description", th.StringType),
        th.Property("ceo", th.StringType),
        th.Property("sector", th.StringType),
        th.Property("country", th.StringType),
        th.Property("full_time_employees", th.IntegerType),
        th.Property("phone", th.StringType),
        th.Property("address", th.StringType),
        th.Property("city", th.StringType),
        th.Property("state", th.StringType),
        th.Property("zip", th.StringType),
        th.Property("image", th.StringType),
        th.Property("ipo_date", th.DateType),
        th.Property("default_image", th.BooleanType),
        th.Property("is_etf", th.BooleanType),
        th.Property("is_actively_trading", th.BooleanType),
        th.Property("is_adr", th.BooleanType),
        th.Property("is_fund", th.BooleanType),
        th.Property("_part", th.IntegerType, required=True),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/profile-bulk"


class StockRatingBulkStream(BaseBulkStream):
    """Stream for Stock Rating Bulk API."""

    name = "stock_rating_bulk"

    _integer_fields = [
        "discounted_cash_flow_score",
        "return_on_equity_score",
        "return_on_assets_score",
        "debt_to_equity_score",
        "price_to_earnings_score",
        "price_to_book_score",
    ]

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("date", th.DateType),
        th.Property("rating", th.StringType),
        th.Property("discounted_cash_flow_score", th.IntegerType),
        th.Property("return_on_equity_score", th.IntegerType),
        th.Property("return_on_assets_score", th.IntegerType),
        th.Property("debt_to_equity_score", th.IntegerType),
        th.Property("price_to_earnings_score", th.IntegerType),
        th.Property("price_to_book_score", th.IntegerType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/rating-bulk"


class DcfValuationsBulkStream(BaseBulkStream):
    """Stream for DCF Valuations Bulk API."""

    name = "dcf_valuations_bulk"

    _decimal_fields = ["dcf", "stock_price"]

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("date", th.DateType),
        th.Property("dcf", th.NumberType),
        th.Property("stock_price", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/dcf-bulk"


class FinancialScoresBulkStream(BaseBulkStream):
    """Stream for Financial Scores Bulk API."""

    name = "financial_scores_bulk"

    _decimal_fields = [
        "working_capital",
        "total_assets",
        "retained_earnings",
        "ebit",
        "market_cap",
        "total_liabilities",
        "revenue",
    ]

    _float_fields = ["altman_z_score"]

    _integer_fields = ["piotroski_score"]

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("reported_currency", th.StringType),
        th.Property("altman_z_score", th.NumberType),
        th.Property("piotroski_score", th.IntegerType),
        th.Property("working_capital", th.NumberType),
        th.Property("total_assets", th.NumberType),
        th.Property("retained_earnings", th.NumberType),
        th.Property("ebit", th.NumberType),
        th.Property("market_cap", th.NumberType),
        th.Property("total_liabilities", th.NumberType),
        th.Property("revenue", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/scores-bulk"


class PriceTargetSummaryBulkStream(BaseBulkStream):
    """Stream for Price Target Summary Bulk API."""

    name = "price_target_summary_bulk"

    _decimal_fields = [
        "last_month_avg_price_target",
        "last_quarter_avg_price_target",
        "last_year_avg_price_target",
        "all_time_avg_price_target",
    ]
    _integer_fields = [
        "last_month_count",
        "last_quarter_count",
        "last_year_count",
        "all_time_count",
    ]

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("last_month_count", th.IntegerType),
        th.Property("last_month_avg_price_target", th.NumberType),
        th.Property("last_quarter_count", th.IntegerType),
        th.Property("last_quarter_avg_price_target", th.NumberType),
        th.Property("last_year_count", th.IntegerType),
        th.Property("last_year_avg_price_target", th.NumberType),
        th.Property("all_time_count", th.IntegerType),
        th.Property("all_time_avg_price_target", th.NumberType),
        th.Property("publishers", th.StringType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/price-target-summary-bulk"


class EtfHolderBulkStream(PaginatedBulkStream):
    """Stream for ETF Holder Bulk API."""

    name = "etf_holder_bulk"

    _decimal_fields = ["market_value"]
    _float_fields = ["weight_percentage", "shares_number"]

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("name", th.StringType),
        th.Property("shares_number", th.NumberType),
        th.Property("asset", th.StringType),
        th.Property("weight_percentage", th.NumberType),
        th.Property("cusip", th.StringType),
        th.Property("isin", th.StringType),
        th.Property("market_value", th.NumberType),
        th.Property("last_updated", th.DateType),
        th.Property("_part", th.IntegerType, required=True),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/etf-holder-bulk"


class UpgradesDowngradesConsensusBulkStream(BaseBulkStream):
    """Stream for Upgrades Downgrades Consensus Bulk API."""

    name = "upgrades_downgrades_consensus_bulk"

    _integer_fields = [
        "strong_buy",
        "buy",
        "hold",
        "sell",
        "strong_sell",
    ]

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("strong_buy", th.IntegerType),
        th.Property("buy", th.IntegerType),
        th.Property("hold", th.IntegerType),
        th.Property("sell", th.IntegerType),
        th.Property("strong_sell", th.IntegerType),
        th.Property("consensus", th.StringType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/upgrades-downgrades-consensus-bulk"


class KeyMetricsTtmBulkStream(TtmBulkStream):
    """Stream for Key Metrics TTM Bulk API."""

    name = "key_metrics_ttm_bulk"

    _decimal_fields = [
        "market_cap",
        "enterprise_value_ttm",
        "graham_number_ttm",
        "graham_net_net_ttm",
        "working_capital_ttm",
        "invested_capital_ttm",
        "average_receivables_ttm",
        "average_payables_ttm",
        "average_inventory_ttm",
        "free_cash_flow_to_equity_ttm",
        "free_cash_flow_to_firm_ttm",
        "tangible_asset_value_ttm",
        "net_current_asset_value_ttm",
    ]

    _float_fields = [
        "ev_to_sales_ttm",
        "ev_to_operating_cash_flow_ttm",
        "ev_to_free_cash_flow_ttm",
        "ev_to_ebitda_ttm",
        "net_debt_to_ebitda_ttm",
        "current_ratio_ttm",
        "income_quality_ttm",
        "tax_burden_ttm",
        "interest_burden_ttm",
        "return_on_assets_ttm",
        "operating_return_on_assets_ttm",
        "return_on_tangible_assets_ttm",
        "return_on_equity_ttm",
        "return_on_invested_capital_ttm",
        "return_on_capital_employed_ttm",
        "earnings_yield_ttm",
        "free_cash_flow_yield_ttm",
        "capex_to_operating_cash_flow_ttm",
        "capex_to_depreciation_ttm",
        "capex_to_revenue_ttm",
        "sales_general_and_administrative_to_revenue_ttm",
        "research_and_developement_to_revenue_ttm",
        "stock_based_compensation_to_revenue_ttm",
        "intangibles_to_total_assets_ttm",
        "days_of_sales_outstanding_ttm",
        "days_of_payables_outstanding_ttm",
        "days_of_inventory_outstanding_ttm",
        "operating_cycle_ttm",
        "cash_conversion_cycle_ttm",
    ]

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("market_cap", th.NumberType),
        th.Property("enterprise_value_ttm", th.NumberType),
        th.Property("ev_to_sales_ttm", th.NumberType),
        th.Property("ev_to_operating_cash_flow_ttm", th.NumberType),
        th.Property("ev_to_free_cash_flow_ttm", th.NumberType),
        th.Property("ev_to_ebitda_ttm", th.NumberType),
        th.Property("net_debt_to_ebitda_ttm", th.NumberType),
        th.Property("current_ratio_ttm", th.NumberType),
        th.Property("income_quality_ttm", th.NumberType),
        th.Property("graham_number_ttm", th.NumberType),
        th.Property("graham_net_net_ttm", th.NumberType),
        th.Property("tax_burden_ttm", th.NumberType),
        th.Property("interest_burden_ttm", th.NumberType),
        th.Property("working_capital_ttm", th.NumberType),
        th.Property("invested_capital_ttm", th.NumberType),
        th.Property("return_on_assets_ttm", th.NumberType),
        th.Property("operating_return_on_assets_ttm", th.NumberType),
        th.Property("return_on_tangible_assets_ttm", th.NumberType),
        th.Property("return_on_equity_ttm", th.NumberType),
        th.Property("return_on_invested_capital_ttm", th.NumberType),
        th.Property("return_on_capital_employed_ttm", th.NumberType),
        th.Property("earnings_yield_ttm", th.NumberType),
        th.Property("free_cash_flow_yield_ttm", th.NumberType),
        th.Property("capex_to_operating_cash_flow_ttm", th.NumberType),
        th.Property("capex_to_depreciation_ttm", th.NumberType),
        th.Property("capex_to_revenue_ttm", th.NumberType),
        th.Property("sales_general_and_administrative_to_revenue_ttm", th.NumberType),
        th.Property("research_and_developement_to_revenue_ttm", th.NumberType),
        th.Property("stock_based_compensation_to_revenue_ttm", th.NumberType),
        th.Property("intangibles_to_total_assets_ttm", th.NumberType),
        th.Property("average_receivables_ttm", th.NumberType),
        th.Property("average_payables_ttm", th.NumberType),
        th.Property("average_inventory_ttm", th.NumberType),
        th.Property("days_of_sales_outstanding_ttm", th.NumberType),
        th.Property("days_of_payables_outstanding_ttm", th.NumberType),
        th.Property("days_of_inventory_outstanding_ttm", th.NumberType),
        th.Property("operating_cycle_ttm", th.NumberType),
        th.Property("cash_conversion_cycle_ttm", th.NumberType),
        th.Property("free_cash_flow_to_equity_ttm", th.NumberType),
        th.Property("free_cash_flow_to_firm_ttm", th.NumberType),
        th.Property("tangible_asset_value_ttm", th.NumberType),
        th.Property("net_current_asset_value_ttm", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/key-metrics-ttm-bulk"


class RatiosTtmBulkStream(TtmBulkStream):
    """Stream for Ratios TTM Bulk API."""

    name = "ratios_ttm_bulk"

    _float_fields = [
        "gross_profit_margin_ttm",
        "ebit_margin_ttm",
        "ebitda_margin_ttm",
        "operating_profit_margin_ttm",
        "pretax_profit_margin_ttm",
        "continuous_operations_profit_margin_ttm",
        "net_profit_margin_ttm",
        "bottom_line_profit_margin_ttm",
        "receivables_turnover_ttm",
        "payables_turnover_ttm",
        "inventory_turnover_ttm",
        "fixed_asset_turnover_ttm",
        "asset_turnover_ttm",
        "current_ratio_ttm",
        "quick_ratio_ttm",
        "solvency_ratio_ttm",
        "cash_ratio_ttm",
        "price_to_earnings_ratio_ttm",
        "price_to_earnings_growth_ratio_ttm",
        "forward_price_to_earnings_growth_ratio_ttm",
        "price_to_book_ratio_ttm",
        "price_to_sales_ratio_ttm",
        "price_to_free_cash_flow_ratio_ttm",
        "price_to_operating_cash_flow_ratio_ttm",
        "debt_to_assets_ratio_ttm",
        "debt_to_equity_ratio_ttm",
        "debt_to_capital_ratio_ttm",
        "long_term_debt_to_capital_ratio_ttm",
        "financial_leverage_ratio_ttm",
        "working_capital_turnover_ratio_ttm",
        "operating_cash_flow_ratio_ttm",
        "operating_cash_flow_sales_ratio_ttm",
        "free_cash_flow_operating_cash_flow_ratio_ttm",
        "debt_service_coverage_ratio_ttm",
        "interest_coverage_ratio_ttm",
        "short_term_operating_cash_flow_coverage_ratio_ttm",
        "operating_cash_flow_coverage_ratio_ttm",
        "capital_expenditure_coverage_ratio_ttm",
        "dividend_paid_and_capex_coverage_ratio_ttm",
        "dividend_payout_ratio_ttm",
        "dividend_yield_ttm",
        "enterprise_value_ttm",
        "revenue_per_share_ttm",
        "net_income_per_share_ttm",
        "interest_debt_per_share_ttm",
        "cash_per_share_ttm",
        "book_value_per_share_ttm",
        "tangible_book_value_per_share_ttm",
        "shareholders_equity_per_share_ttm",
        "operating_cash_flow_per_share_ttm",
        "capex_per_share_ttm",
        "free_cash_flow_per_share_ttm",
        "net_income_per_ebt_ttm",
        "ebt_per_ebit_ttm",
        "price_to_fair_value_ttm",
        "debt_to_market_cap_ttm",
        "effective_tax_rate_ttm",
        "enterprise_value_multiple_ttm",
        "dividend_per_share_ttm",
    ]

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("gross_profit_margin_ttm", th.NumberType),
        th.Property("ebit_margin_ttm", th.NumberType),
        th.Property("ebitda_margin_ttm", th.NumberType),
        th.Property("operating_profit_margin_ttm", th.NumberType),
        th.Property("pretax_profit_margin_ttm", th.NumberType),
        th.Property("continuous_operations_profit_margin_ttm", th.NumberType),
        th.Property("net_profit_margin_ttm", th.NumberType),
        th.Property("bottom_line_profit_margin_ttm", th.NumberType),
        th.Property("receivables_turnover_ttm", th.NumberType),
        th.Property("payables_turnover_ttm", th.NumberType),
        th.Property("inventory_turnover_ttm", th.NumberType),
        th.Property("fixed_asset_turnover_ttm", th.NumberType),
        th.Property("asset_turnover_ttm", th.NumberType),
        th.Property("current_ratio_ttm", th.NumberType),
        th.Property("quick_ratio_ttm", th.NumberType),
        th.Property("solvency_ratio_ttm", th.NumberType),
        th.Property("cash_ratio_ttm", th.NumberType),
        th.Property("price_to_earnings_ratio_ttm", th.NumberType),
        th.Property("price_to_earnings_growth_ratio_ttm", th.NumberType),
        th.Property("forward_price_to_earnings_growth_ratio_ttm", th.NumberType),
        th.Property("price_to_book_ratio_ttm", th.NumberType),
        th.Property("price_to_sales_ratio_ttm", th.NumberType),
        th.Property("price_to_free_cash_flow_ratio_ttm", th.NumberType),
        th.Property("price_to_operating_cash_flow_ratio_ttm", th.NumberType),
        th.Property("debt_to_assets_ratio_ttm", th.NumberType),
        th.Property("debt_to_equity_ratio_ttm", th.NumberType),
        th.Property("debt_to_capital_ratio_ttm", th.NumberType),
        th.Property("long_term_debt_to_capital_ratio_ttm", th.NumberType),
        th.Property("financial_leverage_ratio_ttm", th.NumberType),
        th.Property("working_capital_turnover_ratio_ttm", th.NumberType),
        th.Property("operating_cash_flow_ratio_ttm", th.NumberType),
        th.Property("operating_cash_flow_sales_ratio_ttm", th.NumberType),
        th.Property("free_cash_flow_operating_cash_flow_ratio_ttm", th.NumberType),
        th.Property("debt_service_coverage_ratio_ttm", th.NumberType),
        th.Property("interest_coverage_ratio_ttm", th.NumberType),
        th.Property("short_term_operating_cash_flow_coverage_ratio_ttm", th.NumberType),
        th.Property("operating_cash_flow_coverage_ratio_ttm", th.NumberType),
        th.Property("capital_expenditure_coverage_ratio_ttm", th.NumberType),
        th.Property("dividend_paid_and_capex_coverage_ratio_ttm", th.NumberType),
        th.Property("dividend_payout_ratio_ttm", th.NumberType),
        th.Property("dividend_yield_ttm", th.NumberType),
        th.Property("enterprise_value_ttm", th.NumberType),
        th.Property("revenue_per_share_ttm", th.NumberType),
        th.Property("net_income_per_share_ttm", th.NumberType),
        th.Property("interest_debt_per_share_ttm", th.NumberType),
        th.Property("cash_per_share_ttm", th.NumberType),
        th.Property("book_value_per_share_ttm", th.NumberType),
        th.Property("tangible_book_value_per_share_ttm", th.NumberType),
        th.Property("shareholders_equity_per_share_ttm", th.NumberType),
        th.Property("operating_cash_flow_per_share_ttm", th.NumberType),
        th.Property("capex_per_share_ttm", th.NumberType),
        th.Property("free_cash_flow_per_share_ttm", th.NumberType),
        th.Property("net_income_per_ebt_ttm", th.NumberType),
        th.Property("ebt_per_ebit_ttm", th.NumberType),
        th.Property("price_to_fair_value_ttm", th.NumberType),
        th.Property("debt_to_market_cap_ttm", th.NumberType),
        th.Property("effective_tax_rate_ttm", th.NumberType),
        th.Property("enterprise_value_multiple_ttm", th.NumberType),
        th.Property("dividend_per_share_ttm", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/ratios-ttm-bulk"


class StockPeersBulkStream(BaseBulkStream):
    """Stream for Stock Peers Bulk API."""

    name = "stock_peers_bulk"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("peers", th.StringType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/peers-bulk"


class EarningsSurprisesBulkStream(BaseBulkStream):
    """Stream for Earnings Surprises Bulk API."""

    name = "earnings_surprises_bulk"

    _decimal_fields = ["eps_actual", "eps_estimated"]

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("date", th.DateType),
        th.Property("eps_actual", th.NumberType),
        th.Property("eps_estimated", th.NumberType),
        th.Property("last_updated", th.DateType),
    ).to_dict()

    def _get_years_dict(self):
        if "year" in self.query_params:
            return [{"year": str(self.query_params.get("year"))}]

        other_params = self.stream_config.get("other_params", {})
        years = other_params.get("years")

        if "year" in self.query_params and "years" in other_params:
            raise ValueError(
                f"Stream {self.name}: Cannot specify both 'year' in query_params and 'years' in other_params."
            )

        if not years:
            years = [y for y in range(1990, datetime.today().year + 1)]

        if isinstance(years, (str, int)):
            years = [years]

        return [{"year": str(year)} for year in years]

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        years_dict = self._get_years_dict()
        for year_dict in years_dict:
            self.query_params.update(year_dict)
            yield from super().get_records(context)

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/earnings-surprises-bulk"


class IncomeStatementBulkStream(IncrementalYearPeriodStream):
    """Stream for Income Statement Bulk API."""

    name = "income_statement_bulk"

    _float_fields = [
        "revenue",
        "cost_of_revenue",
        "gross_profit",
        "research_and_development_expenses",
        "general_and_administrative_expenses",
        "selling_and_marketing_expenses",
        "selling_general_and_administrative_expenses",
        "other_expenses",
        "operating_expenses",
        "cost_and_expenses",
        "net_interest_income",
        "interest_income",
        "interest_expense",
        "depreciation_and_amortization",
        "ebitda",
        "ebit",
        "non_operating_income_excluding_interest",
        "operating_income",
        "total_other_income_expenses_net",
        "income_before_tax",
        "income_tax_expense",
        "net_income_from_continuing_operations",
        "net_income_from_discontinued_operations",
        "other_adjustments_to_net_income",
        "net_income",
        "net_income_deductions",
        "bottom_line_net_income",
        "eps",
        "eps_diluted",
        "weighted_average_shs_out",
        "weighted_average_shs_out_dil",
    ]

    _date_fields = ["filing_date"]
    _datetime_fields = ["accepted_date"]

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("date", th.DateType),
        th.Property("symbol", th.StringType),
        th.Property("reported_currency", th.StringType),
        th.Property("cik", th.StringType),
        th.Property("filing_date", th.DateType),
        th.Property("accepted_date", th.DateTimeType),
        th.Property("fiscal_year", th.StringType),
        th.Property("period", th.StringType),
        th.Property("revenue", th.NumberType),
        th.Property("cost_of_revenue", th.NumberType),
        th.Property("gross_profit", th.NumberType),
        th.Property("research_and_development_expenses", th.NumberType),
        th.Property("general_and_administrative_expenses", th.NumberType),
        th.Property("selling_and_marketing_expenses", th.NumberType),
        th.Property("selling_general_and_administrative_expenses", th.NumberType),
        th.Property("other_expenses", th.NumberType),
        th.Property("operating_expenses", th.NumberType),
        th.Property("cost_and_expenses", th.NumberType),
        th.Property("net_interest_income", th.NumberType),
        th.Property("interest_income", th.NumberType),
        th.Property("interest_expense", th.NumberType),
        th.Property("depreciation_and_amortization", th.NumberType),
        th.Property("ebitda", th.NumberType),
        th.Property("ebit", th.NumberType),
        th.Property("non_operating_income_excluding_interest", th.NumberType),
        th.Property("operating_income", th.NumberType),
        th.Property("total_other_income_expenses_net", th.NumberType),
        th.Property("income_before_tax", th.NumberType),
        th.Property("income_tax_expense", th.NumberType),
        th.Property("net_income_from_continuing_operations", th.NumberType),
        th.Property("net_income_from_discontinued_operations", th.NumberType),
        th.Property("other_adjustments_to_net_income", th.NumberType),
        th.Property("net_income", th.NumberType),
        th.Property("net_income_deductions", th.NumberType),
        th.Property("bottom_line_net_income", th.NumberType),
        th.Property("eps", th.NumberType),
        th.Property("eps_diluted", th.NumberType),
        th.Property("weighted_average_shs_out", th.NumberType),
        th.Property("weighted_average_shs_out_dil", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/income-statement-bulk"


class IncomeStatementGrowthBulkStream(IncrementalYearPeriodStream):
    """Stream for Income Statement Growth Bulk API."""

    name = "income_statement_growth_bulk"

    _float_fields = [
        "growth_revenue",
        "growth_cost_of_revenue",
        "growth_gross_profit",
        "growth_gross_profit_ratio",
        "growth_research_and_development_expenses",
        "growth_general_and_administrative_expenses",
        "growth_selling_and_marketing_expenses",
        "growth_other_expenses",
        "growth_operating_expenses",
        "growth_cost_and_expenses",
        "growth_interest_income",
        "growth_interest_expense",
        "growth_depreciation_and_amortization",
        "growth_ebitda",
        "growth_operating_income",
        "growth_income_before_tax",
        "growth_income_tax_expense",
        "growth_net_income",
        "growth_eps",
        "growth_eps_diluted",
        "growth_weighted_average_shs_out",
        "growth_weighted_average_shs_out_dil",
        "growth_ebit",
        "growth_non_operating_income_excluding_interest",
        "growth_net_interest_income",
        "growth_total_other_income_expenses_net",
        "growth_net_income_from_continuing_operations",
        "growth_other_adjustments_to_net_income",
        "growth_net_income_deductions",
    ]

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("date", th.DateType),
        th.Property("fiscal_year", th.StringType),
        th.Property("period", th.StringType),
        th.Property("reported_currency", th.StringType),
        th.Property("growth_revenue", th.NumberType),
        th.Property("growth_cost_of_revenue", th.NumberType),
        th.Property("growth_gross_profit", th.NumberType),
        th.Property("growth_gross_profit_ratio", th.NumberType),
        th.Property("growth_research_and_development_expenses", th.NumberType),
        th.Property("growth_general_and_administrative_expenses", th.NumberType),
        th.Property("growth_selling_and_marketing_expenses", th.NumberType),
        th.Property("growth_other_expenses", th.NumberType),
        th.Property("growth_operating_expenses", th.NumberType),
        th.Property("growth_cost_and_expenses", th.NumberType),
        th.Property("growth_interest_income", th.NumberType),
        th.Property("growth_interest_expense", th.NumberType),
        th.Property("growth_depreciation_and_amortization", th.NumberType),
        th.Property("growth_ebitda", th.NumberType),
        th.Property("growth_operating_income", th.NumberType),
        th.Property("growth_income_before_tax", th.NumberType),
        th.Property("growth_income_tax_expense", th.NumberType),
        th.Property("growth_net_income", th.NumberType),
        th.Property("growth_eps", th.NumberType),
        th.Property("growth_eps_diluted", th.NumberType),
        th.Property("growth_weighted_average_shs_out", th.NumberType),
        th.Property("growth_weighted_average_shs_out_dil", th.NumberType),
        th.Property("growth_ebit", th.NumberType),
        th.Property("growth_non_operating_income_excluding_interest", th.NumberType),
        th.Property("growth_net_interest_income", th.NumberType),
        th.Property("growth_total_other_income_expenses_net", th.NumberType),
        th.Property("growth_net_income_from_continuing_operations", th.NumberType),
        th.Property("growth_other_adjustments_to_net_income", th.NumberType),
        th.Property("growth_net_income_deductions", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/income-statement-growth-bulk"


class BalanceSheetStatementBulkStream(IncrementalYearPeriodStream):
    """Stream for Balance Sheet Statement Bulk API."""

    name = "balance_sheet_statement_bulk"

    _float_fields = [
        "cash_and_cash_equivalents",
        "short_term_investments",
        "cash_and_short_term_investments",
        "net_receivables",
        "accounts_receivables",
        "other_receivables",
        "inventory",
        "prepaids",
        "other_current_assets",
        "total_current_assets",
        "property_plant_equipment_net",
        "goodwill",
        "intangible_assets",
        "goodwill_and_intangible_assets",
        "long_term_investments",
        "tax_assets",
        "other_non_current_assets",
        "total_non_current_assets",
        "other_assets",
        "total_assets",
        "total_payables",
        "account_payables",
        "other_payables",
        "accrued_expenses",
        "short_term_debt",
        "capital_lease_obligations_current",
        "tax_payables",
        "deferred_revenue",
        "other_current_liabilities",
        "total_current_liabilities",
        "long_term_debt",
        "capital_lease_obligations_non_current",
        "deferred_revenue_non_current",
        "deferred_tax_liabilities_non_current",
        "other_non_current_liabilities",
        "total_non_current_liabilities",
        "other_liabilities",
        "capital_lease_obligations",
        "total_liabilities",
        "treasury_stock",
        "preferred_stock",
        "common_stock",
        "retained_earnings",
        "additional_paid_in_capital",
        "accumulated_other_comprehensive_income_loss",
        "other_total_stockholders_equity",
        "total_stockholders_equity",
        "total_equity",
        "minority_interest",
        "total_liabilities_and_total_equity",
        "total_investments",
        "total_debt",
        "net_debt",
    ]

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("date", th.DateType),
        th.Property("symbol", th.StringType),
        th.Property("reported_currency", th.StringType),
        th.Property("cik", th.StringType),
        th.Property("filing_date", th.DateType),
        th.Property("accepted_date", th.DateTimeType),
        th.Property("fiscal_year", th.StringType),
        th.Property("period", th.StringType),
        th.Property("cash_and_cash_equivalents", th.NumberType),
        th.Property("short_term_investments", th.NumberType),
        th.Property("cash_and_short_term_investments", th.NumberType),
        th.Property("net_receivables", th.NumberType),
        th.Property("accounts_receivables", th.NumberType),
        th.Property("other_receivables", th.NumberType),
        th.Property("inventory", th.NumberType),
        th.Property("prepaids", th.NumberType),
        th.Property("other_current_assets", th.NumberType),
        th.Property("total_current_assets", th.NumberType),
        th.Property("property_plant_equipment_net", th.NumberType),
        th.Property("goodwill", th.NumberType),
        th.Property("intangible_assets", th.NumberType),
        th.Property("goodwill_and_intangible_assets", th.NumberType),
        th.Property("long_term_investments", th.NumberType),
        th.Property("tax_assets", th.NumberType),
        th.Property("other_non_current_assets", th.NumberType),
        th.Property("total_non_current_assets", th.NumberType),
        th.Property("other_assets", th.NumberType),
        th.Property("total_assets", th.NumberType),
        th.Property("total_payables", th.NumberType),
        th.Property("account_payables", th.NumberType),
        th.Property("other_payables", th.NumberType),
        th.Property("accrued_expenses", th.NumberType),
        th.Property("short_term_debt", th.NumberType),
        th.Property("capital_lease_obligations_current", th.NumberType),
        th.Property("tax_payables", th.NumberType),
        th.Property("deferred_revenue", th.NumberType),
        th.Property("other_current_liabilities", th.NumberType),
        th.Property("total_current_liabilities", th.NumberType),
        th.Property("long_term_debt", th.NumberType),
        th.Property("capital_lease_obligations_non_current", th.NumberType),
        th.Property("deferred_revenue_non_current", th.NumberType),
        th.Property("deferred_tax_liabilities_non_current", th.NumberType),
        th.Property("other_non_current_liabilities", th.NumberType),
        th.Property("total_non_current_liabilities", th.NumberType),
        th.Property("other_liabilities", th.NumberType),
        th.Property("capital_lease_obligations", th.NumberType),
        th.Property("total_liabilities", th.NumberType),
        th.Property("treasury_stock", th.NumberType),
        th.Property("preferred_stock", th.NumberType),
        th.Property("common_stock", th.NumberType),
        th.Property("retained_earnings", th.NumberType),
        th.Property("additional_paid_in_capital", th.NumberType),
        th.Property("accumulated_other_comprehensive_income_loss", th.NumberType),
        th.Property("other_total_stockholders_equity", th.NumberType),
        th.Property("total_stockholders_equity", th.NumberType),
        th.Property("total_equity", th.NumberType),
        th.Property("minority_interest", th.NumberType),
        th.Property("total_liabilities_and_total_equity", th.NumberType),
        th.Property("total_investments", th.NumberType),
        th.Property("total_debt", th.NumberType),
        th.Property("net_debt", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/balance-sheet-statement-bulk"


class BalanceSheetStatementGrowthBulkStream(IncrementalYearPeriodStream):
    """Stream for Balance Sheet Statement Growth Bulk API."""

    name = "balance_sheet_statement_growth_bulk"

    _float_fields = [
        "growth_cash_and_cash_equivalents",
        "growth_short_term_investments",
        "growth_cash_and_short_term_investments",
        "growth_net_receivables",
        "growth_inventory",
        "growth_other_current_assets",
        "growth_total_current_assets",
        "growth_property_plant_equipment_net",
        "growth_goodwill",
        "growth_intangible_assets",
        "growth_goodwill_and_intangible_assets",
        "growth_long_term_investments",
        "growth_tax_assets",
        "growth_other_non_current_assets",
        "growth_total_non_current_assets",
        "growth_other_assets",
        "growth_total_assets",
        "growth_account_payables",
        "growth_short_term_debt",
        "growth_tax_payables",
        "growth_deferred_revenue",
        "growth_other_current_liabilities",
        "growth_total_current_liabilities",
        "growth_long_term_debt",
        "growth_deferred_revenue_non_current",
        "growth_deferred_tax_liabilities_non_current",
        "growth_other_non_current_liabilities",
        "growth_total_non_current_liabilities",
        "growth_other_liabilities",
        "growth_total_liabilities",
        "growth_preferred_stock",
        "growth_common_stock",
        "growth_retained_earnings",
        "growth_accumulated_other_comprehensive_income_loss",
        "growth_othertotal_stockholders_equity",
        "growth_total_stockholders_equity",
        "growth_minority_interest",
        "growth_total_equity",
        "growth_total_liabilities_and_stockholders_equity",
        "growth_total_investments",
        "growth_total_debt",
        "growth_net_debt",
        "growth_accounts_receivables",
        "growth_other_receivables",
        "growth_prepaids",
        "growth_total_payables",
        "growth_other_payables",
        "growth_accrued_expenses",
        "growth_capital_lease_obligations_current",
        "growth_additional_paid_in_capital",
        "growth_treasury_stock",
    ]

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("date", th.DateType),
        th.Property("fiscal_year", th.StringType),
        th.Property("period", th.StringType),
        th.Property("reported_currency", th.StringType),
        th.Property("growth_cash_and_cash_equivalents", th.NumberType),
        th.Property("growth_short_term_investments", th.NumberType),
        th.Property("growth_cash_and_short_term_investments", th.NumberType),
        th.Property("growth_net_receivables", th.NumberType),
        th.Property("growth_inventory", th.NumberType),
        th.Property("growth_other_current_assets", th.NumberType),
        th.Property("growth_total_current_assets", th.NumberType),
        th.Property("growth_property_plant_equipment_net", th.NumberType),
        th.Property("growth_goodwill", th.NumberType),
        th.Property("growth_intangible_assets", th.NumberType),
        th.Property("growth_goodwill_and_intangible_assets", th.NumberType),
        th.Property("growth_long_term_investments", th.NumberType),
        th.Property("growth_tax_assets", th.NumberType),
        th.Property("growth_other_non_current_assets", th.NumberType),
        th.Property("growth_total_non_current_assets", th.NumberType),
        th.Property("growth_other_assets", th.NumberType),
        th.Property("growth_total_assets", th.NumberType),
        th.Property("growth_account_payables", th.NumberType),
        th.Property("growth_short_term_debt", th.NumberType),
        th.Property("growth_tax_payables", th.NumberType),
        th.Property("growth_deferred_revenue", th.NumberType),
        th.Property("growth_other_current_liabilities", th.NumberType),
        th.Property("growth_total_current_liabilities", th.NumberType),
        th.Property("growth_long_term_debt", th.NumberType),
        th.Property("growth_deferred_revenue_non_current", th.NumberType),
        th.Property("growth_deferred_tax_liabilities_non_current", th.NumberType),
        th.Property("growth_other_non_current_liabilities", th.NumberType),
        th.Property("growth_total_non_current_liabilities", th.NumberType),
        th.Property("growth_other_liabilities", th.NumberType),
        th.Property("growth_total_liabilities", th.NumberType),
        th.Property("growth_preferred_stock", th.NumberType),
        th.Property("growth_common_stock", th.NumberType),
        th.Property("growth_retained_earnings", th.NumberType),
        th.Property(
            "growth_accumulated_other_comprehensive_income_loss", th.NumberType
        ),
        th.Property("growth_othertotal_stockholders_equity", th.NumberType),
        th.Property("growth_total_stockholders_equity", th.NumberType),
        th.Property("growth_minority_interest", th.NumberType),
        th.Property("growth_total_equity", th.NumberType),
        th.Property("growth_total_liabilities_and_stockholders_equity", th.NumberType),
        th.Property("growth_total_investments", th.NumberType),
        th.Property("growth_total_debt", th.NumberType),
        th.Property("growth_net_debt", th.NumberType),
        th.Property("growth_accounts_receivables", th.NumberType),
        th.Property("growth_other_receivables", th.NumberType),
        th.Property("growth_prepaids", th.NumberType),
        th.Property("growth_total_payables", th.NumberType),
        th.Property("growth_other_payables", th.NumberType),
        th.Property("growth_accrued_expenses", th.NumberType),
        th.Property("growth_capital_lease_obligations_current", th.NumberType),
        th.Property("growth_additional_paid_in_capital", th.NumberType),
        th.Property("growth_treasury_stock", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/balance-sheet-statement-growth-bulk"


class CashFlowStatementBulkStream(IncrementalYearPeriodStream):
    """Stream for Cash Flow Statement Bulk API."""

    name = "cash_flow_statement_bulk"

    _float_fields = [
        "net_income",
        "depreciation_and_amortization",
        "deferred_income_tax",
        "stock_based_compensation",
        "change_in_working_capital",
        "accounts_receivables",
        "inventory",
        "accounts_payables",
        "other_working_capital",
        "other_non_cash_items",
        "net_cash_provided_by_operating_activities",
        "investments_in_property_plant_and_equipment",
        "acquisitions_net",
        "purchases_of_investments",
        "sales_maturities_of_investments",
        "other_investing_activities",
        "net_cash_provided_by_investing_activities",
        "net_debt_issuance",
        "long_term_net_debt_issuance",
        "short_term_net_debt_issuance",
        "net_stock_issuance",
        "net_common_stock_issuance",
        "common_stock_issuance",
        "common_stock_repurchased",
        "net_preferred_stock_issuance",
        "net_dividends_paid",
        "common_dividends_paid",
        "preferred_dividends_paid",
        "other_financing_activities",
        "net_cash_provided_by_financing_activities",
        "effect_of_forex_changes_on_cash",
        "net_change_in_cash",
        "cash_at_end_of_period",
        "cash_at_beginning_of_period",
        "operating_cash_flow",
        "capital_expenditure",
        "free_cash_flow",
        "income_taxes_paid",
        "interest_paid",
    ]

    _date_fields = ["filing_date", "accepted_date", "ipo_date"]

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("date", th.DateType),
        th.Property("symbol", th.StringType),
        th.Property("reported_currency", th.StringType),
        th.Property("cik", th.StringType),
        th.Property("filing_date", th.DateType),
        th.Property("accepted_date", th.DateTimeType),
        th.Property("fiscal_year", th.StringType),
        th.Property("period", th.StringType),
        th.Property("net_income", th.NumberType),
        th.Property("depreciation_and_amortization", th.NumberType),
        th.Property("deferred_income_tax", th.NumberType),
        th.Property("stock_based_compensation", th.NumberType),
        th.Property("change_in_working_capital", th.NumberType),
        th.Property("accounts_receivables", th.NumberType),
        th.Property("inventory", th.NumberType),
        th.Property("accounts_payables", th.NumberType),
        th.Property("other_working_capital", th.NumberType),
        th.Property("other_non_cash_items", th.NumberType),
        th.Property("net_cash_provided_by_operating_activities", th.NumberType),
        th.Property("investments_in_property_plant_and_equipment", th.NumberType),
        th.Property("acquisitions_net", th.NumberType),
        th.Property("purchases_of_investments", th.NumberType),
        th.Property("sales_maturities_of_investments", th.NumberType),
        th.Property("other_investing_activities", th.NumberType),
        th.Property("net_cash_provided_by_investing_activities", th.NumberType),
        th.Property("net_debt_issuance", th.NumberType),
        th.Property("long_term_net_debt_issuance", th.NumberType),
        th.Property("short_term_net_debt_issuance", th.NumberType),
        th.Property("net_stock_issuance", th.NumberType),
        th.Property("net_common_stock_issuance", th.NumberType),
        th.Property("common_stock_issuance", th.NumberType),
        th.Property("common_stock_repurchased", th.NumberType),
        th.Property("net_preferred_stock_issuance", th.NumberType),
        th.Property("net_dividends_paid", th.NumberType),
        th.Property("common_dividends_paid", th.NumberType),
        th.Property("preferred_dividends_paid", th.NumberType),
        th.Property("other_financing_activities", th.NumberType),
        th.Property("net_cash_provided_by_financing_activities", th.NumberType),
        th.Property("effect_of_forex_changes_on_cash", th.NumberType),
        th.Property("net_change_in_cash", th.NumberType),
        th.Property("cash_at_end_of_period", th.NumberType),
        th.Property("cash_at_beginning_of_period", th.NumberType),
        th.Property("operating_cash_flow", th.NumberType),
        th.Property("capital_expenditure", th.NumberType),
        th.Property("free_cash_flow", th.NumberType),
        th.Property("income_taxes_paid", th.NumberType),
        th.Property("interest_paid", th.NumberType),
        th.Property("ipo_date", th.DateType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/cash-flow-statement-bulk"


class CashFlowStatementGrowthBulkStream(IncrementalYearPeriodStream):
    """Stream for Cash Flow Statement Growth Bulk API."""

    name = "cash_flow_statement_growth_bulk"

    _float_fields = [
        "growth_net_income",
        "growth_depreciation_and_amortization",
        "growth_deferred_income_tax",
        "growth_stock_based_compensation",
        "growth_change_in_working_capital",
        "growth_accounts_receivables",
        "growth_inventory",
        "growth_accounts_payables",
        "growth_other_working_capital",
        "growth_other_non_cash_items",
        "growth_net_cash_provided_by_operating_activites",
        "growth_investments_in_property_plant_and_equipment",
        "growth_acquisitions_net",
        "growth_purchases_of_investments",
        "growth_sales_maturities_of_investments",
        "growth_other_investing_activites",
        "growth_net_cash_used_for_investing_activites",
        "growth_debt_repayment",
        "growth_common_stock_issued",
        "growth_common_stock_repurchased",
        "growth_dividends_paid",
        "growth_other_financing_activites",
        "growth_net_cash_used_provided_by_financing_activities",
        "growth_effect_of_forex_changes_on_cash",
        "growth_net_change_in_cash",
        "growth_cash_at_end_of_period",
        "growth_cash_at_beginning_of_period",
        "growth_operating_cash_flow",
        "growth_capital_expenditure",
        "growth_free_cash_flow",
        "growth_net_debt_issuance",
        "growth_long_term_net_debt_issuance",
        "growth_short_term_net_debt_issuance",
        "growth_net_stock_issuance",
        "growth_preferred_dividends_paid",
        "growth_income_taxes_paid",
        "growth_interest_paid",
    ]

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("date", th.DateType),
        th.Property("fiscal_year", th.StringType),
        th.Property("period", th.StringType),
        th.Property("reported_currency", th.StringType),
        th.Property("growth_net_income", th.NumberType),
        th.Property("growth_depreciation_and_amortization", th.NumberType),
        th.Property("growth_deferred_income_tax", th.NumberType),
        th.Property("growth_stock_based_compensation", th.NumberType),
        th.Property("growth_change_in_working_capital", th.NumberType),
        th.Property("growth_accounts_receivables", th.NumberType),
        th.Property("growth_inventory", th.NumberType),
        th.Property("growth_accounts_payables", th.NumberType),
        th.Property("growth_other_working_capital", th.NumberType),
        th.Property("growth_other_non_cash_items", th.NumberType),
        th.Property("growth_net_cash_provided_by_operating_activites", th.NumberType),
        th.Property(
            "growth_investments_in_property_plant_and_equipment", th.NumberType
        ),
        th.Property("growth_acquisitions_net", th.NumberType),
        th.Property("growth_purchases_of_investments", th.NumberType),
        th.Property("growth_sales_maturities_of_investments", th.NumberType),
        th.Property("growth_other_investing_activites", th.NumberType),
        th.Property("growth_net_cash_used_for_investing_activites", th.NumberType),
        th.Property("growth_debt_repayment", th.NumberType),
        th.Property("growth_common_stock_issued", th.NumberType),
        th.Property("growth_common_stock_repurchased", th.NumberType),
        th.Property("growth_dividends_paid", th.NumberType),
        th.Property("growth_other_financing_activites", th.NumberType),
        th.Property(
            "growth_net_cash_used_provided_by_financing_activities", th.NumberType
        ),
        th.Property("growth_effect_of_forex_changes_on_cash", th.NumberType),
        th.Property("growth_net_change_in_cash", th.NumberType),
        th.Property("growth_cash_at_end_of_period", th.NumberType),
        th.Property("growth_cash_at_beginning_of_period", th.NumberType),
        th.Property("growth_operating_cash_flow", th.NumberType),
        th.Property("growth_capital_expenditure", th.NumberType),
        th.Property("growth_free_cash_flow", th.NumberType),
        th.Property("growth_net_debt_issuance", th.NumberType),
        th.Property("growth_long_term_net_debt_issuance", th.NumberType),
        th.Property("growth_short_term_net_debt_issuance", th.NumberType),
        th.Property("growth_net_stock_issuance", th.NumberType),
        th.Property("growth_preferred_dividends_paid", th.NumberType),
        th.Property("growth_income_taxes_paid", th.NumberType),
        th.Property("growth_interest_paid", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/cash-flow-statement-growth-bulk"


class EodBulkStream(IncrementalDateStream):
    """Stream for EOD Bulk API."""

    name = "eod_bulk"
    _expect_csv = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("date", th.DateType),
        th.Property("open", th.NumberType),
        th.Property("low", th.NumberType),
        th.Property("high", th.NumberType),
        th.Property("close", th.NumberType),
        th.Property("adj_close", th.NumberType),
        th.Property("volume", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/eod-bulk"

    def get_starting_timestamp(self, context: Context | None) -> str | None:
        """Get the starting timestamp for the stream."""
        start_date = super().get_starting_timestamp(context)
        date_gte = self.stream_config.get("other_params", {}).get("date_gte")

        if date_gte:
            return max(start_date, date_gte)

        return start_date

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Override to add date_lte filtering and skip dates that fail after 3 retries."""
        dates_dict = self._get_dates_dict()
        starting_date = self.get_starting_timestamp(context)
        other_params = self.stream_config.get("other_params", {})
        date_lte = other_params.get("date_lte")

        # Apply both date_gte (via starting_date) and date_lte filtering
        filtered_dates = [d for d in dates_dict if d["date"] >= starting_date]
        if date_lte:
            filtered_dates = [d for d in filtered_dates if d["date"] <= date_lte]

        for date_dict in filtered_dates:
            current_date = date_dict.get("date", "unknown")

            try:
                self.query_params.update(date_dict)
                yield from super(IncrementalDateStream, self).get_records(context)

            except Exception as e:
                # Catch all exceptions that come from the 3-retry failure
                self.logger.warning(
                    f"EOD Bulk: Failed for date {current_date} after 3 retries, skipping to next date. "
                    f"Error: {self.redact_api_key(e)}"
                )
                # Yield minimal record to advance bookmark past failed date
                yield {
                    "surrogate_key": f"eod_bulk_skip_{current_date}",
                    "symbol": "__TIMEOUT_FAILURE__",
                    "date": current_date,
                    "open": None,
                    "low": None,
                    "high": None,
                    "close": None,
                    "adj_close": None,
                    "volume": None,
                }
                continue

    def post_process(self, record: dict, context: Context | None = None) -> dict:
        for col in ["open", "high", "low", "close", "adj_close", "volume"]:
            if col in record and not isinstance(record[col], (int, float, Decimal)):
                if record[col] not in (None, ""):
                    try:
                        record[col] = Decimal(record[col])
                    except Exception:
                        record[col] = float(record[col])
                else:
                    record[col] = None
        return super().post_process(record, context)
