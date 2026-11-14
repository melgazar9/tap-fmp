"""Bulk stream types classes for tap-fmp."""

from __future__ import annotations

import itertools
import typing as t
from functools import cached_property
import requests
from singer_sdk import typing as th
from singer_sdk.helpers.types import Context
from tap_fmp.client import FmpSurrogateKeyStream, IncrementalDateStream
from tap_fmp.helpers import iso_date, whole_count
from datetime import date, datetime
from singer_sdk.exceptions import ConfigValidationError
from decimal import Decimal


class BaseBulkStream(FmpSurrogateKeyStream):
    """A bulk endpoint's CSV carries every value as text; each column is
    converted by its schema type, and a value that doesn't fit stops the stream."""

    _expect_csv = True
    # FMP may restrict a key that keeps calling a bulk file, so its 429s are retried at a slow even pace
    _quota_retry_interval_seconds: t.ClassVar[float] = 300
    _quota_exhausted_after_seconds = 1800
    _csv_booleans: t.ClassVar[dict[str, bool]] = {"true": True, "false": False}
    # columns whose text follows a rule of its own rather than their schema type's
    _column_converters: t.ClassVar[dict[str, t.Callable[[str], t.Any]]] = {}

    def _quota_retry_waits(self) -> t.Iterator[float]:
        return itertools.repeat(self._quota_retry_interval_seconds)

    @cached_property
    def _text_converters(self) -> dict[str, t.Callable[[str], t.Any]]:
        converters = {}
        for column, prop in self.schema["properties"].items():
            types = prop.get("type", [])
            types = types if isinstance(types, list) else [types]
            if column in self._column_converters:
                converters[column] = self._column_converters[column]
            elif "boolean" in types:
                converters[column] = self._boolean_from_text
            elif "integer" in types:
                converters[column] = self._integer_from_text
            elif "number" in types:
                converters[column] = Decimal
            elif prop.get("format") == "date":
                converters[column] = iso_date
            elif prop.get("format") == "date-time":
                converters[column] = datetime.fromisoformat
        return converters

    def _boolean_from_text(self, text: str) -> bool:
        return self._csv_booleans[text.lower()]

    @staticmethod
    def _integer_from_text(text: str) -> int:
        number = Decimal(text)
        if number != number.to_integral_value():
            raise ValueError("not a whole number")
        return int(number)

    def post_process(self, record: dict, context: Context | None = None) -> dict:
        for column, convert in self._text_converters.items():
            value = record.get(column)
            if not isinstance(value, str):
                continue
            text = value.strip()
            try:
                record[column] = convert(text) if text else None
            except (ValueError, ArithmeticError, KeyError) as e:
                raise ValueError(
                    f"Stream {self.name}: column {column} can't hold FMP's value {value!r} ({e!r})"
                ) from None
        return super().post_process(record, context)

    def _requested_years(self) -> list[int]:
        """Years from `years` in other_params ("*" or none means all), never before start_date's year."""
        other_params = self.stream_config.get("other_params", {})
        if "year" in self.query_params and "years" in other_params:
            raise ConfigValidationError(
                f"Stream {self.name}: cannot specify both 'year' in query_params and 'years' in other_params."
            )
        if "year" in self.query_params:
            return [int(self.query_params["year"])]

        first_year = datetime.fromisoformat(str(self.config["start_date"])[:10]).year
        years = other_params.get("years")
        if years in (None, "*", ["*"]):
            return list(range(first_year, datetime.today().year + 1))
        if isinstance(years, (str, int)):
            years = [years]
        return sorted(int(year) for year in years if int(year) >= first_year)


class PaginatedBulkStream(BaseBulkStream):
    """Bulk file split into numbered parts. Each run reads every part (or the
    configured ones): the file is a fresh snapshot, so a bookmark on the part
    would skip the parts before it."""

    _paginate = True
    _paginate_key = "part"
    # FMP's reply to a part number past the file's last; on the first part it is a fault
    _past_last_part_reply: t.ClassVar[str] = (
        "Query Error: Invalid or missing query parameter - part"
    )

    def _is_past_last_page(self, response: requests.Response, page: int | None) -> bool:
        if super()._is_past_last_page(response, page):
            return True
        return (
            response.status_code == 400
            and bool(page)
            and response.text.strip() == self._past_last_part_reply
        )

    def _configured_parts(self) -> list[int] | None:
        """`other_params.parts`; none or "*" means every part, and `query_params.part` reads that part alone."""
        parts = self.stream_config.get("other_params", {}).get("parts")
        if "part" in self.query_params and parts is not None:
            raise ConfigValidationError(
                "Cannot specify both 'part' in query_params and 'parts' in other_params."
            )
        if parts in (None, "*", ["*"]):
            return None
        if isinstance(parts, (str, int)):
            parts = [parts]
        return [int(part) for part in parts]

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        parts = self._configured_parts()
        if parts is None:
            yield from super().get_records(context)
            return
        url = self.get_url(context)
        for part in parts:
            records = self._fetch_with_retry(url, self.query_params, part)
            yield from self._yield_processed(records, context)

    def post_process(self, row: dict, context: Context | None = None) -> dict:
        row["_part"] = self.query_params["part"]
        return super().post_process(row, context)


class IncrementalYearPeriodStream(BaseBulkStream):
    replication_key = "date"
    replication_method = "INCREMENTAL"

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
        starting_year = datetime.fromisoformat(
            str(self.get_starting_timestamp(context))[:10]
        ).year
        filtered_years = [y for y in self._requested_years() if y >= starting_year]

        periods = self._get_periods()

        updated_query_params = [
            {"year": y, "period": p} for y in filtered_years for p in periods
        ]

        for query_dict in updated_query_params:
            self.query_params.update(query_dict)
            yield from super().get_records(context)


class TtmBulkStream(BaseBulkStream):
    pass


class CompanyProfileBulkStream(PaginatedBulkStream):
    """Stream for Company Profile Bulk API."""

    name = "company_profile_bulk"
    _column_converters = {"full_time_employees": whole_count}

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
        th.Property("research_and_development_to_revenue_ttm", th.NumberType),
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
        th.Property("price_to_earnings_diluted_ratio_ttm", th.NumberType),
        th.Property("price_to_earnings_diluted_growth_ratio_ttm", th.NumberType),
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

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("date", th.DateType),
        th.Property("eps_actual", th.NumberType),
        th.Property("eps_estimated", th.NumberType),
        th.Property("last_updated", th.DateType),
    ).to_dict()

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        for year in self._requested_years():
            self.query_params.update({"year": str(year)})
            yield from super().get_records(context)

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/earnings-surprises-bulk"


class IncomeStatementBulkStream(IncrementalYearPeriodStream):
    """Stream for Income Statement Bulk API."""

    name = "income_statement_bulk"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("date", th.DateType),
        th.Property("symbol", th.StringType),
        th.Property("reported_currency", th.StringType),
        th.Property("cik", th.StringType),
        th.Property("filing_date", th.DateType),
        th.Property("accepted_date", th.DateTimeType),
        th.Property("fiscal_year", th.IntegerType),
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

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("date", th.DateType),
        th.Property("fiscal_year", th.IntegerType),
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

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("date", th.DateType),
        th.Property("symbol", th.StringType),
        th.Property("reported_currency", th.StringType),
        th.Property("cik", th.StringType),
        th.Property("filing_date", th.DateType),
        th.Property("accepted_date", th.DateTimeType),
        th.Property("fiscal_year", th.IntegerType),
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

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("date", th.DateType),
        th.Property("fiscal_year", th.IntegerType),
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
        th.Property("growth_other_total_stockholders_equity", th.NumberType),
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

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("date", th.DateType),
        th.Property("symbol", th.StringType),
        th.Property("reported_currency", th.StringType),
        th.Property("cik", th.StringType),
        th.Property("filing_date", th.DateType),
        th.Property("accepted_date", th.DateTimeType),
        th.Property("fiscal_year", th.IntegerType),
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
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/cash-flow-statement-bulk"


class CashFlowStatementGrowthBulkStream(IncrementalYearPeriodStream):
    """Stream for Cash Flow Statement Growth Bulk API."""

    name = "cash_flow_statement_growth_bulk"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("date", th.DateType),
        th.Property("fiscal_year", th.IntegerType),
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
        th.Property("growth_net_cash_provided_by_operating_activities", th.NumberType),
        th.Property(
            "growth_investments_in_property_plant_and_equipment", th.NumberType
        ),
        th.Property("growth_acquisitions_net", th.NumberType),
        th.Property("growth_purchases_of_investments", th.NumberType),
        th.Property("growth_sales_maturities_of_investments", th.NumberType),
        th.Property("growth_other_investing_activities", th.NumberType),
        th.Property("growth_net_cash_used_for_investing_activities", th.NumberType),
        th.Property("growth_debt_repayment", th.NumberType),
        th.Property("growth_common_stock_issued", th.NumberType),
        th.Property("growth_common_stock_repurchased", th.NumberType),
        th.Property("growth_dividends_paid", th.NumberType),
        th.Property("growth_other_financing_activities", th.NumberType),
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


class EodBulkStream(BaseBulkStream, IncrementalDateStream):
    """Stream for EOD Bulk API."""

    name = "eod_bulk"
    primary_keys = ["symbol", "date"]
    # Dates are requested in order, so an interrupted backfill resumes at its last date instead of from the start.
    is_sorted = True

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
        """Override to add date_lte filtering. A date that fails ends the stream so its bookmark stays on that date."""
        dates_dict = self._get_dates_dict()
        starting_date = self.get_starting_timestamp(context)
        other_params = self.stream_config.get("other_params", {})
        date_lte = other_params.get("date_lte")

        # Apply both date_gte (via starting_date) and date_lte filtering
        filtered_dates = [d for d in dates_dict if d["date"] >= starting_date]
        if date_lte:
            filtered_dates = [d for d in filtered_dates if d["date"] <= date_lte]

        for date_dict in filtered_dates:
            requested_date = date.fromisoformat(date_dict["date"])
            self.query_params.update(date_dict)
            for record in super(IncrementalDateStream, self).get_records(context):
                # The bookmark follows the row date, so a row from another date would move it past dates not yet read.
                if record.get("date") != requested_date:
                    raise ValueError(
                        f"eod_bulk: requested {requested_date} but FMP returned "
                        f"{record.get('symbol')} dated {record.get('date')}"
                    )
                yield record
