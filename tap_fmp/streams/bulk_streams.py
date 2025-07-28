"""Bulk stream types classes for tap-fmp."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_fmp.client import FmpRestStream


class PartitionedBulkStream(FmpRestStream):
    """Base class for bulk streams that need partitioning by part parameter."""

    _paginate = True
    _paginate_key = "part"

    # TODO: Update client.py to have self.page_key as a param so we can pass "part" instead of page. This file is just a
    # placeholder for now. Need to test these streams as a follow-up --> cannot query these streams with starter plan.

    @property
    def partitions(self):
        query_params_part = self.query_params.get("part")
        other_params_parts = self.config.get("other_params", {}).get("parts")

        if query_params_part is not None:
            return [{"part": str(query_params_part)}]
        elif other_params_parts:
            return [{"part": str(part)} for part in other_params_parts]
        else:
            return [{"part": "0"}]

    def get_records(self, context: Context | None):
        self.query_params.update(context)
        return super().get_records(context)


class YearPeriodPartitionedBulkStream(FmpRestStream):
    """Base class for bulk streams that need partitioning by year and period."""

    @property
    def partitions(self):
        query_params_year = self.query_params.get("year")
        query_params_period = self.query_params.get("period")

        other_params_years = self.config.get("other_params", {}).get("years")
        other_params_periods = self.config.get("other_params", {}).get("periods")

        if query_params_year:
            years = (
                [query_params_year]
                if isinstance(query_params_year, str)
                else query_params_year
            )
        elif other_params_years:
            years = (
                other_params_years
                if isinstance(other_params_years, list)
                else [other_params_years]
            )
        else:
            years = ["2024", "2023", "2022"]  # Default years

        if query_params_period:
            periods = (
                [query_params_period]
                if isinstance(query_params_period, str)
                else query_params_period
            )
        elif other_params_periods:
            periods = (
                other_params_periods
                if isinstance(other_params_periods, list)
                else [other_params_periods]
            )
        else:
            periods = ["FY"]

        partitions = []
        for year in years:
            for period in periods:
                partitions.append({"year": str(year), "period": str(period)})
        return partitions

    def get_records(self, context: Context | None):
        self.query_params.update(context)
        return super().get_records(context)


class CompanyProfileBulkStream(PartitionedBulkStream):
    """Stream for Company Profile Bulk API."""

    name = "company_profile_bulk"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("price", th.NumberType),
        th.Property("market_cap", th.NumberType),
        th.Property("beta", th.NumberType),
        th.Property("last_dividend", th.NumberType),
        th.Property("range", th.StringType),
        th.Property("change", th.NumberType),
        th.Property("change_percentage", th.NumberType),
        th.Property("volume", th.IntegerType),
        th.Property("average_volume", th.IntegerType),
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
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/profile-bulk"


class StockRatingBulkStream(FmpRestStream):
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


class DcfValuationsBulkStream(FmpRestStream):
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


class FinancialScoresBulkStream(FmpRestStream):
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


class PriceTargetSummaryBulkStream(FmpRestStream):
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


class EtfHolderBulkStream(PartitionedBulkStream):
    """Stream for ETF Holder Bulk API."""

    name = "etf_holder_bulk"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("name", th.StringType),
        th.Property("shares_number", th.IntegerType),
        th.Property("asset", th.StringType),
        th.Property("weight_percentage", th.NumberType),
        th.Property("cusip", th.StringType),
        th.Property("isin", th.StringType),
        th.Property("market_value", th.NumberType),
        th.Property("last_updated", th.DateType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/etf-holder-bulk"


class UpgradesDowngradesConsensusBulkStream(FmpRestStream):
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


class KeyMetricsTtmBulkStream(FmpRestStream):
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


class RatiosTtmBulkStream(FmpRestStream):
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


class StockPeersBulkStream(FmpRestStream):
    """Stream for Stock Peers Bulk API."""

    name = "stock_peers_bulk"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("peers", th.StringType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/peers-bulk"


class EarningsSurprisesBulkStream(YearPeriodPartitionedBulkStream):
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

    @property
    def partitions(self):
        query_params_year = self.query_params.get("year")
        other_params_years = self.config.get("other_params", {}).get("years")

        if query_params_year:
            years = (
                [query_params_year]
                if isinstance(query_params_year, str)
                else query_params_year
            )
        elif other_params_years:
            years = (
                other_params_years
                if isinstance(other_params_years, list)
                else [other_params_years]
            )
        else:
            years = ["2024", "2023", "2022"]  # Default years

        return [{"year": str(year)} for year in years]

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/earnings-surprises-bulk"


class IncomeStatementBulkStream(YearPeriodPartitionedBulkStream):
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


class IncomeStatementGrowthBulkStream(YearPeriodPartitionedBulkStream):
    """Stream for Income Statement Growth Bulk API."""

    name = "income_statement_growth_bulk"

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


class BalanceSheetStatementBulkStream(YearPeriodPartitionedBulkStream):
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
        th.Property("additional_paid_in_capital", th.StringType),
        th.Property("accumulated_other_comprehensive_income_loss", th.StringType),
        th.Property("other_total_stockholders_equity", th.StringType),
        th.Property("total_stockholders_equity", th.StringType),
        th.Property("total_equity", th.StringType),
        th.Property("minority_interest", th.StringType),
        th.Property("total_liabilities_and_total_equity", th.StringType),
        th.Property("total_investments", th.StringType),
        th.Property("total_debt", th.StringType),
        th.Property("net_debt", th.StringType),
    ).to_dict()


class BalanceSheetStatementGrowthBulkStream(YearPeriodPartitionedBulkStream):
    """Stream for Balance Sheet Statement Growth Bulk API."""

    name = "balance_sheet_statement_growth_bulk"

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


class CashFlowStatementBulkStream(YearPeriodPartitionedBulkStream):
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
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/cash-flow-statement-bulk"


class CashFlowStatementGrowthBulkStream(YearPeriodPartitionedBulkStream):
    """Stream for Cash Flow Statement Growth Bulk API."""

    name = "cash_flow_statement_growth_bulk"

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


class EodBulkStream(FmpRestStream):
    """Stream for EOD Bulk API."""

    name = "eod_bulk"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("date", th.DateType),
        th.Property("open", th.NumberType),
        th.Property("low", th.NumberType),
        th.Property("high", th.NumberType),
        th.Property("close", th.NumberType),
        th.Property("adj_close", th.NumberType),
        th.Property("volume", th.IntegerType),
    ).to_dict()

    @property
    def partitions(self):
        query_params_date = self.query_params.get("date")
        other_params_dates = self.config.get("other_params", {}).get("dates")

        if query_params_date:
            dates = (
                [query_params_date]
                if isinstance(query_params_date, str)
                else query_params_date
            )
        elif other_params_dates:
            dates = (
                other_params_dates
                if isinstance(other_params_dates, list)
                else [other_params_dates]
            )
        else:
            dates = ["2024-10-22"]
        return [{"date": str(date)} for date in dates]

    def get_records(self, context: Context | None):
        self.query_params.update(context)
        return super().get_records(context)

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/eod-bulk"
