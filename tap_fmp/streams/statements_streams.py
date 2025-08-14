"""Stream type classes for tap-fmp."""

from __future__ import annotations

from singer_sdk.helpers.types import Context
from singer_sdk import typing as th
from datetime import datetime

from tap_fmp.client import (
    FmpRestStream,
    SymbolPartitionStream,
    SymbolPartitionPeriodPartitionStream,
)


class StatementStream(SymbolPartitionPeriodPartitionStream):
    _add_surrogate_key = True

    def post_process(self, row: dict, context: Context | None = None) -> dict:
        if "fiscal_year" in row:
            row["fiscal_year"] = int(row["fiscal_year"])
        return super().post_process(row, context)


class TtmStream(SymbolPartitionStream):
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True

    def post_process(self, row: dict, context: Context = None) -> dict:
        if "fiscal_year" in row:
            row["fiscal_year"] = int(row["fiscal_year"])
        return super().post_process(row, context)


class IncomeStatementStream(StatementStream):
    name = "income_statement"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("date", th.DateType, required=True),
        th.Property("symbol", th.StringType, required=True),
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

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/income-statement"


class BalanceSheetStream(StatementStream):
    name = "balance_sheet"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("date", th.DateType, required=True),
        th.Property("symbol", th.StringType, required=True),
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
        th.Property("capital_lease_obligations_non_current", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/balance-sheet-statement"


class CashFlowStream(StatementStream):
    name = "cash_flow"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType),
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

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/cash-flow-statement"


class LatestFinancialStatementsStream(FmpRestStream):
    name = "latest_financial_statements"
    _paginate = True
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("date", th.DateType),
        th.Property("calendar_year", th.IntegerType),
        th.Property("period", th.StringType),
        th.Property("date_added", th.DateTimeType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/latest-financial-statements"


class IncomeStatementTtmStream(TtmStream):
    name = "income_statement_ttm"

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

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/income-statement-ttm"


class BalanceSheetTtmStream(TtmStream):
    name = "balance_sheet_ttm"

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
        th.Property("capital_lease_obligations_non_current", th.NumberType),
    ).to_dict()

    _add_surrogate_key = True

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/balance-sheet-statement-ttm"


class CashFlowTtmStream(TtmStream):
    """Cash flow statement data for companies."""

    name = "cash_flow_ttm"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("date", th.DateType, required=True),
        th.Property("symbol", th.StringType, required=True),
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

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/cash-flow-statement-ttm"


class KeyMetricsStream(StatementStream):
    """Key metrics data for companies."""

    name = "key_metrics"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("date", th.DateType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("period", th.StringType),
        th.Property("revenue_per_share", th.NumberType),
        th.Property("net_income_per_share", th.NumberType),
        th.Property("operating_cash_flow_per_share", th.NumberType),
        th.Property("free_cash_flow_per_share", th.NumberType),
        th.Property("cash_per_share", th.NumberType),
        th.Property("book_value_per_share", th.NumberType),
        th.Property("tangible_book_value_per_share", th.NumberType),
        th.Property("shareholders_equity_per_share", th.NumberType),
        th.Property("interest_debt_per_share", th.NumberType),
        th.Property("market_cap", th.NumberType),
        th.Property("enterprise_value", th.NumberType),
        th.Property("pe_ratio", th.NumberType),
        th.Property("price_to_sales_ratio", th.NumberType),
        th.Property("pocf_ratio", th.NumberType),
        th.Property("pfcf_ratio", th.NumberType),
        th.Property("pb_ratio", th.NumberType),
        th.Property("ptb_ratio", th.NumberType),
        th.Property("ev_to_sales", th.NumberType),
        th.Property("enterprise_value_over_ebitda", th.NumberType),
        th.Property("ev_to_operating_cash_flow", th.NumberType),
        th.Property("ev_to_free_cash_flow", th.NumberType),
        th.Property("earnings_yield", th.NumberType),
        th.Property("free_cash_flow_yield", th.NumberType),
        th.Property("debt_to_equity", th.NumberType),
        th.Property("debt_to_assets", th.NumberType),
        th.Property("net_debt_to_ebitda", th.NumberType),
        th.Property("current_ratio", th.NumberType),
        th.Property("interest_coverage", th.NumberType),
        th.Property("income_quality", th.NumberType),
        th.Property("dividend_yield", th.NumberType),
        th.Property("payout_ratio", th.NumberType),
        th.Property("sales_general_and_administrative_to_revenue", th.NumberType),
        th.Property("research_and_development_to_revenue", th.NumberType),
        th.Property("intangibles_to_total_assets", th.NumberType),
        th.Property("capex_to_operating_cash_flow", th.NumberType),
        th.Property("capex_to_revenue", th.NumberType),
        th.Property("capex_to_depreciation", th.NumberType),
        th.Property("stock_based_compensation_to_revenue", th.NumberType),
        th.Property("graham_number", th.NumberType),
        th.Property("roic", th.NumberType),
        th.Property("return_on_tangible_assets", th.NumberType),
        th.Property("graham_net_net", th.NumberType),
        th.Property("working_capital", th.NumberType),
        th.Property("tangible_asset_value", th.NumberType),
        th.Property("net_current_asset_value", th.NumberType),
        th.Property("invested_capital", th.NumberType),
        th.Property("average_receivables", th.NumberType),
        th.Property("average_payables", th.NumberType),
        th.Property("average_inventory", th.NumberType),
        th.Property("days_sales_outstanding", th.NumberType),
        th.Property("days_payables_outstanding", th.NumberType),
        th.Property("days_of_inventory_on_hand", th.NumberType),
        th.Property("receivables_turnover", th.NumberType),
        th.Property("payables_turnover", th.NumberType),
        th.Property("inventory_turnover", th.NumberType),
        th.Property("roe", th.NumberType),
        th.Property("capex_per_share", th.NumberType),
        th.Property("reported_currency", th.StringType),
        th.Property("return_on_assets", th.NumberType),
        th.Property("return_on_invested_capital", th.NumberType),
        th.Property("fiscal_year", th.IntegerType),
        th.Property("free_cash_flow_to_firm", th.NumberType),
        th.Property("return_on_equity", th.NumberType),
        th.Property("days_of_payables_outstanding", th.NumberType),
        th.Property("operating_cycle", th.NumberType),
        th.Property("net_debt_toebitda", th.NumberType),
        th.Property("days_of_inventory_outstanding", th.NumberType),
        th.Property("ev_toebitda", th.NumberType),
        th.Property("return_on_capital_employed", th.NumberType),
        th.Property("free_cash_flow_to_equity", th.NumberType),
        th.Property("tax_burden", th.NumberType),
        th.Property("cash_conversion_cycle", th.NumberType),
        th.Property("operating_return_on_assets", th.NumberType),
        th.Property("interest_burden", th.NumberType),
        th.Property("days_of_sales_outstanding", th.NumberType),
        th.Property("research_and_developement_to_revenue", th.NumberType),
        th.Property("ev_to_ebitda", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/key-metrics"


class FinancialRatiosStream(StatementStream):
    """Financial ratios data for companies."""

    name = "financial_ratios"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("date", th.StringType, required=True),
        th.Property("fiscal_year", th.IntegerType),
        th.Property("period", th.StringType),
        th.Property("reported_currency", th.StringType),
        th.Property("gross_profit_margin", th.NumberType),
        th.Property("ebit_margin", th.NumberType),
        th.Property("ebitda_margin", th.NumberType),
        th.Property("operating_profit_margin", th.NumberType),
        th.Property("pretax_profit_margin", th.NumberType),
        th.Property("continuous_operations_profit_margin", th.NumberType),
        th.Property("net_profit_margin", th.NumberType),
        th.Property("bottom_line_profit_margin", th.NumberType),
        th.Property("receivables_turnover", th.NumberType),
        th.Property("payables_turnover", th.NumberType),
        th.Property("inventory_turnover", th.NumberType),
        th.Property("fixed_asset_turnover", th.NumberType),
        th.Property("asset_turnover", th.NumberType),
        th.Property("current_ratio", th.NumberType),
        th.Property("quick_ratio", th.NumberType),
        th.Property("solvency_ratio", th.NumberType),
        th.Property("cash_ratio", th.NumberType),
        th.Property("price_to_earnings_ratio", th.NumberType),
        th.Property("price_to_earnings_growth_ratio", th.NumberType),
        th.Property("forward_price_to_earnings_growth_ratio", th.NumberType),
        th.Property("price_to_book_ratio", th.NumberType),
        th.Property("price_to_sales_ratio", th.NumberType),
        th.Property("price_to_free_cash_flow_ratio", th.NumberType),
        th.Property("price_to_operating_cash_flow_ratio", th.NumberType),
        th.Property("debt_to_assets_ratio", th.NumberType),
        th.Property("debt_to_equity_ratio", th.NumberType),
        th.Property("debt_to_capital_ratio", th.NumberType),
        th.Property("long_term_debt_to_capital_ratio", th.NumberType),
        th.Property("financial_leverage_ratio", th.NumberType),
        th.Property("working_capital_turnover_ratio", th.NumberType),
        th.Property("operating_cash_flow_ratio", th.NumberType),
        th.Property("operating_cash_flow_sales_ratio", th.NumberType),
        th.Property("free_cash_flow_operating_cash_flow_ratio", th.NumberType),
        th.Property("debt_service_coverage_ratio", th.NumberType),
        th.Property("interest_coverage_ratio", th.NumberType),
        th.Property("short_term_operating_cash_flow_coverage_ratio", th.NumberType),
        th.Property("operating_cash_flow_coverage_ratio", th.NumberType),
        th.Property("capital_expenditure_coverage_ratio", th.NumberType),
        th.Property("dividend_paid_and_capex_coverage_ratio", th.NumberType),
        th.Property("dividend_payout_ratio", th.NumberType),
        th.Property("dividend_yield", th.NumberType),
        th.Property("dividend_yield_percentage", th.NumberType),
        th.Property("revenue_per_share", th.NumberType),
        th.Property("net_income_per_share", th.NumberType),
        th.Property("interest_debt_per_share", th.NumberType),
        th.Property("cash_per_share", th.NumberType),
        th.Property("book_value_per_share", th.NumberType),
        th.Property("tangible_book_value_per_share", th.NumberType),
        th.Property("shareholders_equity_per_share", th.NumberType),
        th.Property("operating_cash_flow_per_share", th.NumberType),
        th.Property("capex_per_share", th.NumberType),
        th.Property("free_cash_flow_per_share", th.NumberType),
        th.Property("net_income_per_ebt", th.NumberType),
        th.Property("ebt_per_ebit", th.NumberType),
        th.Property("price_to_fair_value", th.NumberType),
        th.Property("debt_to_market_cap", th.NumberType),
        th.Property("effective_tax_rate", th.NumberType),
        th.Property("enterprise_value_multiple", th.NumberType),
        th.Property("dividend_per_share", th.NumberType),
        th.Property("net_income_perebt", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/ratios"


class KeyMetricsTtmStream(SymbolPartitionStream):
    """Key metrics TTM data for companies."""

    name = "key_metrics_ttm"
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
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

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/key-metrics-ttm"

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


class FinancialRatiosTtmStream(SymbolPartitionStream):
    """Financial ratios TTM data for companies."""

    name = "financial_ratios_ttm"
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
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
        th.Property("working_capital_turnover_ratio_ttm", th.NumberType),
        th.Property("operating_cash_flow_coverage_ratio_ttm", th.NumberType),
        th.Property("payables_turnover_ttm", th.NumberType),
        th.Property("free_cash_flow_operating_cash_flow_ratio_ttm", th.NumberType),
        th.Property("asset_turnover_ttm", th.NumberType),
        th.Property("cash_ratio_ttm", th.NumberType),
        th.Property("current_ratio_ttm", th.NumberType),
        th.Property("short_term_operating_cash_flow_coverage_ratio_ttm", th.NumberType),
        th.Property("enterprise_value_ttm", th.NumberType),
        th.Property("price_to_book_ratio_ttm", th.NumberType),
        th.Property("price_to_sales_ratio_ttm", th.NumberType),
        th.Property("forward_price_to_earnings_growth_ratio_ttm", th.NumberType),
        th.Property("dividend_payout_ratio_ttm", th.NumberType),
        th.Property("tangible_book_value_per_share_ttm", th.NumberType),
        th.Property("interest_debt_per_share_ttm", th.NumberType),
        th.Property("long_term_debt_to_capital_ratio_ttm", th.NumberType),
        th.Property("pretax_profit_marginttm", th.NumberType),
        th.Property("ebitda_marginttm", th.NumberType),
        th.Property("continuous_operations_profit_marginttm", th.NumberType),
        th.Property("price_to_earnings_ratio_ttm", th.NumberType),
        th.Property("dividend_paid_and_capex_coverage_ratio_ttm", th.NumberType),
        th.Property("debt_service_coverage_ratio_ttm", th.NumberType),
        th.Property("capex_per_share_ttm", th.NumberType),
        th.Property("quick_ratio_ttm", th.NumberType),
        th.Property("net_profit_marginttm", th.NumberType),
        th.Property("debt_to_capital_ratio_ttm", th.NumberType),
        th.Property("book_value_per_share_ttm", th.NumberType),
        th.Property("operating_cash_flow_sales_ratio_ttm", th.NumberType),
        th.Property("bottom_line_profit_marginttm", th.NumberType),
        th.Property("gross_profit_marginttm", th.NumberType),
        th.Property("operating_profit_marginttm", th.NumberType),
        th.Property("financial_leverage_ratio_ttm", th.NumberType),
        th.Property("revenue_per_share_ttm", th.NumberType),
        th.Property("price_to_free_cash_flow_ratio_ttm", th.NumberType),
        th.Property("operating_cash_flow_ratio_ttm", th.NumberType),
        th.Property("fixed_asset_turnover_ttm", th.NumberType),
        th.Property("shareholders_equity_per_share_ttm", th.NumberType),
        th.Property("cash_per_share_ttm", th.NumberType),
        th.Property("debt_to_equity_ratio_ttm", th.NumberType),
        th.Property("net_income_perebt_ttm", th.NumberType),
        th.Property("price_to_fair_value_ttm", th.NumberType),
        th.Property("debt_to_market_cap_ttm", th.NumberType),
        th.Property("price_to_earnings_growth_ratio_ttm", th.NumberType),
        th.Property("interest_coverage_ratio_ttm", th.NumberType),
        th.Property("ebt_per_ebitttm", th.NumberType),
        th.Property("free_cash_flow_per_share_ttm", th.NumberType),
        th.Property("effective_tax_ratettm", th.NumberType),
        th.Property("receivables_turnover_ttm", th.NumberType),
        th.Property("inventory_turnover_ttm", th.NumberType),
        th.Property("capital_expenditure_coverage_ratio_ttm", th.NumberType),
        th.Property("solvency_ratio_ttm", th.NumberType),
        th.Property("price_to_operating_cash_flow_ratio_ttm", th.NumberType),
        th.Property("operating_cash_flow_per_share_ttm", th.NumberType),
        th.Property("ebit_marginttm", th.NumberType),
        th.Property("net_income_per_share_ttm", th.NumberType),
        th.Property("dividend_yieldttm", th.NumberType),
        th.Property("dividend_per_share_ttm", th.NumberType),
        th.Property("debt_to_assets_ratio_ttm", th.NumberType),
        th.Property("enterprise_value_multiplettm", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/ratios-ttm"

    def post_process(self, row: dict, context: Context = None) -> dict:
        if "net_income_per_ebtttm" in row:
            row["net_income_per_ebt_ttm"] = row.pop("net_income_per_ebtttm")
        return super().post_process(row, context)


class FinancialScoresStream(SymbolPartitionStream):
    """Financial scores data for companies."""

    name = "financial_scores"
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("altman_z_score", th.NumberType),
        th.Property("piotroski_score", th.NumberType),
        th.Property("working_capital", th.NumberType),
        th.Property("total_assets", th.NumberType),
        th.Property("retained_earnings", th.NumberType),
        th.Property("ebit", th.NumberType),
        th.Property("market_cap", th.NumberType),
        th.Property("total_liabilities", th.NumberType),
        th.Property("revenue", th.NumberType),
        th.Property("reported_currency", th.StringType),
        th.Property("altmanz_score", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/financial-scores"


class OwnerEarningsStream(SymbolPartitionStream):
    """Owner earnings data for companies."""

    name = "owner_earnings"
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("date", th.DateType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("fiscal_year", th.IntegerType),
        th.Property("period", th.StringType),
        th.Property("owners_earnings", th.NumberType),
        th.Property("net_income", th.NumberType),
        th.Property("depreciation_and_amortization", th.NumberType),
        th.Property("capital_expenditure", th.NumberType),
        th.Property("change_in_working_capital", th.NumberType),
        th.Property("average_ppe", th.NumberType),
        th.Property("growth_capex", th.NumberType),
        th.Property("owners_earnings_per_share", th.NumberType),
        th.Property("reported_currency", th.StringType),
        th.Property("maintenance_capex", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/owner-earnings"

    def post_process(self, record: dict, context: Context | None = None) -> dict:
        if "fiscal_year" in record:
            record["fiscal_year"] = int(record["fiscal_year"])
        return super().post_process(record, context)


class EnterpriseValuesStream(StatementStream):
    """Enterprise values data for companies."""

    name = "enterprise_values"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("date", th.DateType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("stock_price", th.NumberType),
        th.Property("number_of_shares", th.NumberType),
        th.Property("market_capitalization", th.NumberType),
        th.Property("minus_cash_and_cash_equivalents", th.NumberType),
        th.Property("add_total_debt", th.NumberType),
        th.Property("enterprise_value", th.NumberType),
        th.Property("period", th.StringType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/enterprise-values"


class IncomeStatementGrowthStream(StatementStream):
    """Income statement growth data for companies."""

    name = "income_statement_growth"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("date", th.DateType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("period", th.StringType),
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
        th.Property("growth_interest_expense", th.NumberType),
        th.Property("growth_depreciation_and_amortization", th.NumberType),
        th.Property("growth_ebit", th.NumberType),
        th.Property("growth_ebitda", th.NumberType),
        th.Property("growth_ebitda_ratio", th.NumberType),
        th.Property("growth_operating_income", th.NumberType),
        th.Property("growth_operating_income_ratio", th.NumberType),
        th.Property("growth_total_other_income_expenses_net", th.NumberType),
        th.Property("growth_income_before_tax", th.NumberType),
        th.Property("growth_income_before_tax_ratio", th.NumberType),
        th.Property("growth_income_tax_expense", th.NumberType),
        th.Property("growth_net_income", th.NumberType),
        th.Property("growth_net_income_ratio", th.NumberType),
        th.Property("growth_eps", th.NumberType),
        th.Property("growth_eps_diluted", th.NumberType),
        th.Property("growth_weighted_average_shs_out", th.NumberType),
        th.Property("growth_weighted_average_shs_out_dil", th.NumberType),
        th.Property("growthebitda", th.NumberType),
        th.Property("growthebit", th.NumberType),
        th.Property("growtheps_diluted", th.NumberType),
        th.Property("growtheps", th.NumberType),
        th.Property("growth_non_operating_income_excluding_interest", th.NumberType),
        th.Property("growth_other_adjustments_to_net_income", th.NumberType),
        th.Property("growth_net_income_deductions", th.NumberType),
        th.Property("growth_interest_income", th.NumberType),
        th.Property("growth_net_interest_income", th.NumberType),
        th.Property("growth_net_income_from_continuing_operations", th.NumberType),
        th.Property("fiscal_year", th.IntegerType),
        th.Property("reported_currency", th.StringType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/income-statement-growth"


class BalanceSheetGrowthStream(StatementStream):
    """Balance sheet growth data for companies."""

    name = "balance_sheet_growth"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("date", th.DateType, required=True),
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

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/balance-sheet-statement-growth"


class CashFlowGrowthStream(StatementStream):
    """Cash flow growth data for companies."""

    name = "cash_flow_growth"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("date", th.DateType, required=True),
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

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/cash-flow-statement-growth"


class FinancialStatementGrowthStream(StatementStream):
    """Financial statement growth data for companies."""

    name = "financial_statement_growth"

    schema = th.PropertiesList(
        th.Property("symbol", th.StringType),
        th.Property("date", th.DateType),
        th.Property("fiscal_year", th.IntegerType),
        th.Property("period", th.StringType),
        th.Property("reported_currency", th.StringType),
        th.Property("revenue_growth", th.NumberType),
        th.Property("gross_profit_growth", th.NumberType),
        th.Property("ebit_growth", th.NumberType),
        th.Property("operating_income_growth", th.NumberType),
        th.Property("net_income_growth", th.NumberType),
        th.Property("eps_growth", th.NumberType),
        th.Property("eps_diluted_growth", th.NumberType),
        th.Property("weighted_average_shares_growth", th.NumberType),
        th.Property("weighted_average_shares_diluted_growth", th.NumberType),
        th.Property("dividends_per_share_growth", th.NumberType),
        th.Property("operating_cash_flow_growth", th.NumberType),
        th.Property("receivables_growth", th.NumberType),
        th.Property("inventory_growth", th.NumberType),
        th.Property("asset_growth", th.NumberType),
        th.Property("book_value_per_share_growth", th.NumberType),
        th.Property("debt_growth", th.NumberType),
        th.Property("rd_expense_growth", th.NumberType),
        th.Property("sga_expenses_growth", th.NumberType),
        th.Property("free_cash_flow_growth", th.NumberType),
        th.Property("ten_y_revenue_growth_per_share", th.NumberType),
        th.Property("five_y_revenue_growth_per_share", th.NumberType),
        th.Property("three_y_revenue_growth_per_share", th.NumberType),
        th.Property("ten_y_operating_cf_growth_per_share", th.NumberType),
        th.Property("five_y_operating_cf_growth_per_share", th.NumberType),
        th.Property("three_y_operating_cf_growth_per_share", th.NumberType),
        th.Property("ten_y_net_income_growth_per_share", th.NumberType),
        th.Property("five_y_net_income_growth_per_share", th.NumberType),
        th.Property("three_y_net_income_growth_per_share", th.NumberType),
        th.Property("ten_y_shareholders_equity_growth_per_share", th.NumberType),
        th.Property("five_y_shareholders_equity_growth_per_share", th.NumberType),
        th.Property("three_y_shareholders_equity_growth_per_share", th.NumberType),
        th.Property("ten_y_dividend_per_share_growth_per_share", th.NumberType),
        th.Property("five_y_dividend_per_share_growth_per_share", th.NumberType),
        th.Property("three_y_dividend_per_share_growth_per_share", th.NumberType),
        th.Property("ebitda_growth", th.NumberType),
        th.Property("growth_capital_expenditure", th.NumberType),
        th.Property("ten_y_bottom_line_net_income_growth_per_share", th.NumberType),
        th.Property("five_y_bottom_line_net_income_growth_per_share", th.NumberType),
        th.Property("three_y_bottom_line_net_income_growth_per_share", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/financial-growth"


class FinancialStatementReportDatesStream(SymbolPartitionStream):
    """Financial statements report dates."""

    name = "financial_report_dates"
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("date", th.DateType),
        th.Property("period", th.StringType),
        th.Property("fiscal_year", th.IntegerType),
        th.Property("link_xlsx", th.StringType),
        th.Property("link_json", th.StringType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/financial-reports-dates"

    def post_process(self, row: dict, context: Context | None = None) -> dict:
        if "fiscal_year" in row.keys():
            row["fiscal_year"] = int(row["fiscal_year"])
        return super().post_process(row, context)


class FinancialReportsForm10kJsonStream(StatementStream):
    """Financial reports Form 10-K JSON."""

    name = "financial_reports_form_10k_json"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("period", th.StringType, required=True),
        th.Property("year", th.StringType, required=True),
        th.Property("cover_page", th.ArrayType(th.AnyType())),
        th.Property("auditor_information", th.ArrayType(th.AnyType())),
        th.Property("consolidated_statements_of_oper", th.ArrayType(th.AnyType())),
        th.Property("consolidated_statements_of_comp", th.ArrayType(th.AnyType())),
        th.Property("consolidated_balance_sheets", th.ArrayType(th.AnyType())),
        th.Property(
            "consolidated_balance_sheets_parenthetical", th.ArrayType(th.AnyType())
        ),
        th.Property("consolidated_statements_of_shar", th.ArrayType(th.AnyType())),
        th.Property("consolidated_statements_of_cash", th.ArrayType(th.AnyType())),
        th.Property(
            "summary_of_significant_accounting_policies", th.ArrayType(th.AnyType())
        ),
        th.Property("revenue", th.ArrayType(th.AnyType())),
        th.Property("financial_instruments", th.ArrayType(th.AnyType())),
        th.Property("consolidated_financial_statemen", th.ArrayType(th.AnyType())),
        th.Property("income_taxes", th.ArrayType(th.AnyType())),
        th.Property("leases", th.ArrayType(th.AnyType())),
        th.Property("debt", th.ArrayType(th.AnyType())),
        th.Property("shareholders_equity", th.ArrayType(th.AnyType())),
        th.Property("benefit_plans", th.ArrayType(th.AnyType())),
        th.Property("commitments_and_contingencies", th.ArrayType(th.AnyType())),
        th.Property("segment_information_and_geograp", th.ArrayType(th.AnyType())),
        th.Property("summary_of_significant_accoun_2", th.ArrayType(th.AnyType())),
        th.Property("summary_of_significant_accoun_3", th.ArrayType(th.AnyType())),
        th.Property("revenue_tables", th.ArrayType(th.AnyType())),
        th.Property("financial_instruments_tables", th.ArrayType(th.AnyType())),
        th.Property("consolidated_financial_statem_2", th.ArrayType(th.AnyType())),
        th.Property("income_taxes_tables", th.ArrayType(th.AnyType())),
        th.Property("leases_tables", th.ArrayType(th.AnyType())),
        th.Property("debt_tables", th.ArrayType(th.AnyType())),
        th.Property("shareholders_equity_tables", th.ArrayType(th.AnyType())),
        th.Property("benefit_plans_tables", th.ArrayType(th.AnyType())),
        th.Property("commitments_and_contingencies_tables", th.ArrayType(th.AnyType())),
        th.Property("segment_information_and_geogr_2", th.ArrayType(th.AnyType())),
        th.Property("summary_of_significant_accoun_4", th.ArrayType(th.AnyType())),
        th.Property("summary_of_significant_accoun_5", th.ArrayType(th.AnyType())),
        th.Property("revenue_net_sales_disaggregat", th.ArrayType(th.AnyType())),
        th.Property("revenue_additional_informatio", th.ArrayType(th.AnyType())),
        th.Property("revenue_deferred_revenue_exp", th.ArrayType(th.AnyType())),
        th.Property("financial_instruments_cash_c", th.ArrayType(th.AnyType())),
        th.Property("financial_instruments_non_cur", th.ArrayType(th.AnyType())),
        th.Property("financial_instruments_additio", th.ArrayType(th.AnyType())),
        th.Property("financial_instruments_notiona", th.ArrayType(th.AnyType())),
        th.Property("financial_instruments_gross_f", th.ArrayType(th.AnyType())),
        th.Property("financial_instruments_derivat", th.ArrayType(th.AnyType())),
        th.Property("consolidated_financial_statem_3", th.ArrayType(th.AnyType())),
        th.Property("consolidated_financial_statem_4", th.ArrayType(th.AnyType())),
        th.Property("consolidated_financial_statem_5", th.ArrayType(th.AnyType())),
        th.Property("income_taxes_provision_for_in", th.ArrayType(th.AnyType())),
        th.Property("income_taxes_additional_infor", th.ArrayType(th.AnyType())),
        th.Property("income_taxes_reconciliation_o", th.ArrayType(th.AnyType())),
        th.Property("income_taxes_significant_comp", th.ArrayType(th.AnyType())),
        th.Property("income_taxes_aggregate_change", th.ArrayType(th.AnyType())),
        th.Property("leases_additional_information", th.ArrayType(th.AnyType())),
        th.Property("leases_rou_assets_and_lease_l", th.ArrayType(th.AnyType())),
        th.Property("leases_lease_liability_maturi", th.ArrayType(th.AnyType())),
        th.Property("debt_additional_information", th.ArrayType(th.AnyType())),
        th.Property("debt_summary_of_cash_flows_as", th.ArrayType(th.AnyType())),
        th.Property("debt_summary_of_term_debt_de", th.ArrayType(th.AnyType())),
        th.Property("debt_future_principal_payment", th.ArrayType(th.AnyType())),
        th.Property("shareholders_equity_addition", th.ArrayType(th.AnyType())),
        th.Property("shareholders_equity_shares_o", th.ArrayType(th.AnyType())),
        th.Property("benefit_plans_additional_info", th.ArrayType(th.AnyType())),
        th.Property("benefit_plans_restricted_stoc", th.ArrayType(th.AnyType())),
        th.Property("benefit_plans_summary_of_shar", th.ArrayType(th.AnyType())),
        th.Property("commitments_and_contingencies_", th.ArrayType(th.AnyType())),
        th.Property("segment_information_and_geogr_3", th.ArrayType(th.AnyType())),
        th.Property("segment_information_and_geogr_4", th.ArrayType(th.AnyType())),
        th.Property("segment_information_and_geogr_5", th.ArrayType(th.AnyType())),
        th.Property("segment_information_and_geogr_6", th.ArrayType(th.AnyType())),
        th.Property("condensed_consolidated_statemen", th.ArrayType(th.AnyType())),
        th.Property("condensed_consolidated_statem_2", th.ArrayType(th.AnyType())),
        th.Property("condensed_consolidated_balance", th.ArrayType(th.AnyType())),
        th.Property("condensed_consolidated_balanc_2", th.ArrayType(th.AnyType())),
        th.Property("condensed_consolidated_statem_3", th.ArrayType(th.AnyType())),
        th.Property("condensed_consolidated_statem_4", th.ArrayType(th.AnyType())),
        th.Property("summary_of_significant_accounti", th.ArrayType(th.AnyType())),
        th.Property("earnings_per_share", th.ArrayType(th.AnyType())),
        th.Property("condensed_consolidated_financia", th.ArrayType(th.AnyType())),
        th.Property("share_based_compensation", th.ArrayType(th.AnyType())),
        th.Property("contingencies", th.ArrayType(th.AnyType())),
        th.Property("pay_vs_performance_disclosure", th.ArrayType(th.AnyType())),
        th.Property("insider_trading_arrangements", th.ArrayType(th.AnyType())),
        th.Property("earnings_per_share_tables", th.ArrayType(th.AnyType())),
        th.Property("condensed_consolidated_financ_2", th.ArrayType(th.AnyType())),
        th.Property("share_based_compensation_table", th.ArrayType(th.AnyType())),
        th.Property("earnings_per_share_computatio", th.ArrayType(th.AnyType())),
        th.Property("condensed_consolidated_financ_3", th.ArrayType(th.AnyType())),
        th.Property("condensed_consolidated_financ_4", th.ArrayType(th.AnyType())),
        th.Property("share_based_compensation_rest", th.ArrayType(th.AnyType())),
        th.Property("share_based_compensation_addi", th.ArrayType(th.AnyType())),
        th.Property("share_based_compensation_summ", th.ArrayType(th.AnyType())),
        th.Property("condensed_consolidated_financ_5", th.ArrayType(th.AnyType())),
        th.Property("earnings_per_share_additional", th.ArrayType(th.AnyType())),
        th.Property("revenue_disaggregated_net_sal", th.ArrayType(th.AnyType())),
        th.Property("cash_equivalents_and_marketab_2", th.ArrayType(th.AnyType())),
        th.Property("balance_sheet_components_othe", th.ArrayType(th.AnyType())),
        th.Property("cash_equivalents_and_marketable", th.ArrayType(th.AnyType())),
        th.Property("commitments_and_contingencies_3", th.ArrayType(th.AnyType())),
        th.Property("leases_narrative_details", th.ArrayType(th.AnyType())),
        th.Property("organization_and_summary_of_sig", th.ArrayType(th.AnyType())),
        th.Property("consolidated_statements_of_sh_2", th.ArrayType(th.AnyType())),
        th.Property("balance_sheet_components_reve", th.ArrayType(th.AnyType())),
        th.Property("fair_value_of_financial_asset_2", th.ArrayType(th.AnyType())),
        th.Property("derivative_financial_instrume_3", th.ArrayType(th.AnyType())),
        th.Property("segment_information_reconcili", th.ArrayType(th.AnyType())),
        th.Property("audit_information", th.ArrayType(th.AnyType())),
        th.Property("business_combination_details", th.ArrayType(th.AnyType())),
        th.Property("amortizable_intangible_assets_2", th.ArrayType(th.AnyType())),
        th.Property("segment_information_schedule", th.ArrayType(th.AnyType())),
        th.Property("balance_sheet_components_narr", th.ArrayType(th.AnyType())),
        th.Property("balance_sheet_components_table", th.ArrayType(th.AnyType())),
        th.Property("shareholders_equity_details", th.ArrayType(th.AnyType())),
        th.Property("cash_equivalents_and_marketab_4", th.ArrayType(th.AnyType())),
        th.Property("derivative_financial_instrume_2", th.ArrayType(th.AnyType())),
        th.Property("segment_information", th.ArrayType(th.AnyType())),
        th.Property("income_taxes_components_of_in", th.ArrayType(th.AnyType())),
        th.Property("stock_based_compensation", th.ArrayType(th.AnyType())),
        th.Property("net_income_per_share", th.ArrayType(th.AnyType())),
        th.Property("stock_based_compensation_equi", th.ArrayType(th.AnyType())),
        th.Property("stock_based_compensation_narr", th.ArrayType(th.AnyType())),
        th.Property("segment_information_reportabl", th.ArrayType(th.AnyType())),
        th.Property("balance_sheet_components_defe", th.ArrayType(th.AnyType())),
        th.Property("schedule_i_i_valuation_and_qua", th.ArrayType(th.AnyType())),
        th.Property("fair_value_of_financial_asset_4", th.ArrayType(th.AnyType())),
        th.Property("fair_value_of_financial_assets", th.ArrayType(th.AnyType())),
        th.Property("income_taxes_income_tax_recon", th.ArrayType(th.AnyType())),
        th.Property("balance_sheet_components_accr", th.ArrayType(th.AnyType())),
        th.Property("segment_information_tables", th.ArrayType(th.AnyType())),
        th.Property("income_taxes_unrecognized_tax", th.ArrayType(th.AnyType())),
        th.Property("leases_schedule_of_future_min", th.ArrayType(th.AnyType())),
        th.Property("goodwill_details", th.ArrayType(th.AnyType())),
        th.Property("balance_sheet_components_inve", th.ArrayType(th.AnyType())),
        th.Property("balance_sheet_components_prop", th.ArrayType(th.AnyType())),
        th.Property("cash_equivalents_and_marketab_5", th.ArrayType(th.AnyType())),
        th.Property("amortizable_intangible_assets", th.ArrayType(th.AnyType())),
        th.Property("balance_sheet_components_ot_2", th.ArrayType(th.AnyType())),
        th.Property("fair_value_of_financial_asset_3", th.ArrayType(th.AnyType())),
        th.Property("fair_value_of_financial_asset_6", th.ArrayType(th.AnyType())),
        th.Property("debt_narrative_details", th.ArrayType(th.AnyType())),
        th.Property("organization_and_summary_of_s_3", th.ArrayType(th.AnyType())),
        th.Property("income_taxes_narrative_detai", th.ArrayType(th.AnyType())),
        th.Property("income_taxes_deferred_taxes", th.ArrayType(th.AnyType())),
        th.Property("net_income_per_share_details", th.ArrayType(th.AnyType())),
        th.Property("consolidated_balance_sheets_pa", th.ArrayType(th.AnyType())),
        th.Property("goodwill", th.ArrayType(th.AnyType())),
        th.Property("business_combination", th.ArrayType(th.AnyType())),
        th.Property("segment_information_narrative", th.ArrayType(th.AnyType())),
        th.Property("commitments_and_contingencies_2", th.ArrayType(th.AnyType())),
        th.Property("derivative_financial_instrume_4", th.ArrayType(th.AnyType())),
        th.Property("stock_based_compensation_sche", th.ArrayType(th.AnyType())),
        th.Property("balance_sheet_components_sc_5", th.ArrayType(th.AnyType())),
        th.Property("segment_information_revenue_2", th.ArrayType(th.AnyType())),
        th.Property("debt_schedule_of_instruments", th.ArrayType(th.AnyType())),
        th.Property("segment_information_schedul_2", th.ArrayType(th.AnyType())),
        th.Property("segment_information_schedul_4", th.ArrayType(th.AnyType())),
        th.Property("cover", th.ArrayType(th.AnyType())),
        th.Property("amortizable_intangible_assets_a", th.ArrayType(th.AnyType())),
        th.Property("commitment_and_contingencies", th.ArrayType(th.AnyType())),
        th.Property("commitment_and_contingencies_t", th.ArrayType(th.AnyType())),
        th.Property("stock_based_compensation_allo", th.ArrayType(th.AnyType())),
        th.Property("subsequent_events", th.ArrayType(th.AnyType())),
        th.Property("balance_sheet_components_sc_4", th.ArrayType(th.AnyType())),
        th.Property("balance_sheet_components_sc_2", th.ArrayType(th.AnyType())),
        th.Property("segment_information_schedul_3", th.ArrayType(th.AnyType())),
        th.Property("debt_schedule_of_long_term_de", th.ArrayType(th.AnyType())),
        th.Property("subsequent_events_details", th.ArrayType(th.AnyType())),
        th.Property("subsequent_events_tables", th.ArrayType(th.AnyType())),
        th.Property("balance_sheet_components_sche", th.ArrayType(th.AnyType())),
        th.Property("stock_based_compensation_table", th.ArrayType(th.AnyType())),
        th.Property("leases_schedule_of_other_leas", th.ArrayType(th.AnyType())),
        th.Property("stock_based_compensation_summ", th.ArrayType(th.AnyType())),
        th.Property("error_message", th.AnyType()),
        th.Property("income_taxes_details", th.ArrayType(th.AnyType())),
        th.Property("property_plant_and_equipment", th.ArrayType(th.AnyType())),
        th.Property("commitments_contingencies_and", th.ArrayType(th.AnyType())),
        th.Property("commitments_contingencies_an_2", th.ArrayType(th.AnyType())),
        th.Property("commitments_contingencies_an_3", th.ArrayType(th.AnyType())),
        th.Property("insider_trading_policies_and_pr", th.ArrayType(th.AnyType())),
        th.Property("consolidated_financial_statem_6", th.ArrayType(th.AnyType())),
        th.Property("cash_equivalents_and_marketab_3", th.ArrayType(th.AnyType())),
        th.Property("cash_equivalents_and_marketab_6", th.ArrayType(th.AnyType())),
        th.Property("segment_information_revenue", th.ArrayType(th.AnyType())),
        th.Property("segment_information_revenue_a", th.ArrayType(th.AnyType())),
        th.Property("segment_information_summary_o", th.ArrayType(th.AnyType())),
        th.Property("segment_information_concentra", th.ArrayType(th.AnyType())),
        th.Property("condensed_consolidated_statem_5", th.ArrayType(th.AnyType())),
        th.Property("consolidated_statements_of_inco", th.ArrayType(th.AnyType())),
        th.Property("balance_sheet_components", th.ArrayType(th.AnyType())),
        th.Property("balance_sheet_components_chan", th.ArrayType(th.AnyType())),
        th.Property("balance_sheet_components_sc_3", th.ArrayType(th.AnyType())),
        th.Property("net_income_per_share_tables", th.ArrayType(th.AnyType())),
        th.Property("stock_based_compensation_sc_2", th.ArrayType(th.AnyType())),
        th.Property("fair_value_of_financial_asset_5", th.ArrayType(th.AnyType())),
        th.Property("amortizable_intangible_assets_3", th.ArrayType(th.AnyType())),
        th.Property("derivative_financial_instrument", th.ArrayType(th.AnyType())),
        th.Property("document_and_entity_information", th.ArrayType(th.AnyType())),
        th.Property("organization_and_summary_of_s_2", th.ArrayType(th.AnyType())),
        th.Property("employee_retirement_plans", th.ArrayType(th.AnyType())),
        th.Property("employee_retirement_plans_deta", th.ArrayType(th.AnyType())),
        th.Property("business_combination_assets_a", th.ArrayType(th.AnyType())),
        th.Property("business_combination_intangib", th.ArrayType(th.AnyType())),
        th.Property("business_combination_terminat", th.ArrayType(th.AnyType())),
        th.Property("business_combination_pro_form", th.ArrayType(th.AnyType())),
        th.Property("business_combination_tables", th.ArrayType(th.AnyType())),
        th.Property("business_combination_acquisit", th.ArrayType(th.AnyType())),
        th.Property("debt_schedule_of_debt_detail", th.ArrayType(th.AnyType())),
        th.Property("schedule_ii_valuation_and_qua", th.ArrayType(th.AnyType())),
        th.Property("schedule_ii_valuation_and_q_2", th.ArrayType(th.AnyType())),
        th.Property("cybersecurity_risk_management_a", th.ArrayType(th.AnyType())),
        th.Property("unaudited_condensed_consolidate", th.ArrayType(th.AnyType())),
        th.Property("allowance_for_credit_losses_tr", th.ArrayType(th.AnyType())),
        th.Property("inventories", th.ArrayType(th.AnyType())),
        th.Property("intangible_assets", th.ArrayType(th.AnyType())),
        th.Property("other_current_liabilities", th.ArrayType(th.AnyType())),
        th.Property("pension_and_other_postretiremen", th.ArrayType(th.AnyType())),
        th.Property("commitments_and_contingent_liab", th.ArrayType(th.AnyType())),
        th.Property("equity_rights_offering", th.ArrayType(th.AnyType())),
        th.Property("accumulated_other_comprehensive", th.ArrayType(th.AnyType())),
        th.Property("derivative_instruments", th.ArrayType(th.AnyType())),
        th.Property("fair_value", th.ArrayType(th.AnyType())),
        th.Property("net_sales_and_income_loss_bef", th.ArrayType(th.AnyType())),
        th.Property("net_sales_and_loss_income_bef", th.ArrayType(th.AnyType())),
        th.Property("litigation", th.ArrayType(th.AnyType())),
        th.Property("environmental_matters", th.ArrayType(th.AnyType())),
        th.Property("related_parties", th.ArrayType(th.AnyType())),
        th.Property("business_segments", th.ArrayType(th.AnyType())),
        th.Property("unaudited_condensed_consolida_2", th.ArrayType(th.AnyType())),
        th.Property("inventories_tables", th.ArrayType(th.AnyType())),
        th.Property("intangible_assets_tables", th.ArrayType(th.AnyType())),
        th.Property("other_current_liabilities_tabl", th.ArrayType(th.AnyType())),
        th.Property("pension_and_other_postretirem_2", th.ArrayType(th.AnyType())),
        th.Property("accumulated_other_comprehensi_2", th.ArrayType(th.AnyType())),
        th.Property("derivative_instruments_tables", th.ArrayType(th.AnyType())),
        th.Property("fair_value_tables", th.ArrayType(th.AnyType())),
        th.Property("net_sales_and_income_loss_b_2", th.ArrayType(th.AnyType())),
        th.Property("net_sales_and_loss_income_b_2", th.ArrayType(th.AnyType())),
        th.Property("litigation_tables", th.ArrayType(th.AnyType())),
        th.Property("related_parties_tables", th.ArrayType(th.AnyType())),
        th.Property("business_segments_tables", th.ArrayType(th.AnyType())),
        th.Property("unaudited_condensed_consolida_3", th.ArrayType(th.AnyType())),
        th.Property("allowance_for_credit_losses_2", th.ArrayType(th.AnyType())),
        th.Property("inventories_schedule_of_inven", th.ArrayType(th.AnyType())),
        th.Property("property_plant_and_equipment_2", th.ArrayType(th.AnyType())),
        th.Property("intangible_assets_schedule_of", th.ArrayType(th.AnyType())),
        th.Property("intangible_assets_summary_of", th.ArrayType(th.AnyType())),
        th.Property("other_current_liabilities_sch", th.ArrayType(th.AnyType())),
        th.Property("other_current_liabilities_s_2", th.ArrayType(th.AnyType())),
        th.Property("other_current_liabilities_add", th.ArrayType(th.AnyType())),
        th.Property("other_current_liabilities_s_3", th.ArrayType(th.AnyType())),
        th.Property("debt_schedule_of_outstanding", th.ArrayType(th.AnyType())),
        th.Property("pension_and_other_postretirem_3", th.ArrayType(th.AnyType())),
        th.Property("pension_and_other_postretirem_4", th.ArrayType(th.AnyType())),
        th.Property("commitments_and_contingent_li_2", th.ArrayType(th.AnyType())),
        th.Property("equity_rights_offering_additi", th.ArrayType(th.AnyType())),
        th.Property("accumulated_other_comprehensi_3", th.ArrayType(th.AnyType())),
        th.Property("accumulated_other_comprehensi_4", th.ArrayType(th.AnyType())),
        th.Property("accumulated_other_comprehensi_5", th.ArrayType(th.AnyType())),
        th.Property("accumulated_other_comprehensi_6", th.ArrayType(th.AnyType())),
        th.Property("derivative_instruments_additi", th.ArrayType(th.AnyType())),
        th.Property("derivative_instruments_summar", th.ArrayType(th.AnyType())),
        th.Property("derivative_instruments_summ_2", th.ArrayType(th.AnyType())),
        th.Property("fair_value_fair_value_of_fina", th.ArrayType(th.AnyType())),
        th.Property("net_sales_and_income_loss_b_3", th.ArrayType(th.AnyType())),
        th.Property("net_sales_and_income_loss_b_4", th.ArrayType(th.AnyType())),
        th.Property("net_sales_and_income_loss_b_5", th.ArrayType(th.AnyType())),
        th.Property("net_sales_and_loss_income_b_3", th.ArrayType(th.AnyType())),
        th.Property("net_sales_and_loss_income_b_4", th.ArrayType(th.AnyType())),
        th.Property("net_sales_and_loss_income_b_5", th.ArrayType(th.AnyType())),
        th.Property("stock_based_compensation_addi", th.ArrayType(th.AnyType())),
        th.Property("litigation_schedule_of_loss_c", th.ArrayType(th.AnyType())),
        th.Property("litigation_schedule_of_loss_2", th.ArrayType(th.AnyType())),
        th.Property("litigation_summary_of_activit", th.ArrayType(th.AnyType())),
        th.Property("litigation_summary_of_activ_2", th.ArrayType(th.AnyType())),
        th.Property("litigation_additional_informa", th.ArrayType(th.AnyType())),
        th.Property("environmental_matters_additio", th.ArrayType(th.AnyType())),
        th.Property("related_parties_additional_in", th.ArrayType(th.AnyType())),
        th.Property("related_parties_summary_of_lo", th.ArrayType(th.AnyType())),
        th.Property("related_parties_summary_of_sa", th.ArrayType(th.AnyType())),
        th.Property("related_parties_summary_of_ba", th.ArrayType(th.AnyType())),
        th.Property("business_segments_additional", th.ArrayType(th.AnyType())),
        th.Property("business_segments_business_se", th.ArrayType(th.AnyType())),
        th.Property("business_segments_business_2", th.ArrayType(th.AnyType())),
        th.Property("business_segments_summary_of", th.ArrayType(th.AnyType())),
        th.Property("business_segments_schedule_of", th.ArrayType(th.AnyType())),
        th.Property("business_segments_schedule_2", th.ArrayType(th.AnyType())),
        th.Property("net_sales_and_income_before_inc", th.ArrayType(th.AnyType())),
        # Additional fields for contract assets and customer related liabilities
        th.Property("contract_assets", th.ArrayType(th.AnyType())),
        th.Property("contract_assets_tables", th.ArrayType(th.AnyType())),
        th.Property("contract_assets_summary_of_ch", th.ArrayType(th.AnyType())),
        th.Property("customer_related_liabilities", th.ArrayType(th.AnyType())),
        th.Property("customer_related_liabilities_t", th.ArrayType(th.AnyType())),
        th.Property("customer_related_liabilities_2", th.ArrayType(th.AnyType())),
        th.Property("customer_related_liabilities_3", th.ArrayType(th.AnyType())),
        # Additional overview field
        th.Property("overview_of_the_business", th.ArrayType(th.AnyType())),
        th.Property("overview_of_the_business_addi", th.ArrayType(th.AnyType())),
        # Missing fields identified from schema validation warnings
        th.Property("operating_lease_right_of_use_2", th.ArrayType(th.AnyType())),
        th.Property("operating_lease_liabilities_2", th.ArrayType(th.AnyType())),
        th.Property("investments_in_joint_ventures", th.ArrayType(th.AnyType())),
        th.Property("pension_and_other_postretirem_6", th.ArrayType(th.AnyType())),
        th.Property("pension_and_other_postretire_16", th.ArrayType(th.AnyType())),
        th.Property("operating_lease_liabilities_s", th.ArrayType(th.AnyType())),
        th.Property("operating_lease_right_of_use_as", th.ArrayType(th.AnyType())),
        th.Property("pension_and_other_postretire_12", th.ArrayType(th.AnyType())),
        th.Property("stock_based_compensation_su_2", th.ArrayType(th.AnyType())),
        th.Property("income_taxes_difference_betwe", th.ArrayType(th.AnyType())),
        th.Property("pension_and_other_postretirem_8", th.ArrayType(th.AnyType())),
        th.Property("description_of_business_addit", th.ArrayType(th.AnyType())),
        th.Property("pension_and_other_postretire_14", th.ArrayType(th.AnyType())),
        th.Property("pension_and_other_postretirem_7", th.ArrayType(th.AnyType())),
        th.Property("description_of_business", th.ArrayType(th.AnyType())),
        th.Property("stock_based_compensation_su_3", th.ArrayType(th.AnyType())),
        th.Property("pension_and_other_postretire_10", th.ArrayType(th.AnyType())),
        th.Property("pension_and_other_postretire_11", th.ArrayType(th.AnyType())),
        th.Property("equity_rights_offering_summar", th.ArrayType(th.AnyType())),
        th.Property("intangible_assets_additional", th.ArrayType(th.AnyType())),
        th.Property("pension_and_other_postretire_15", th.ArrayType(th.AnyType())),
        th.Property("litigation_summary_of_asbesto", th.ArrayType(th.AnyType())),
        th.Property("operating_lease_liabilities_a", th.ArrayType(th.AnyType())),
        th.Property("pension_and_other_postretirem_5", th.ArrayType(th.AnyType())),
        th.Property("operating_lease_liabilities", th.ArrayType(th.AnyType())),
        th.Property("operating_lease_liabilities_ta", th.ArrayType(th.AnyType())),
        th.Property("income_taxes_income_loss_fr", th.ArrayType(th.AnyType())),
        th.Property("research_and_development_costs", th.ArrayType(th.AnyType())),
        th.Property("equity_rights_offering_tables", th.ArrayType(th.AnyType())),
        th.Property("pension_and_other_postretire_13", th.ArrayType(th.AnyType())),
        th.Property("income_taxes_summary_of_incom", th.ArrayType(th.AnyType())),
        th.Property("income_taxes_deferred_income", th.ArrayType(th.AnyType())),
        th.Property("pension_and_other_postretirem_9", th.ArrayType(th.AnyType())),
        th.Property("cybersecurity_risk_management", th.ArrayType(th.AnyType())),
        th.Property("net_sales_and_income_before_i_2", th.ArrayType(th.AnyType())),
        th.Property("net_sales_and_income_before_i_3", th.ArrayType(th.AnyType())),
        th.Property("net_sales_and_income_before_i_4", th.ArrayType(th.AnyType())),
        th.Property("net_sales_and_income_before_i_5", th.ArrayType(th.AnyType())),
        th.Property("income_taxes_loss_income_fr", th.ArrayType(th.AnyType())),
        th.Property("revenue_net_sales_detail", th.ArrayType(th.AnyType())),
    ).to_dict()

    @property
    def partitions(self):
        query_params = self.config.get(self.name, {}).get("query_params", {})
        periods = query_params.get("period")
        years = query_params.get("years")
        if periods is None or periods == "*":
            periods = ["Q1", "Q2", "Q3", "Q4", "FY", "annual", "quarter"]
        if years is None or years == "*":
            years = [y for y in range(2023, datetime.today().date().year + 1)]
            years = [str(y) for y in years]
        return [
            {"symbol": s["symbol"], "period": p, "year": y}
            for s in self._tap.get_cached_symbols()
            for p in periods
            for y in years
        ]

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/financial-reports-json"


class RevenueProductSegmentationStream(StatementStream):
    """Revenue product segmentation data."""

    name = "revenue_product_segmentation"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("date", th.DateType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("fiscal_year", th.IntegerType),
        th.Property("period", th.StringType),
        th.Property("reported_currency", th.StringType),
        th.Property("data", th.StringType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/revenue-product-segmentation"

    def post_process(self, row: dict, context: Context | None = None) -> dict:
        if "data" in row:
            row["data"] = str(row["data"])
        return super().post_process(row, context)


class RevenueGeographicSegmentationStream(RevenueProductSegmentationStream):
    """Revenue geographic segmentation data."""

    name = "revenue_geographic_segmentation"

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/revenue-geographic-segmentation"


class AsReportedIncomeStatementsStream(StatementStream):
    """As reported income statements."""

    name = "as_reported_income_statement"
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("fiscal_year", th.IntegerType, required=True),
        th.Property("period", th.StringType, required=True),
        th.Property("reported_currency", th.StringType),
        th.Property("date", th.DateType, required=True),
        th.Property("data", th.StringType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/income-statement-as-reported"

    def post_process(self, row: dict, context: Context | None = None) -> dict:
        if "data" in row:
            row["data"] = str(row["data"])
        return super().post_process(row, context)


class AsReportedBalanceStatementsStream(AsReportedIncomeStatementsStream):
    """As reported balance statements."""

    name = "as_reported_balance_statement"

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/balance-sheet-statement-as-reported"


class AsReportedCashflowStatementsStream(AsReportedIncomeStatementsStream):
    """As reported cash flow statements."""

    name = "as_reported_cashflow_statement"

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/cash-flow-statement-as-reported"


class AsReportedFinancialStatementsStream(AsReportedIncomeStatementsStream):
    """As reported financial statements (all combined)."""

    name = "as_reported_financial_statement"

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/financial-statement-full-as-reported"
