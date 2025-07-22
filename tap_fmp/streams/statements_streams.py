"""Stream type classes for tap-fmp."""

from __future__ import annotations

import typing as t

from singer_sdk.helpers.types import Context
from singer_sdk import typing as th

from tap_fmp.client import FmpRestStream, SymbolPartitionStream

from tap_fmp.helpers import generate_surrogate_key


class StatementStream(FmpRestStream):
    primary_keys = ["surrogate_key"]

    @property
    def partitions(self):
        periods = ["Q1", "Q2", "Q3", "Q4", "FY", "annual", "quarter"]
        return [
            {"symbol": s["symbol"], "period": p}
            for s in self._tap.get_cached_symbols()
            for p in periods
        ]

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        self.query_params.update(context)
        yield from super().get_records(context)

    def post_process(self, row: dict, context: Context | None = None) -> dict:
        row["surrogate_key"] = generate_surrogate_key(row)
        return row


class IncomeStatementStream(StatementStream):
    name = "income_statement"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.DateType, required=True),
        th.Property("date", th.DateType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("reported_currency", th.StringType),
        th.Property("cik", th.StringType),
        th.Property("filing_date", th.DateType),
        th.Property("accepted_date", th.DateType),
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

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/income-statement"

class BalanceSheetStream(StatementStream):
    name = "balance_sheet"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.DateType, required=True),
        th.Property("date", th.StringType, required=True),
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
    )

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/cash-flow-statement"


class LatestFinancialStatementsStream(FmpRestStream):
    name = "latest_financial_statements"
    _paginate = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.DateType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("date", th.DateType),
        th.Property("calendar_year", th.IntegerType),
        th.Property("period", th.StringType),
        th.Property("date_added", th.DateTimeType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/latest-financial-statements"

    def post_process(self, row: dict, context: Context | None = None) -> dict:
        row["surrogate_key"] = generate_surrogate_key(row)
        return row

class IncomeStatementTtmStream(SymbolPartitionStream):
    name = "income_statement_ttm"
    primary_keys = ["surrogate_key"]

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.DateType, required=True),
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
    )

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/income-statement-ttm"

class BalanceSheetTtmStream(SymbolPartitionStream):
    name = "balance_sheet_ttm"
    primary_keys = ["surrogate_key"]

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.DateType, required=True),
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
    )

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/income-statement-ttm"

    def post_process(self, row: dict, context: Context | None = None) -> dict:
        row["surrogate_key"] = generate_surrogate_key(row)
        return row

