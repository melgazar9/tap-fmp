from tap_fmp.client import SymbolPartitionStream, TimeSliceStream
from singer_sdk.helpers.types import Context
from singer_sdk import typing as th

from typing import Generator

class DcfValuationStream(SymbolPartitionStream):
    name = "dcf_valuation"
    primary_keys = ["symbol", "date"]

    schema = th.PropertiesList(
        th.Property("symbol", th.StringType, required=True),
        th.Property("date", th.DateType),
        th.Property("dcf", th.NumberType),
        th.Property("stock_price", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/discounted-cash-flow"

class LeveredDcfStream(DcfValuationStream):
    name = "levered_dcf"

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/levered-discounted-cash-flow"

class CustomDcfStream(SymbolPartitionStream):
    """ There are many query parameters for this endpoint. For simplicity, we'll use the defaults. """
    name = "custom_dcf"
    primary_keys = ["surrogate_key"]

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("year", th.StringType),
        th.Property("revenue", th.NumberType),
        th.Property("revenue_percentage", th.NumberType),
        th.Property("ebitda", th.NumberType),
        th.Property("ebitda_percentage", th.NumberType),
        th.Property("ebit", th.NumberType),
        th.Property("ebit_percentage", th.NumberType),
        th.Property("depreciation", th.NumberType),
        th.Property("depreciation_percentage", th.NumberType),
        th.Property("total_cash", th.NumberType),
        th.Property("total_cash_percentage", th.NumberType),
        th.Property("receivables", th.NumberType),
        th.Property("receivables_percentage", th.NumberType),
        th.Property("inventories", th.NumberType),
        th.Property("inventories_percentage", th.NumberType),
        th.Property("payable", th.NumberType),
        th.Property("payable_percentage", th.NumberType),
        th.Property("capital_expenditure", th.NumberType),
        th.Property("capital_expenditure_percentage", th.NumberType),
        th.Property("price", th.NumberType),
        th.Property("beta", th.NumberType),
        th.Property("diluted_shares_outstanding", th.NumberType),
        th.Property("cost_of_debt", th.NumberType),
        th.Property("tax_rate", th.NumberType),
        th.Property("after_tax_cost_of_debt", th.NumberType),
        th.Property("risk_free_rate", th.NumberType),
        th.Property("market_risk_premium", th.NumberType),
        th.Property("cost_of_equity", th.NumberType),
        th.Property("total_debt", th.NumberType),
        th.Property("total_equity", th.NumberType),
        th.Property("total_capital", th.NumberType),
        th.Property("debt_weighting", th.NumberType),
        th.Property("equity_weighting", th.NumberType),
        th.Property("wacc", th.NumberType),
        th.Property("tax_rate_cash", th.NumberType),
        th.Property("ebiat", th.NumberType),
        th.Property("ufcf", th.NumberType),
        th.Property("sum_pv_ufcf", th.NumberType),
        th.Property("long_term_growth_rate", th.NumberType),
        th.Property("terminal_value", th.NumberType),
        th.Property("present_terminal_value", th.NumberType),
        th.Property("enterprise_value", th.NumberType),
        th.Property("net_debt", th.NumberType),
        th.Property("equity_value", th.NumberType),
        th.Property("equity_value_per_share", th.NumberType),
        th.Property("free_cash_flow_t1", th.NumberType),
    ).to_dict()

    _add_surrogate_key = True

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/custom-discounted-cash-flow"

    def post_process(self, row: dict, context: Context | None = None) -> dict:
        if "costof_debt" in row:
            row["cost_of_debt"] = row.pop("costof_debt")
        return super().post_process(row)


class CustomDcfLeveredStream(CustomDcfStream):
    name = "custom_dcf_levered"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("year", th.StringType),
        th.Property("revenue", th.NumberType),
        th.Property("revenue_percentage", th.NumberType),
        th.Property("capital_expenditure", th.NumberType),
        th.Property("capital_expenditure_percentage", th.NumberType),
        th.Property("price", th.NumberType),
        th.Property("beta", th.NumberType),
        th.Property("diluted_shares_outstanding", th.NumberType),
        th.Property("cost_of_debt", th.NumberType),
        th.Property("tax_rate", th.NumberType),
        th.Property("after_tax_cost_of_debt", th.NumberType),
        th.Property("risk_free_rate", th.NumberType),
        th.Property("market_risk_premium", th.NumberType),
        th.Property("cost_of_equity", th.NumberType),
        th.Property("total_debt", th.NumberType),
        th.Property("total_equity", th.NumberType),
        th.Property("total_capital", th.NumberType),
        th.Property("debt_weighting", th.NumberType),
        th.Property("equity_weighting", th.NumberType),
        th.Property("wacc", th.NumberType),
        th.Property("operating_cash_flow", th.NumberType),
        th.Property("operating_cash_flow_percentage", th.NumberType),
        th.Property("pv_lfcf", th.NumberType),
        th.Property("sum_pv_lfcf", th.NumberType),
        th.Property("long_term_growth_rate", th.NumberType),
        th.Property("free_cash_flow", th.NumberType),
        th.Property("free_cash_flow_t1", th.NumberType),
        th.Property("terminal_value", th.NumberType),
        th.Property("present_terminal_value", th.NumberType),
        th.Property("enterprise_value", th.NumberType),
        th.Property("net_debt", th.NumberType),
        th.Property("equity_value", th.NumberType),
        th.Property("equity_value_per_share", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/custom-levered-discounted-cash-flow"