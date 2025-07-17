"""Stream type classes for tap-fmp."""

from __future__ import annotations
from singer_sdk.helpers.types import Context
from singer_sdk import typing as th
from tap_fmp.client import FmpRestStream, SymbolPartitionedStream

### Skipping the below endpoints
# Stock Symbol Search
# Company Name Search
# CIK
# CUSIP
# ISIN

class StockScreenerStream(FmpRestStream):
    name = "stock_screener"

    schema = th.PropertiesList(
        th.Property("symbol", th.StringType, required=True),
        th.Property("company_name", th.StringType),
        th.Property("market_cap", th.NumberType),
        th.Property("sector", th.StringType),
        th.Property("industry", th.StringType),
        th.Property("beta", th.NumberType),
        th.Property("price", th.NumberType),
        th.Property("last_annual_dividend", th.NumberType),
        th.Property("volume", th.NumberType),
        th.Property("exchange", th.StringType),
        th.Property("exchange_short_name", th.StringType),
        th.Property("country", th.StringType),
        th.Property("is_etf", th.BooleanType),
        th.Property("is_fund", th.BooleanType),
        th.Property("is_actively_trading", th.BooleanType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/company-screener"


class ExchangeVariantsStream(SymbolPartitionedStream):
    name = "exchange_variants"

    schema = th.PropertiesList(
        th.Property("symbol", th.StringType),
        th.Property("price", th.NumberType),
        th.Property("beta", th.NumberType),
        th.Property("vol_avg", th.NumberType),
        th.Property("mkt_cap", th.NumberType),
        th.Property("last_div", th.NumberType),
        th.Property("range", th.StringType),
        th.Property("changes", th.NumberType),
        th.Property("company_name", th.StringType),
        th.Property("currency", th.StringType),
        th.Property("cik", th.StringType),
        th.Property("isin", th.StringType),
        th.Property("cusip", th.StringType),
        th.Property("exchange", th.StringType),
        th.Property("exchange_short_name", th.StringType),
        th.Property("industry", th.StringType),
        th.Property("website", th.StringType),
        th.Property("description", th.StringType),
        th.Property("ceo", th.StringType),
        th.Property("sector", th.StringType),
        th.Property("country", th.StringType),
        th.Property("full_time_employees", th.StringType),
        th.Property("phone", th.StringType),
        th.Property("address", th.StringType),
        th.Property("city", th.StringType),
        th.Property("state", th.StringType),
        th.Property("zip", th.StringType),
        th.Property("dcf_diff", th.NumberType),
        th.Property("dcf", th.NumberType),
        th.Property("image", th.StringType),
        th.Property("ipo_date", th.DateTimeType),
        th.Property("default_image", th.BooleanType),
        th.Property("is_etf", th.BooleanType),
        th.Property("is_actively_trading", th.BooleanType),
        th.Property("is_adr", th.BooleanType),
        th.Property("is_fund", th.BooleanType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/search-exchange-variants"
