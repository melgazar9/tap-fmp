"""ESG (Environmental, Social, and Governance) Streams."""

import typing as t
from typing import Any, Generator

from tap_fmp.client import SymbolPartitionStream, FmpRestStream
from singer_sdk.helpers.types import Context
from singer_sdk import typing as th
from datetime import datetime


class EtfStream(SymbolPartitionStream):
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True
    
    @property
    def partitions(self):
        # Use ETF symbols instead of regular stock symbols
        return [{"symbol": s["symbol"]} for s in self._tap.get_cached_etf_symbols()]


class EtfAndFundHoldingsStream(EtfStream):
    name = "etf_and_fund_holdings"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("asset", th.StringType),
        th.Property("name", th.StringType),
        th.Property("isin", th.StringType),
        th.Property("security_cusip", th.StringType),
        th.Property("shares_number", th.NumberType),
        th.Property("weight_percentage", th.NumberType),
        th.Property("market_value", th.NumberType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("updated", th.DateTimeType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/etf/holdings"


class EtfAndMutualFundInformationStream(EtfStream):
    name = "etf_and_mutual_fund_holdings"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("name", th.StringType),
        th.Property("description", th.StringType),
        th.Property("isin", th.StringType),
        th.Property("asset_class", th.StringType),
        th.Property("security_cusip", th.StringType),
        th.Property("domicile", th.StringType),
        th.Property("website", th.StringType),
        th.Property("etf_company", th.StringType),
        th.Property("expense_ratio", th.NumberType),
        th.Property("assets_under_management", th.NumberType),
        th.Property("avg_volume", th.IntegerType),
        th.Property("inception_date", th.DateType),
        th.Property("nav", th.NumberType),
        th.Property("nav_currency", th.StringType),
        th.Property("holdings_count", th.IntegerType),
        th.Property("updated_at", th.DateTimeType),
        th.Property(
            "sectors_list",
            th.ArrayType(
                th.ObjectType(
                    th.Property("industry", th.StringType),
                    th.Property("exposure", th.NumberType),
                )
            ),
        ),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/etf/info"


class EtfAndFundCountryAllocationStream(EtfStream):
    name = "etf_and_fund_country_allocation"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("country", th.StringType, required=True),
        th.Property("weight_percentage", th.StringType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/etf/country-weightings"


class EtfAssetExposureStream(EtfStream):
    name = "etf_asset_exposure"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("asset", th.StringType),
        th.Property("shares_number", th.NumberType),
        th.Property("weight_percentage", th.NumberType),
        th.Property("market_value", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/etf/asset-exposure"


class EtfSectorWeightingStream(EtfStream):
    name = "etf_sector_weighting"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("sector", th.StringType),
        th.Property("weight_percentage", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/etf/sector-weightings"


class MutualFundAndEtfDisclosureStream(EtfStream):
    name = "mutual_fund_and_etf_disclosure"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("cik", th.StringType, required=True),
        th.Property("holder", th.StringType),
        th.Property("shares", th.NumberType),
        th.Property("date_reported", th.DateType),
        th.Property("change", th.NumberType),
        th.Property("weight_percent", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/funds/disclosure-holders-latest"

    def post_process(self, record: dict, context: Context | None = None) -> dict:
        if record and context:
            record["symbol"] = context.get("symbol")
        return super().post_process(record, context)
        


class MutualFundDisclosuresStream(EtfStream):
    name = "mutual_fund_disclosures"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("cik", th.StringType),
        th.Property("date", th.DateType),
        th.Property("accepted_date", th.DateTimeType),
        th.Property("symbol", th.StringType),
        th.Property("name", th.StringType),
        th.Property("lei", th.StringType),
        th.Property("title", th.StringType),
        th.Property("cusip", th.StringType),
        th.Property("isin", th.StringType),
        th.Property("balance", th.NumberType),
        th.Property("units", th.StringType),
        th.Property("cur_cd", th.StringType),
        th.Property("val_usd", th.NumberType),
        th.Property("pct_val", th.NumberType),
        th.Property("payoff_profile", th.StringType),
        th.Property("asset_cat", th.StringType),
        th.Property("issuer_cat", th.StringType),
        th.Property("inv_country", th.StringType),
        th.Property("is_restricted_sec", th.StringType),
        th.Property("fair_val_level", th.StringType),
        th.Property("is_cash_collateral", th.StringType),
        th.Property("is_non_cash_collateral", th.StringType),
        th.Property("is_loan_by_fund", th.StringType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/funds/disclosure"

    @property
    def partitions(self):
        quarters = [1, 2, 3, 4]
        years = [str(y) for y in range(2020, datetime.today().year + 1)]
        etf_symbols = self._tap.get_cached_etf_symbols()
        mutual_fund_partitions = [
            {"quarter": str(q), "year": str(y), "symbol": s["symbol"]}
            for q in quarters
            for y in years
            for s in etf_symbols
        ]
        return mutual_fund_partitions

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        if context:
            self.query_params.update({
                "quarter": context.get("quarter"),
                "year": context.get("year"), 
                "symbol": context.get("symbol")
            })
        yield from super().get_records(context)


class MutualFundAndEtfDisclosureNameSearchStream(FmpRestStream):
    """Search for mutual fund and ETF disclosure information by name."""

    name = "mutual_fund_and_etf_disclosure_name_search"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("cik", th.StringType),
        th.Property("class_id", th.StringType),
        th.Property("series_id", th.StringType),
        th.Property("entity_name", th.StringType),
        th.Property("entity_org_type", th.StringType),
        th.Property("series_name", th.StringType),
        th.Property("class_name", th.StringType),
        th.Property("reporting_file_number", th.StringType),
        th.Property("address", th.StringType),
        th.Property("city", th.StringType),
        th.Property("zip_code", th.StringType),
        th.Property("state", th.StringType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/funds/disclosure-holders-search"


class FundAndEtfDisclosuresByDateStream(EtfStream):
    name = "fund_and_etf_disclosures_by_date"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("date", th.StringType, required=True),
        th.Property("year", th.IntegerType),
        th.Property("quarter", th.IntegerType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/funds/disclosure-dates"
