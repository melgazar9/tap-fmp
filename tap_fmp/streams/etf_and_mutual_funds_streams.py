"""ESG (Environmental, Social, and Governance) Streams. """

import typing as t
from typing import Any, Generator

from tap_fmp.client import SymbolPartitionStream, FmpRestStream
from singer_sdk.helpers.types import Context
from singer_sdk import typing as th
from datetime import datetime
from tap_fmp.helpers import generate_surrogate_key


class EtfStream(SymbolPartitionStream):
    primary_keys = ["surrogate_key"]

    def post_process(self, row: dict, context: Context | None = None) -> dict:
        row["surrogate_key"] = generate_surrogate_key(row)
        return row


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
        th.Property("cik", th.StringType, required=True),
        th.Property("holder", th.StringType),
        th.Property("shares", th.NumberType),
        th.Property("date_reported", th.DateType),
        th.Property("change", th.NumberType),
        th.Property("weight_percent", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/etf/disclosure-holders-latest"

class MutualFundDisclosuresStream(FmpRestStream):
    name = "mutual_fund_disclosures"
    _use_cached_symbols_default = True

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
        return f"{self.url_base}/stable/etf/disclosure"

    @property
    def partitions(self):
        quarters = [1, 2, 3, 4]
        years = [i + 1 for i in range(2015, datetime.today().year)]
        symbols = self._tap.get_cached_symbols()
        mutual_fund_partitions = [
            {"quarter": str(q), "year": str(y), "symbol": s}
            for q in quarters
            for y in years
            for s in symbols
        ]
        return mutual_fund_partitions

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        self.query_params.update(context.get("quarter"), context.get("year"), context.get("symbol"))
        yield from super().get_records(context)

class MutualFundAndEtfDisclosureNameSearchStream(FmpRestStream):
    """ Ignore for now, just keep as a placeholder. Need to supply 'name' as a query parameter. """

    name = "mutual_fund_and_etf_disclosure_name_search"

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
        return f"{self.url_base}/stable/etf/disclosure-holders-search"

class FundAndEtfDisclosuresByDateStream(EtfStream):
    name = "fund_and_etf_disclosures_by_date"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("date", th.StringType, required=True),
        th.Property("year", th.StringType),
        th.Property("quarter", th.StringType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/etf/holdings"

    def post_process(self, row: dict, context: Context | None = None) -> Generator[Any, None, None]:
        row["symbol"] = context.get("symbol")
        yield from super().post_process(row, context)