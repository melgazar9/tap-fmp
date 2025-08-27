"""ETF and Mutual Fund Streams."""

import typing as t

from tap_fmp.client import FmpSurrogateKeyStream, SymbolPartitionStream
from tap_fmp.mixins import BaseSymbolPartitionMixin, EtfConfigMixin
from singer_sdk.helpers.types import Context
from singer_sdk import typing as th
from datetime import datetime


class EtfListStream(EtfConfigMixin, FmpSurrogateKeyStream):
    """Stream for ETF List API."""

    name = "etf_list"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("name", th.StringType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        """Get URL for the request."""
        return f"{self.url_base}/stable/etf-list"


class EtfSymbolPartitionMixin(BaseSymbolPartitionMixin):

    @property
    def selection_config_section(self) -> str:
        return "etf_symbols"

    @property
    def selection_field_name(self) -> str:
        return "select_etf_symbols"

    def get_cached_symbols(self) -> list[dict]:
        return self._tap.get_cached_etf_symbols()


class EtfAndFundHoldingsStream(
    EtfSymbolPartitionMixin, SymbolPartitionStream, FmpSurrogateKeyStream
):
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


class EtfAndMutualFundInformationStream(
    EtfSymbolPartitionMixin, SymbolPartitionStream, FmpSurrogateKeyStream
):
    name = "etf_and_mutual_fund_info"

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
        th.Property("avg_volume", th.NumberType),
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

    def post_process(self, record: dict, context: Context | None = None) -> dict:
        record["symbol"] = context.get("symbol")
        return super().post_process(record, context)


class EtfAndFundCountryAllocationStream(
    EtfSymbolPartitionMixin, SymbolPartitionStream, FmpSurrogateKeyStream
):
    name = "etf_and_fund_country_allocation"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("country", th.StringType),
        th.Property("weight_percentage", th.StringType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/etf/country-weightings"

    def post_process(self, row: dict, context: Context | None = None) -> dict:
        row["symbol"] = context.get("symbol")
        if "weight_percentage" in row:
            row["weight_percentage"] = str(row["weight_percentage"])
        return super().post_process(row, context)


class EtfAssetExposureStream(
    EtfSymbolPartitionMixin, SymbolPartitionStream, FmpSurrogateKeyStream
):
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


class EtfSectorWeightingStream(
    EtfSymbolPartitionMixin, SymbolPartitionStream, FmpSurrogateKeyStream
):
    name = "etf_sector_weighting"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("sector", th.StringType),
        th.Property("weight_percentage", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/etf/sector-weightings"


class MutualFundAndEtfDisclosureStream(
    EtfSymbolPartitionMixin, SymbolPartitionStream, FmpSurrogateKeyStream
):
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


class MutualFundDisclosuresStream(
    EtfSymbolPartitionMixin, SymbolPartitionStream, FmpSurrogateKeyStream
):
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

        symbol_data = self.get_cached_symbols()

        mutual_fund_partitions = [
            {"quarter": str(q), "year": str(y), "symbol": symbol["symbol"]}
            for q in quarters
            for y in years
            for symbol in symbol_data
        ]
        return mutual_fund_partitions

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        if context:
            self.query_params.update(
                {
                    "quarter": context.get("quarter"),
                    "year": context.get("year"),
                    "symbol": context.get("symbol"),
                }
            )
        yield from super().get_records(context)


class MutualFundAndEtfDisclosureNameSearchStream(FmpSurrogateKeyStream):
    """Search for mutual fund and ETF disclosure information by name."""

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

    @property
    def partitions(self):
        stream_config = self.config.get(self.name, {})
        query_params_name = stream_config.get("query_params", {}).get("name")
        other_params_names = stream_config.get("other_params", {}).get("names")

        assert not (query_params_name and other_params_names), (
            f"Cannot specify name configurations in both query_params and "
            f"other_params for stream {self.name}."
        )

        if query_params_name:
            return (
                [{"name": query_params_name}]
                if isinstance(query_params_name, str)
                else query_params_name
            )
        elif other_params_names:
            return (
                [{"name": name} for name in other_params_names]
                if isinstance(other_params_names, list)
                else other_params_names
            )
        else:
            raise ValueError(
                f"Stream '{self.name}' requires name configuration. "
                f"Configure either 'query_params.name' or 'other_params.names' "
                f"for this search stream."
            )

    def get_records(self, context: Context | None):
        self.query_params.update(context)
        return super().get_records(context)

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/funds/disclosure-holders-search"


class FundAndEtfDisclosuresByDateStream(
    EtfSymbolPartitionMixin, SymbolPartitionStream, FmpSurrogateKeyStream
):
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
