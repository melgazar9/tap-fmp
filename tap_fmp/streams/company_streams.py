import typing as t

from tap_fmp.client import (
    FmpRestStream,
    SymbolPartitionStream,
    SymbolPartitionTimeSliceStream,
    IncrementalYearStream,
)

from singer_sdk.helpers.types import Context
from singer_sdk import typing as th

from tap_fmp.mixins import BatchSymbolPartitionMixin, CompanyBatchStreamMixin


class CompanySymbolPartitionStream(SymbolPartitionStream):
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True


class CompanyProfileBySymbolStream(CompanySymbolPartitionStream):
    name = "company_profile_by_symbol"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("price", th.NumberType),
        th.Property("market_cap", th.NumberType),
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
        th.Property("full_time_employees", th.StringType),
        th.Property("phone", th.StringType),
        th.Property("address", th.StringType),
        th.Property("city", th.StringType),
        th.Property("state", th.StringType),
        th.Property("zip", th.StringType),
        th.Property("image", th.StringType),
        th.Property("ipo_date", th.StringType),
        th.Property("default_image", th.BooleanType),
        th.Property("is_etf", th.BooleanType),
        th.Property("is_actively_trading", th.BooleanType),
        th.Property("is_adr", th.BooleanType),
        th.Property("is_fund", th.BooleanType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/profile"


class CikProfileStream(FmpRestStream):
    name = "company_profile_by_cik"
    primary_keys = ["surrogate_key"]

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("cik", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("price", th.NumberType),
        th.Property("market_cap", th.NumberType),
        th.Property("beta", th.NumberType),
        th.Property("last_dividend", th.NumberType),
        th.Property("range", th.StringType),
        th.Property("change", th.NumberType),
        th.Property("change_percentage", th.NumberType),
        th.Property("volume", th.NumberType),
        th.Property("average_volume", th.NumberType),
        th.Property("company_name", th.StringType),
        th.Property("currency", th.StringType),
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
        th.Property("full_time_employees", th.StringType),
        th.Property("phone", th.StringType),
        th.Property("address", th.StringType),
        th.Property("city", th.StringType),
        th.Property("state", th.StringType),
        th.Property("zip", th.StringType),
        th.Property("image", th.StringType),
        th.Property("ipo_date", th.StringType),
        th.Property("default_image", th.BooleanType),
        th.Property("is_etf", th.BooleanType),
        th.Property("is_actively_trading", th.BooleanType),
        th.Property("is_adr", th.BooleanType),
        th.Property("is_fund", th.BooleanType),
    ).to_dict()

    _add_surrogate_key = True

    @property
    def partitions(self):
        return [{"cik": c["cik"]} for c in self._tap.get_cached_ciks()]

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/profile-cik"

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        self.query_params.update({"cik": context.get("cik")})
        yield from super().get_records(context)

    def post_process(self, row: dict, context: Context | None = None) -> dict:
        row["cik"] = context.get("cik")
        return super().post_process(row, context)


class CompanyNotesStream(CompanySymbolPartitionStream):
    name = "company_notes"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("cik", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("title", th.StringType),
        th.Property("exchange", th.StringType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/company-notes"


class StockPeerComparisonStream(CompanySymbolPartitionStream):
    name = "stock_peer_comparison"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("company_name", th.StringType),
        th.Property("price", th.NumberType),
        th.Property("mkt_cap", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/stock-peers"


class DelistedCompaniesStream(FmpRestStream):
    name = "delisted_companies"
    _paginate = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("company_name", th.StringType),
        th.Property("exchange", th.StringType),
        th.Property("ipo_date", th.StringType),
        th.Property("delisted_date", th.StringType),
    ).to_dict()

    _add_surrogate_key = True

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/delisted-companies"


class CompanyEmployeeCountStream(CompanySymbolPartitionStream):
    name = "company_employee_count"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("cik", th.StringType),
        th.Property("acceptance_time", th.DateTimeType),
        th.Property("period_of_report", th.DateType),
        th.Property("company_name", th.StringType),
        th.Property("form_type", th.StringType),
        th.Property("filing_date", th.DateType),
        th.Property("employee_count", th.IntegerType),
        th.Property("source", th.StringType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/employee-count"


class CompanyHistoricalEmployeeCountStream(CompanyEmployeeCountStream):
    name = "company_historical_employee_count"

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/historical-employee-count"


class CompanyMarketCapStream(SymbolPartitionStream):
    name = "company_market_cap"
    primary_keys = ["symbol", "date"]

    schema = th.PropertiesList(
        th.Property("symbol", th.StringType),
        th.Property("date", th.DateType),
        th.Property("market_cap", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/market-capitalization"


class CompanyBatchMarketCapStream(BatchSymbolPartitionMixin, CompanyBatchStreamMixin, FmpRestStream):
    name = "company_batch_market_cap"
    primary_keys = ["symbol", "date"]

    schema = th.PropertiesList(
        th.Property("symbol", th.StringType),
        th.Property("date", th.DateType),
        th.Property("market_cap", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/market-capitalization-batch"


class HistoricalMarketCapStream(SymbolPartitionTimeSliceStream):
    name = "historical_market_cap"
    primary_keys = ["symbol", "date"]

    schema = th.PropertiesList(
        th.Property("symbol", th.StringType),
        th.Property("date", th.DateType),
        th.Property("market_cap", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/historical-market-capitalization"


class CompanyShareAndLiquidityFloatStream(CompanySymbolPartitionStream):
    name = "company_share_and_liquidity_float"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("date", th.DateTimeType),
        th.Property("free_float", th.NumberType),
        th.Property("float_shares", th.NumberType),
        th.Property("outstanding_shares", th.NumberType),
        th.Property("source", th.StringType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/shares-float"


class AllSharesFloatStream(FmpRestStream):
    name = "all_shares_float"
    primary_keys = ["surrogate_key"]
    _paginate = True
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("date", th.DateTimeType),
        th.Property("free_float", th.NumberType),
        th.Property("float_shares", th.NumberType),
        th.Property("outstanding_shares", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/shares-float-all"


class LatestMergersAndAcquisitionsStream(FmpRestStream):
    name = "latest_mergers_and_acquisitions"
    primary_keys = ["surrogate_key"]
    _paginate = True
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("company_name", th.StringType),
        th.Property("cik", th.StringType),
        th.Property("targeted_company_name", th.StringType),
        th.Property("targeted_cik", th.StringType),
        th.Property("targeted_symbol", th.StringType),
        th.Property("transaction_date", th.DateType),
        th.Property("accepted_date", th.DateTimeType),
        th.Property("link", th.StringType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/mergers-acquisitions-latest"


class SearchMergersAndAcquisitionsStream(LatestMergersAndAcquisitionsStream):
    """Ignore this stream, it's just here as a placeholder to show it exists."""

    name = "search_mergers_and_acquisitions"
    primary_keys = ["surrogate_key"]
    _paginate = False

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/mergers-acquisitions-search"


class CompanyExecutiveStream(CompanySymbolPartitionStream):
    name = "company_executives"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("title", th.StringType),
        th.Property("name", th.StringType),
        th.Property("pay", th.NumberType),
        th.Property("currency_pay", th.StringType),
        th.Property("gender", th.StringType),
        th.Property("year_born", th.IntegerType),
        th.Property("active", th.BooleanType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/key-executives"


class ExecutiveCompensationStream(CompanySymbolPartitionStream):
    name = "executive_compensation"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("cik", th.StringType),
        th.Property("symbol", th.StringType),
        th.Property("company_name", th.StringType),
        th.Property("filing_date", th.StringType),
        th.Property("accepted_date", th.DateTimeType),
        th.Property("name_and_position", th.StringType),
        th.Property("year", th.IntegerType),
        th.Property("salary", th.NumberType),
        th.Property("bonus", th.NumberType),
        th.Property("stock_award", th.NumberType),
        th.Property("option_award", th.NumberType),
        th.Property("incentive_plan_compensation", th.NumberType),
        th.Property("all_other_compensation", th.NumberType),
        th.Property("total", th.NumberType),
        th.Property("link", th.StringType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/governance-executive-compensation"


class ExecutiveCompensationBenchmarkStream(IncrementalYearStream):
    name = "executive_compensation_benchmark"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("industry_title", th.StringType),
        th.Property("year", th.NumberType),
        th.Property("average_compensation", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/executive-compensation-benchmark"
