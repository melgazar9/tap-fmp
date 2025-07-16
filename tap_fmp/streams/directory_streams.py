import typing as t
from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_fmp.client import FmpRestStream
from tap_fmp.helpers import SymbolFetcher


class CompanySymbolsStream(FmpRestStream):
    """Stream for pulling all company symbols."""

    name = "company_symbols"
    primary_keys = ["symbol", "company_name"]
    _use_cached_symbols_default = False

    schema = th.PropertiesList(
        th.Property("symbol", th.StringType, required=True),
        th.Property("company_name", th.StringType),
    ).to_dict()

    def get_symbol_list(self) -> list[str] | None:
        """Get a list of selected symbols from config."""
        symbols_config = self.config.get("symbols", {})
        selected_symbols = symbols_config.get("select_symbols")

        if not selected_symbols or selected_symbols in ("*", ["*"]):
            return None

        if isinstance(selected_symbols, str):
            return selected_symbols.split(",")

        if isinstance(selected_symbols, list):
            if selected_symbols == ["*"]:
                return None
            return selected_symbols
        return None

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/stock-list?apikey={self.config.get('api_key')}"

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Get symbol records - no partitions, handles all symbols directly."""
        selected_symbols = self.get_symbol_list()

        if not selected_symbols:
            self.logger.info("No specific symbols selected, fetching all symbols...")
            symbol_fetcher = SymbolFetcher(self.config)
            symbol_records = symbol_fetcher.fetch_all_symbols()
        else:
            self.logger.info(f"Processing selected symbols: {selected_symbols}")
            symbol_fetcher = SymbolFetcher(self.config)
            symbol_records = symbol_fetcher.fetch_specific_symbols(selected_symbols)

        for record in symbol_records:
            yield record


class FinancialStatementSymbolsStream(FmpRestStream):
    name = "financial_statement_symbols"
    primary_keys = ["symbol", "company_name"]
    _use_cached_symbols_default = False

    schema = th.PropertiesList(
        th.Property("symbol", th.StringType, required=True),
        th.Property("company_name", th.StringType),
        th.Property("trading_currency", th.StringType),
        th.Property("reporting_currency", th.StringType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/financial-statement-symbol-list"


class CikListStream(FmpRestStream):
    name = "cik_list"
    primary_keys = ["cik", "company_name"]
    _use_cached_symbols_default = False

    schema = th.PropertiesList(
        th.Property("cik", th.StringType, required=True),
        th.Property("company_name", th.StringType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/cik-list"


class SymbolChangesStream(FmpRestStream):
    name = "symbol_changes"
    primary_keys = ["date", "company_name", "old_symbol", "new_symbol"]
    _use_cached_symbols_default = False

    schema = th.PropertiesList(
        th.Property("date", th.DateType, required=True),
        th.Property("company_name", th.StringType, required=True),
        th.Property("old_symbol", th.StringType, required=True),
        th.Property("new_symbol", th.StringType, required=True),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/symbol-change"

class ETFSymbolStream(FmpRestStream):
    name = "etf_symbols"
    primary_keys = ["symbol", "name"]
    _use_cached_symbols_default = False

    schema = th.PropertiesList(
        th.Property("symbol", th.StringType, required=True),
        th.Property("name", th.StringType, required=True),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/etf-list"


class ActivelyTradingStream(FmpRestStream):
    name = "actively_trading"
    primary_keys = ["symbol", "name"]
    _use_cached_symbols_default = False

    schema = th.PropertiesList(
        th.Property("symbol", th.StringType, required=True),
        th.Property("name", th.StringType, required=True),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/actively-trading-list"


class EarningsTranscriptStream(FmpRestStream):
    name = "earnings_transcript_list"
    primary_keys = ["symbol", "company_name", "no_of_transcripts"]
    _use_cached_symbols_default = False

    schema = th.PropertiesList(
        th.Property("symbol", th.StringType, required=True),
        th.Property("company_name", th.StringType),
        th.Property("no_of_transcripts", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/earnings-transcript-list"


class AvailableExchangesStream(FmpRestStream):
    name = "available_exchanges"
    primary_keys = ["exchange", "name"]
    _use_cached_symbols_default = False

    schema = th.PropertiesList(
        th.Property("exchange", th.StringType, required=True),
        th.Property("name", th.StringType, required=True),
        th.Property("country_name", th.StringType),
        th.Property("country_code", th.StringType),
        th.Property("symbol_suffix", th.StringType),
        th.Property("delay", th.StringType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/available-exchanges"


class AvailableSectorsStream(FmpRestStream):
    name = "available_sectors"
    primary_keys = ["sectors"]
    _use_cached_symbols_default = False

    schema = th.PropertiesList(
        th.Property("sector", th.StringType, required=True),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/available-sectors"


class AvailableIndustriesStream(FmpRestStream):
    name = "available_industries"
    primary_keys = ["industry"]
    _use_cached_symbols_default = False

    schema = th.PropertiesList(
        th.Property("industry", th.StringType, required=True),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/available-industries"


class AvailableCountriesStream(FmpRestStream):
    name = "available_countries"
    primary_keys = ["country"]
    _use_cached_symbols_default = False

    schema = th.PropertiesList(
        th.Property("country", th.StringType, required=True),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/available-countries"
