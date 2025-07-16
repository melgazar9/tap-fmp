import typing as t
from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_fmp.client import FmpRestStream
from tap_fmp.helpers import SymbolFetcher, fmp_api_retry, clean_json_keys


class SymbolsStream(FmpRestStream):
    """Stream for pulling all Symbols."""

    name = "symbols"
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
        return f"{self.url_base}stable/stock-list?apikey={self.config.get('api_key')}"

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


class CompanySymbolsListStream(FmpRestStream):
    _use_cached_symbols_default = False

    def get_url(self, context: Context):
        return f"{self.url_base()}/stable/stock-list"


class FinancialStatementSymbolsListStream(FmpRestStream):
    _use_cached_symbols_default = False

    def get_url(self, context: Context):
        return f"{self.url_base()}/stable/financial-statement-symbol-list"


class CIKListStream(FmpRestStream):
    _use_cached_symbols_default = False

    def get_url(self, context: Context):
        return f"{self.url_base()}/stable/cik-list"


class SymbolChangesListStream(FmpRestStream):
    _use_cached_symbols_default = False

    def get_url(self, context: Context):
        return f"{self.url_base()}/stable/symbol-change"


class ETFSymbolListStream(FmpRestStream):
    _use_cached_symbols_default = False

    def get_url(self, context: Context):
        return f"{self.url_base()}/stable/etf-list"


class ActivelyTradingListStream(FmpRestStream):
    _use_cached_symbols_default = False

    def get_url(self, context: Context):
        return f"{self.url_base()}/stable/actively-trading-list"


class EarningsTranscriptListStream(FmpRestStream):
    _use_cached_symbols_default = False

    def get_url(self, context: Context):
        return f"{self.url_base()}/stable/earnings-transcript-list"


class AvailableExchangesStream(FmpRestStream):
    _use_cached_symbols_default = False

    def get_url(self, context: Context):
        return f"{self.url_base()}/stable/available-exchanges"


class AvailableSectorsStream(FmpRestStream):
    _use_cached_symbols_default = False

    def get_url(self, context: Context):
        return f"{self.url_base()}/stable/available-sectors"


class AvailableIndustriesStream(FmpRestStream):
    _use_cached_symbols_default = False

    def get_url(self, context: Context):
        return f"{self.url_base()}/stable/available-industries"


class AvailableCountriesStream(FmpRestStream):
    _use_cached_symbols_default = False

    def get_url(self, context: Context):
        return f"{self.url_base()}/stable/available-countries"
