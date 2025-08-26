import typing as t
from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_fmp.client import (
    FmpRestStream,
    FmpSurrogateKeyStream,
    SymbolFetcher,
    CikFetcher,
    ExchangeFetcher,
)
from tap_fmp.mixins import SelectableStreamMixin


class CompanySymbolsStream(SymbolFetcher):
    """Stream for pulling all company symbols."""

    name = "company_symbols"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("company_name", th.StringType),
    ).to_dict()

    def get_symbol_list(self) -> list[str] | None:
        """Get a list of selected symbols from config."""
        symbols_config = self.config.get("company_symbols", {})
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

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/stock-list"

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        selected_symbols = self.get_symbol_list()
        if not selected_symbols:
            self.logger.info("No specific symbols selected, fetching all symbols...")
            symbol_records = self.fetch_all_symbols()
        else:
            self.logger.info(f"Processing selected symbols: {selected_symbols}")
            symbol_records = self.fetch_specific_symbols(selected_symbols)
        for record in symbol_records:
            record = self.post_process(record)
            self._check_missing_fields(self.schema, record)
            yield record


class FinancialStatementSymbolsStream(FmpRestStream):
    name = "financial_statement_symbols"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("company_name", th.StringType),
        th.Property("trading_currency", th.StringType),
        th.Property("reporting_currency", th.StringType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/financial-statement-symbol-list"


class CikListStream(CikFetcher):
    name = "cik_list"
    primary_keys = ["surrogate_key"]
    _paginate = True
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("cik", th.StringType, required=True),
        th.Property("company_name", th.StringType),
    ).to_dict()

    def get_cik_list(self) -> list[str] | None:
        """Get a list of selected CIKs from config."""
        ciks_config = self.config.get("ciks", {})
        selected_ciks = ciks_config.get("select_ciks")

        if not selected_ciks or selected_ciks in ("*", ["*"]):
            return None

        if isinstance(selected_ciks, str):
            return selected_ciks.split(",")

        if isinstance(selected_ciks, list):
            if selected_ciks == ["*"]:
                return None
            return selected_ciks
        return None

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/cik-list"

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Get CIK records - no partitions, handles all CIKs directly."""
        selected_ciks = self.get_cik_list()

        if not selected_ciks:
            self.logger.info("No specific CIKs selected, fetching all CIKs...")
            cik_records = self.fetch_all_ciks(context)
        else:
            self.logger.info(f"Processing selected CIKs: {selected_ciks}")
            cik_records = self.fetch_specific_ciks(selected_ciks)

        for record in cik_records:
            record = self.post_process(record)
            self._check_missing_fields(self.schema, record)
            yield record


class SymbolChangesStream(FmpRestStream):
    name = "symbol_changes"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("date", th.DateType, required=True),
        th.Property("company_name", th.StringType),
        th.Property("old_symbol", th.StringType),
        th.Property("new_symbol", th.StringType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/symbol-change"


class ETFSymbolStream(FmpRestStream):
    name = "etf_symbols"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("name", th.StringType),
    ).to_dict()

    def get_etf_list(self) -> list[str] | None:
        """Get a list of selected ETF symbols from config."""
        etf_symbols_config = self.config.get("etf_symbols", {})
        selected_etf_symbols = etf_symbols_config.get("select_etf_symbols")

        if not selected_etf_symbols or selected_etf_symbols in ("*", ["*"]):
            return None

        if isinstance(selected_etf_symbols, str):
            return selected_etf_symbols.split(",")

        if isinstance(selected_etf_symbols, list):
            if selected_etf_symbols == ["*"]:
                return None
            return selected_etf_symbols
        return None

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/etf-list"

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        selected_etf_symbols = self.get_etf_list()

        if not selected_etf_symbols:
            self.logger.info(
                "No specific ETF symbols selected, fetching all ETF symbols..."
            )
            etf_symbol_records = self.fetch_all_etf_symbols(context)
        else:
            self.logger.info(f"Processing selected ETF symbols: {selected_etf_symbols}")
            etf_symbol_records = self.fetch_specific_etf_symbols(selected_etf_symbols)

        for record in etf_symbol_records:
            record = self.post_process(record)
            self._check_missing_fields(self.schema, record)
            yield record

    def fetch_all_etf_symbols(self, context: Context | None = None) -> list[dict]:
        url = self.get_url(context)
        return self._fetch_with_retry(url, self.query_params)

    @staticmethod
    def fetch_specific_etf_symbols(etf_symbol_list: list[str]) -> list[dict]:
        """Create ETF symbol records for a specific list of ETF symbols."""
        if isinstance(etf_symbol_list, str):
            return [{"symbol": etf_symbol_list.upper(), "name": None}]
        return [
            {
                "symbol": etf_symbol.upper(),
                "name": None,
            }
            for etf_symbol in etf_symbol_list
        ]


class ActivelyTradingStream(FmpSurrogateKeyStream):
    name = "actively_trading"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("name", th.StringType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/actively-trading-list"


class EarningsTranscriptListStream(FmpRestStream):
    name = "earnings_transcript_list"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("company_name", th.StringType),
        th.Property("no_of_transcripts", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/earnings-transcript-list"


class AvailableExchangesStream(ExchangeFetcher):
    """Stream for pulling all exchanges."""

    name = "available_exchanges"
    primary_keys = ["exchange", "name"]
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("exchange", th.StringType, required=True),
        th.Property("name", th.StringType, required=True),
        th.Property("opening_hour", th.StringType),
        th.Property("closing_hour", th.StringType),
        th.Property("timezone", th.StringType),
        th.Property("is_market_open", th.BooleanType),
        th.Property("closing_additional", th.StringType),
        th.Property("opening_additional", th.StringType),
    ).to_dict()

    def get_exchange_list(self) -> list[str] | None:
        """Get a list of selected exchanges from config."""

        exchanges_config = self.config.get("exchanges", {})
        selected_exchanges = exchanges_config.get("select_exchanges")

        if not selected_exchanges or selected_exchanges in ("*", ["*"]):
            return None

        if isinstance(selected_exchanges, str) and selected_exchanges != "*":
            return selected_exchanges.split(",")
        elif selected_exchanges == "*":
            return None

        if isinstance(selected_exchanges, list):
            if selected_exchanges == ["*"]:
                return None
            return selected_exchanges
        return None

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        selected_exchanges = self.get_exchange_list()

        if not selected_exchanges:
            self.logger.info(
                "No specific exchanges selected, fetching all exchanges..."
            )
            exchange_records = self.fetch_all_exchanges()
        else:
            self.logger.info(f"Processing selected exchanges: {selected_exchanges}")
            exchange_records = self.fetch_specific_exchanges(selected_exchanges)

        for record in exchange_records:
            record = self.post_process(record)
            self._check_missing_fields(self.schema, record)
            yield record


class AvailableSectorsStream(SelectableStreamMixin, FmpRestStream):
    name = "available_sectors"
    primary_keys = ["sector"]

    schema = th.PropertiesList(
        th.Property("sector", th.StringType, required=True),
    ).to_dict()

    @property
    def selection_config_key(self) -> str:
        return "sectors"

    @property
    def selection_field_key(self) -> str:
        return "select_sectors"

    @property
    def item_name_singular(self) -> str:
        return "sector"

    @property
    def item_name_plural(self) -> str:
        return "sectors"

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/available-sectors"


class AvailableIndustriesStream(SelectableStreamMixin, FmpRestStream):
    name = "available_industries"
    primary_keys = ["industry"]

    schema = th.PropertiesList(
        th.Property("industry", th.StringType, required=True),
    ).to_dict()

    @property
    def selection_config_key(self) -> str:
        return "industries"

    @property
    def selection_field_key(self) -> str:
        return "select_industries"

    @property
    def item_name_singular(self) -> str:
        return "industry"

    @property
    def item_name_plural(self) -> str:
        return "industries"

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/available-industries"


class AvailableCountriesStream(FmpRestStream):
    name = "available_countries"
    primary_keys = ["country"]

    schema = th.PropertiesList(
        th.Property("country", th.StringType, required=True),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/available-countries"
