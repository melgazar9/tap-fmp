import typing as t
from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_fmp.client import FmpRestStream, TickerFetcher

class TickersStream(FmpRestStream):
    """Stream for pulling all tickers."""

    name = "tickers"
    primary_keys = ["ticker", "company_name"]
    _use_cached_tickers_default = False

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType, required=True),
        th.Property("company_name", th.StringType),
    ).to_dict()

    def get_ticker_list(self) -> list[str] | None:
        """Get a list of selected tickers from config."""
        tickers_config = self.config.get("tickers", {})
        selected_tickers = tickers_config.get("select_tickers")

        if not selected_tickers or selected_tickers in ("*", ["*"]):
            return None

        if isinstance(selected_tickers, str):
            return selected_tickers.split(",")

        if isinstance(selected_tickers, list):
            if selected_tickers == ["*"]:
                return None
            return selected_tickers

        return None

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Get ticker records - no partitions, handles all tickers directly."""
        selected_tickers = self.get_ticker_list()

        if not selected_tickers:
            self.logger.info("No specific tickers selected, fetching all tickers...")
            ticker_fetcher = TickerFetcher()
            ticker_records = ticker_fetcher.fetch_all_tickers()
        else:
            self.logger.info(f"Processing selected tickers: {selected_tickers}")
            ticker_fetcher = TickerFetcher()
            ticker_records = ticker_fetcher.fetch_specific_tickers(selected_tickers)

        for record in ticker_records:
            yield record

class CompanySymbolsListStream(FmpRestStream):
    _use_cached_tickers_default = False

    def get_url(self, context: Context):
        return f"{self.url_base()}/stable/stock-list"

class FinancialStatementSymbolsListStream(FmpRestStream):
    _use_cached_tickers_default = False

    def get_url(self, context: Context):
        return f"{self.url_base()}/stable/financial-statement-symbol-list"

class CIKListStream(FmpRestStream):
    _use_cached_tickers_default = False

    def get_url(self, context: Context):
        return f"{self.url_base()}/stable/cik-list"

class SymbolChangesListStream(FmpRestStream):
    _use_cached_tickers_default = False

    def get_url(self, context: Context):
        return f"{self.url_base()}/stable/symbol-change"

class ETFSymbolListStream(FmpRestStream):
    _use_cached_tickers_default = False

    def get_url(self, context: Context):
        return f"{self.url_base()}/stable/etf-list"

class ActivelyTradingListStream(FmpRestStream):
    _use_cached_tickers_default = False

    def get_url(self, context: Context):
        return f"{self.url_base()}/stable/actively-trading-list"

class EarningsTranscriptListStream(FmpRestStream):
    _use_cached_tickers_default = False

    def get_url(self, context: Context):
        return f"{self.url_base()}/stable/earnings-transcript-list"

class AvailableExchangesStream(FmpRestStream):
    _use_cached_tickers_default = False

    def get_url(self, context: Context):
        return f"{self.url_base()}/stable/available-exchanges"

class AvailableSectorsStream(FmpRestStream):
    _use_cached_tickers_default = False

    def get_url(self, context: Context):
        return f"{self.url_base()}/stable/available-sectors"


class AvailableIndustriesStream(FmpRestStream):
    _use_cached_tickers_default = False

    def get_url(self, context: Context):
        return f"{self.url_base()}/stable/available-industries"


class AvailableCountriesStream(FmpRestStream):
    _use_cached_tickers_default = False

    def get_url(self, context: Context):
        return f"{self.url_base()}/stable/available-countries"