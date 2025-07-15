"""Stream type classes for tap-fmp."""

from __future__ import annotations

import typing as t
from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_fmp.client import FMPStream, TickerFetcher


class TickersStream(FMPStream):
    """Stream for pulling all tickers."""

    name = "symbols"
    primary_keys = ["symbol", "company_name"]
    _use_cached_tickers_default = False

    schema = th.PropertiesList(
        th.Property("symbol", th.StringType, required=True),
        th.Property("company_name", th.StringType),
    ).to_dict()

    def get_ticker_list(self) -> list[str] | None:
        """Get list of selected tickers from config."""
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


class HistoricalRatingsStream(FMPStream):
    """Stream for historical rating data."""

    name = "historical_ratings"
    primary_keys = ["symbol", "date"]

    schema = th.PropertiesList(
        th.Property("symbol", th.StringType, required=True),
        th.Property("date", th.DateType, required=True),
        th.Property("rating", th.StringType),
        th.Property("overall_score", th.NumberType),
        th.Property("discounted_cash_flow_score", th.NumberType),
        th.Property("return_on_equity_score", th.NumberType),
        th.Property("return_on_assets_score", th.NumberType),
        th.Property("debt_to_equity_score", th.NumberType),
        th.Property("price_to_earnings_score", th.NumberType),
        th.Property("price_to_book_score", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context) -> str:
        return (
            f"{self.url_base}/stable/ratings-historical?symbol="
            f"{context.get('ticker')}&apikey={self.config.get('api_key')}"
        )