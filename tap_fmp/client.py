"""REST client handling, including FmpRestStream base class."""

from __future__ import annotations
from abc import ABC
from singer_sdk.helpers.types import Context
from typing import Union
from singer_sdk.streams import Stream
from singer_sdk import Tap
from tap_fmp.helpers import fmp_api_retry, TickerFetcher, clean_json_keys
import typing as t
import logging
import requests


class FmpRestStream(Stream, ABC):
    """FMP stream class with ticker partitioning support."""

    _use_cached_tickers_default = True
    _valid_segments = None

    def __init__(self, tap: Tap) -> None:
        super().__init__(tap)
        self._all_tickers = None

    def _get_stream_config(self) -> dict:
        """Get configuration for this specific stream."""
        return self.config.get(self.name, {})

    @property
    def use_cached_tickers(self) -> bool:
        """Whether to use cached tickers for this stream."""
        stream_config = self._get_stream_config()

        if "use_cached_tickers" in stream_config:
            use_cached_tickers = stream_config["use_cached_tickers"]
            assert isinstance(use_cached_tickers, bool), (
                f"Config for {self.name}.use_cached_tickers must be bool, "
                f"got {type(use_cached_tickers)}"
            )
            return use_cached_tickers

        if hasattr(type(self), "_use_cached_tickers_default"):
            return getattr(type(self), "_use_cached_tickers_default")

        raise AttributeError(
            f"use_cached_tickers is not defined for stream {self.name}"
        )

    @property
    def partitions(self) -> list[dict] | None:
        """Get partitions for ticker-based streams with segment filtering."""
        if not self.use_cached_tickers:
            return None

        ticker_fetcher = TickerFetcher()
        ticker_list = self.config.get("ticker_list") or self.config.get(
            "tickers", {}
        ).get("select_tickers")

        if ticker_list and ticker_list not in ("*", ["*"]):
            ticker_records = ticker_fetcher.fetch_specific_tickers(ticker_list)
            self.logger.info(
                f"{self.name}: Using specific tickers from config: {ticker_list}"
            )
        else:
            ticker_records = self._tap.get_cached_tickers()
            self.logger.info(f"{self.name}: Using cached tickers from tap")

        self._all_tickers = {ticker["ticker"]: ticker for ticker in ticker_records}

        filtered_tickers = self._filter_tickers_by_segments(
            ticker_records, allowed_segments=self._valid_segments
        )

        partitions = [{"ticker": ticker["ticker"]} for ticker in filtered_tickers]

        self.logger.info(f"Created {len(partitions)} ticker partitions for {self.name}")
        return partitions

    @property
    def url_base(self) -> str:
        return self.config.get("base_url", "https://financialmodelingprep.com")

    def _filter_tickers_by_segments(
        self, tickers: list[dict], allowed_segments: list[str] | None = None
    ) -> list[dict]:
        """Centralized ticker filtering by segments."""
        if allowed_segments is None:
            self.logger.info(
                f"{self.name}: Processing all {len(tickers)} tickers (no segment filtering)"
            )
            return tickers

        original_count = len(tickers)

        filtered_tickers = [
            ticker for ticker in tickers if ticker.get("segment") in allowed_segments
        ]

        self.logger.info(
            f"{self.name}: Filtered to {len(filtered_tickers)} tickers from {original_count} "
            f"(allowed segments: {allowed_segments})"
        )

        excluded_tickers = [
            f"{t['symbol']} ({t.get('segment', 'unknown')})"
            for t in tickers
            if t not in filtered_tickers
        ]
        if excluded_tickers:
            self.logger.info(f"{self.name}: Excluded tickers: {excluded_tickers}")

        return filtered_tickers

    def _is_valid_ticker_for_stream(self, ticker: str) -> bool:
        """
        Check if ticker is valid for this stream based on segment.
        Uses the actual segment data instead of regex patterns.
        """
        if not self._valid_segments:
            return True  # No segment restrictions

        if not self._all_tickers:
            return True  # No ticker data available, allow through

        ticker_data = self._all_tickers.get(ticker)
        if not ticker_data:
            return False

        return ticker_data.get("segment") in self._valid_segments

    def _get_ticker_from_context(self, context: Context) -> Union[str, None]:
        """Validates and returns ticker from context."""
        context = context or {}
        ticker = context.get("ticker")
        if not ticker:
            self.logger.error("No ticker found in context")
            return None

        if not self._is_valid_ticker_for_stream(ticker):
            self.logger.warning(
                f"Skipping {ticker} - not valid for {self.name} stream based on segment rules"
            )
            return None

        return ticker

    @fmp_api_retry
    def _fetch_with_retry(self, url, query_params=None) -> dict:
        """Centralized API call with retry logic."""
        query_params = {} if query_params is None else query_params
        response = requests.get(url, params=query_params)
        response.raise_for_status()
        records = response.json()
        records = clean_json_keys(records)
        return records

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        records = self._fetch_with_retry(
            self.get_url(context), context.get("query_params")
        )
        for record in records:
            yield record


class CachedTickerProvider:
    """Provider for cached tickers (matching tap-fmp pattern)."""

    def __init__(self, tap):
        self.tap = tap
        self._tickers = None

    def get_tickers(self):
        if self._tickers is None:
            logging.info("Have not fetched tickers yet. Retrieving from tap cache...")
            self._tickers = self.tap.get_cached_tickers()
        return self._tickers
