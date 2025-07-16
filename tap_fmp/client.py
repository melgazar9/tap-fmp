"""REST client handling, including FmpRestStream base class."""

from __future__ import annotations
from abc import ABC
from singer_sdk.helpers.types import Context
from typing import Union
from singer_sdk.streams import Stream
from singer_sdk import Tap
from tap_fmp.helpers import SymbolFetcher, fmp_api_retry, clean_json_keys
import typing as t
import logging
import requests


class FmpRestStream(Stream, ABC):
    """FMP stream class with symbol partitioning support."""

    _use_cached_symbols_default = True
    _valid_segments = None

    def __init__(self, tap: Tap) -> None:
        super().__init__(tap)
        self._all_symbols = None

    def _get_stream_config(self) -> dict:
        """Get configuration for this specific stream."""
        return self.config.get(self.name, {})

    @property
    def use_cached_symbols(self) -> bool:
        """Whether to use cached symbols for this stream."""
        stream_config = self._get_stream_config()

        if "use_cached_symbols" in stream_config:
            use_cached_symbols = stream_config["use_cached_symbols"]
            assert isinstance(use_cached_symbols, bool), (
                f"Config for {self.name}.use_cached_symbols must be bool, "
                f"got {type(use_cached_symbols)}"
            )
            return use_cached_symbols

        if hasattr(type(self), "_use_cached_symbols_default"):
            return getattr(type(self), "_use_cached_symbols_default")

        raise AttributeError(
            f"use_cached_symbols is not defined for stream {self.name}"
        )

    @property
    def partitions(self) -> list[dict] | None:
        """Get partitions for symbol-based streams with segment filtering."""
        if not self.use_cached_symbols:
            return None

        symbol_fetcher = SymbolFetcher(self.config)
        symbol_list = self.config.get("symbol_list") or self.config.get(
            "symbols", {}
        ).get("select_symbols")

        if symbol_list and symbol_list not in ("*", ["*"]):
            symbol_records = symbol_fetcher.fetch_specific_symbols(symbol_list)
            self.logger.info(
                f"{self.name}: Using specific symbols from config: {symbol_list}"
            )
        else:
            symbol_records = self._tap.get_cached_symbols()
            self.logger.info(f"{self.name}: Using cached symbols from tap")

        self._all_symbols = {symbol["symbol"]: symbol for symbol in symbol_records}

        filtered_symbols = self._filter_symbols_by_segments(
            symbol_records, allowed_segments=self._valid_segments
        )

        partitions = [{"symbol": symbol["symbol"]} for symbol in filtered_symbols]

        self.logger.info(f"Created {len(partitions)} symbol partitions for {self.name}")
        return partitions

    @property
    def url_base(self) -> str:
        return self.config.get("base_url", "https://financialmodelingprep.com")

    def _filter_symbols_by_segments(
        self, symbols: list[dict], allowed_segments: list[str] | None = None
    ) -> list[dict]:
        """Centralized symbol filtering by segments."""
        if allowed_segments is None:
            self.logger.info(
                f"{self.name}: Processing all {len(symbols)} symbols (no segment filtering)"
            )
            return symbols

        original_count = len(symbols)

        filtered_symbols = [
            symbol for symbol in symbols if symbol.get("segment") in allowed_segments
        ]

        self.logger.info(
            f"{self.name}: Filtered to {len(filtered_symbols)} symbols from {original_count} "
            f"(allowed segments: {allowed_segments})"
        )

        excluded_symbols = [
            f"{s['symbol']} ({s.get('segment', 'unknown')})"
            for s in symbols
            if s not in filtered_symbols
        ]
        if excluded_symbols:
            self.logger.info(f"{self.name}: Excluded symbols: {excluded_symbols}")

        return filtered_symbols

    def _is_valid_symbol_for_stream(self, symbol: str) -> bool:
        """
        Check if symbol is valid for this stream based on segment.
        Uses the actual segment data instead of regex patterns.
        """
        if not self._valid_segments:
            return True  # No segment restrictions

        if not self._all_symbols:
            return True  # No symbol data available, allow through

        symbol_data = self._all_symbols.get(symbol)
        if not symbol_data:
            return False

        return symbol_data.get("segment") in self._valid_segments

    def _get_symbol_from_context(self, context: Context) -> Union[str, None]:
        """Validates and returns symbol from context."""
        context = context or {}
        symbol = context.get("symbol")
        if not symbol:
            self.logger.error("No symbol found in context")
            return None

        if not self._is_valid_symbol_for_stream(symbol):
            self.logger.warning(
                f"Skipping {symbol} - not valid for {self.name} stream based on segment rules"
            )
            return None
        return symbol

    def get_url_params(self, context: Context | None, next_page_token: dict | None) -> dict:
        """Return a dictionary of URL parameters for the stream."""
        query_params = context.get("query_params", {}).copy() if context else {}
        if next_page_token:
            query_params["page"] = next_page_token.get("page")
        return query_params

    def get_next_page_token(self, response: dict, previous_token: dict | None) -> dict | None:
        """Extract next page token from response. Adjust logic for FMP's pagination style."""
        if not response or len(response) == 0:
            return None
        if previous_token is None:
            page = 1
        else:
            page = previous_token.get("page", 1) + 1
        if len(response) < self.page_size:
            return None
        return {"page": page, "limit": self.page_size}

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
        next_page_token = None
        while True:
            params = self.get_url_params(context, next_page_token)
            records = self._fetch_with_retry(self.get_url(context), params)
            if not records:
                break
            for record in records:
                yield record
            next_page_token = self.get_next_page_token(records, next_page_token)
            if not next_page_token:
                break


class CachedSymbolProvider:
    """Provider for cached symbols (matching tap-fmp pattern)."""

    def __init__(self, tap):
        self.tap = tap
        self._symbols = None

    def get_symbols(self):
        if self._symbols is None:
            logging.info("Have not fetched symbols yet. Retrieving from tap cache...")
            self._symbols = self.tap.get_cached_symbols()
        return self._symbols
