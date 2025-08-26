"""Quote stream types classes for tap-fmp."""

from __future__ import annotations

import typing as t
from singer_sdk import typing as th
from singer_sdk.helpers.types import Context
from tap_fmp.client import SymbolPartitionStream, FmpSurrogateKeyStream


class QuoteSymbolPartitionStream(SymbolPartitionStream, FmpSurrogateKeyStream):
    """Base class for quote streams with surrogate key support."""


class BatchSymbolStream(FmpSurrogateKeyStream):
    """Base class for batch symbol streams with automatic chunking."""

    def get_symbol_chunks(self, chunk_size: int = 100):
        """Get symbols in chunks for batch processing."""
        symbols = self.config.get("symbols", {}).get("select_symbols", [])
        if isinstance(symbols, str):
            symbols = [symbols]

        for i in range(0, len(symbols), chunk_size):
            yield symbols[i : i + chunk_size]

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Override to handle batching by chunks of symbols."""
        for symbol_chunk in self.get_symbol_chunks():
            self.query_params["symbols"] = ",".join(symbol_chunk)
            yield from super().get_records(context)


class StockQuoteStream(QuoteSymbolPartitionStream):
    """Stream for real-time stock quotes."""

    name = "stock_quote"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("name", th.StringType),
        th.Property("price", th.NumberType),
        th.Property("changes_percentage", th.NumberType),
        th.Property("change_percentage", th.NumberType),
        th.Property("change", th.NumberType),
        th.Property("day_low", th.NumberType),
        th.Property("day_high", th.NumberType),
        th.Property("year_high", th.NumberType),
        th.Property("year_low", th.NumberType),
        th.Property("market_cap", th.NumberType),
        th.Property("price_avg50", th.NumberType),
        th.Property("price_avg200", th.NumberType),
        th.Property("exchange", th.StringType),
        th.Property("volume", th.NumberType),
        th.Property("avg_volume", th.NumberType),
        th.Property("open", th.NumberType),
        th.Property("previous_close", th.NumberType),
        th.Property("eps", th.NumberType),
        th.Property("pe", th.NumberType),
        th.Property("earnings_announcement", th.DateTimeType),
        th.Property("shares_outstanding", th.NumberType),
        th.Property("timestamp", th.IntegerType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/quote"


class StockQuoteShortStream(QuoteSymbolPartitionStream):
    """Stream for short format stock quotes."""

    name = "stock_quote_short"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("price", th.NumberType),
        th.Property("change", th.NumberType),
        th.Property("volume", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/quote-short"


class AftermarketTradeStream(QuoteSymbolPartitionStream):
    """Stream for aftermarket trade data."""

    name = "aftermarket_trade"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("price", th.NumberType),
        th.Property("size", th.NumberType),
        th.Property("trade_size", th.NumberType),
        th.Property("timestamp", th.IntegerType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/aftermarket-trade"


class AftermarketQuoteStream(QuoteSymbolPartitionStream):
    """Stream for aftermarket quotes."""

    name = "aftermarket_quote"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("ask_price", th.NumberType),
        th.Property("ask_size", th.NumberType),
        th.Property("bid_price", th.NumberType),
        th.Property("bid_size", th.NumberType),
        th.Property("volume", th.NumberType),
        th.Property("timestamp", th.IntegerType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/aftermarket-quote"


class StockPriceChangeStream(QuoteSymbolPartitionStream):
    """Stream for stock price changes."""

    name = "stock_price_change"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("name", th.StringType),
        th.Property("change", th.NumberType),
        th.Property("price", th.NumberType),
        th.Property("changes_percentage", th.NumberType),
        th.Property("1_d", th.NumberType),
        th.Property("5_d", th.NumberType),
        th.Property("1_m", th.NumberType),
        th.Property("3_m", th.NumberType),
        th.Property("6_m", th.NumberType),
        th.Property("ytd", th.NumberType),
        th.Property("1_y", th.NumberType),
        th.Property("3_y", th.NumberType),
        th.Property("5_y", th.NumberType),
        th.Property("10_y", th.NumberType),
        th.Property("max", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/stock-price-change"


class StockBatchStream(BatchSymbolStream):
    """Stream for batch stock data with automatic chunking."""

    name = "stock_batch"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("name", th.StringType),
        th.Property("price", th.NumberType),
        th.Property("changes_percentage", th.NumberType),
        th.Property("change_percentage", th.NumberType),
        th.Property("change", th.NumberType),
        th.Property("day_low", th.NumberType),
        th.Property("day_high", th.NumberType),
        th.Property("year_high", th.NumberType),
        th.Property("year_low", th.NumberType),
        th.Property("market_cap", th.NumberType),
        th.Property("price_avg50", th.NumberType),
        th.Property("price_avg200", th.NumberType),
        th.Property("exchange", th.StringType),
        th.Property("volume", th.NumberType),
        th.Property("avg_volume", th.NumberType),
        th.Property("open", th.NumberType),
        th.Property("previous_close", th.NumberType),
        th.Property("eps", th.NumberType),
        th.Property("pe", th.NumberType),
        th.Property("earnings_announcement", th.DateTimeType),
        th.Property("shares_outstanding", th.NumberType),
        th.Property("timestamp", th.IntegerType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/batch-quote"


class StockBatchQuoteStream(StockBatchStream):
    """Stream for batch stock quotes (same as StockBatchStream)."""

    name = "stock_batch_quote"


class StockBatchQuoteShortStream(BatchSymbolStream):
    """Stream for batch short stock quotes with automatic chunking."""

    name = "stock_batch_quote_short"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("price", th.NumberType),
        th.Property("change", th.NumberType),
        th.Property("volume", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/batch-quote-short"


class BatchAftermarketTradeStream(BatchSymbolStream):
    """Stream for batch aftermarket trades with automatic chunking."""

    name = "batch_aftermarket_trade"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("price", th.NumberType),
        th.Property("size", th.NumberType),
        th.Property("trade_size", th.NumberType),
        th.Property("timestamp", th.IntegerType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/batch-aftermarket-trade"


class BatchAftermarketQuoteStream(BatchSymbolStream):
    """Stream for batch aftermarket quotes with automatic chunking."""

    name = "batch_aftermarket_quote"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("ask_price", th.NumberType),
        th.Property("ask_size", th.NumberType),
        th.Property("bid_price", th.NumberType),
        th.Property("bid_size", th.NumberType),
        th.Property("volume", th.NumberType),
        th.Property("timestamp", th.IntegerType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/batch-aftermarket-quote"


class MutualFundPriceQuotesStream(QuoteSymbolPartitionStream):
    """Stream for mutual fund price quotes."""

    name = "mutual_fund_price_quotes"
    _use_cached_symbols_default = False

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("name", th.StringType),
        th.Property("price", th.NumberType),
        th.Property("changes_percentage", th.NumberType),
        th.Property("change_percentage", th.NumberType),
        th.Property("change", th.NumberType),
        th.Property("day_low", th.NumberType),
        th.Property("day_high", th.NumberType),
        th.Property("year_high", th.NumberType),
        th.Property("year_low", th.NumberType),
        th.Property("market_cap", th.NumberType),
        th.Property("price_avg50", th.NumberType),
        th.Property("price_avg200", th.NumberType),
        th.Property("exchange", th.StringType),
        th.Property("volume", th.NumberType),
        th.Property("open", th.NumberType),
        th.Property("previous_close", th.NumberType),
        th.Property("timestamp", th.IntegerType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/quote"


class ETFPriceQuotesStream(QuoteSymbolPartitionStream):
    """Stream for ETF price quotes using ETF symbol list."""

    name = "etf_price_quotes"
    _use_cached_symbols_default = False

    def get_symbols(self, context: Context | None = None) -> t.List[dict]:
        """Use cached ETF symbols instead of regular stock symbols."""
        return self.tap.get_cached_etf_symbols()

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("name", th.StringType),
        th.Property("price", th.NumberType),
        th.Property("changes_percentage", th.NumberType),
        th.Property("change_percentage", th.NumberType),
        th.Property("change", th.NumberType),
        th.Property("day_low", th.NumberType),
        th.Property("day_high", th.NumberType),
        th.Property("year_high", th.NumberType),
        th.Property("year_low", th.NumberType),
        th.Property("market_cap", th.NumberType),
        th.Property("price_avg50", th.NumberType),
        th.Property("price_avg200", th.NumberType),
        th.Property("exchange", th.StringType),
        th.Property("volume", th.NumberType),
        th.Property("open", th.NumberType),
        th.Property("previous_close", th.NumberType),
        th.Property("timestamp", th.IntegerType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/quote"
