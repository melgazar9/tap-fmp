"""FMP tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th
import typing as t

from tap_fmp.streams.directory_streams import (
    SymbolsStream,
)

from tap_fmp.streams.analyst_streams import (
    HistoricalRatingsStream,
)


class TapFMP(Tap):
    """FMP tap class."""

    name = "tap-fmp"

    _cached_symbols: t.List[dict] | None = None
    _symbols_stream_instance: SymbolsStream | None = None

    config_jsonschema = th.PropertiesList(
        th.Property(
            "start_date",
            th.DateTimeType,
            description="Start date for data extraction",
        ),
        th.Property(
            "symbols",
            th.ObjectType(
                th.Property(
                    "select_symbols",
                    th.OneOf(th.StringType, th.ArrayType(th.StringType)),
                ),
            ),
            description="Ticker configuration including selection and query params",
            required=True,
        ),
    ).to_dict()

    def get_cached_symbols(self) -> t.List[dict]:
        if self._cached_symbols is None:
            self.logger.info("Fetching and caching symbols...")
            symbols_stream = self.get_symbols_stream()
            self._cached_symbols = list(symbols_stream.get_records(context=None))
            self.logger.info(f"Cached {len(self._cached_symbols)} symbols.")
        return self._cached_symbols

    def get_symbols_stream(self) -> SymbolsStream:
        if self._symbols_stream_instance is None:
            self.logger.info("Creating SymbolsStream instance...")
            self._symbols_stream_instance = SymbolsStream(self)
        return self._symbols_stream_instance

    def discover_streams(self) -> list:
        """Return a list of discovered streams."""
        return [
            SymbolsStream(self),
            HistoricalRatingsStream(self),
        ]


if __name__ == "__main__":
    TapFMP.cli()
