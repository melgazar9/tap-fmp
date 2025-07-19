"""FMP tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th

import typing as t
import threading

from tap_fmp.streams.search_streams import (
    StockScreenerStream,
    ExchangeVariantsStream,
)

from tap_fmp.streams.directory_streams import (
    CompanySymbolsStream,
    FinancialStatementSymbolsStream,
    CikListStream,
    SymbolChangesStream,
    ETFSymbolStream,
    ActivelyTradingStream,
    # EarningsTranscriptStream,
    AvailableExchangesStream,
    AvailableSectorsStream,
    AvailableIndustriesStream,
    AvailableCountriesStream,
)

from tap_fmp.streams.analyst_streams import (
    AnalystEstimatesAnnualStream,
    # AnalystEstimatesQuarterlyStream,
    RatingSnapshotStream,
    HistoricalRatingsStream,
    PriceTargetSummaryStream,
    PriceTargetConsensusStream,
    PriceTargetNewsStream,
    PriceTargetLatestNewsStream,
    StockGradesStream,
    HistoricalStockGradesStream,
    StockGradesConsensusStream,
    StockGradeNewsStream,
    StockGradeLatestNewsStream,
)

from tap_fmp.streams.calendar_streams import (
    DividendsCompanyStream,
    DividendsCalendarStream,
    EarningsReportStream,
    IPOsCalendarStream,
    IPOsDisclosureStream,
    IPOsProspectusStream,
    StockSplitDetailsStream,
    StockSplitsCalendarStream,
)


class TapFMP(Tap):
    """FMP tap class."""

    name = "tap-fmp"

    _cached_symbols: t.List[dict] | None = None
    _symbols_stream_instance: CompanySymbolsStream | None = None
    _symbols_lock = threading.Lock()

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
            description="Symbol configuration including selection and query params",
            required=True,
        ),
    ).to_dict()

    def get_cached_symbols(self) -> t.List[dict]:
        """Thread-safe symbol caching for parallel execution."""
        if self._cached_symbols is None:
            # prevent race conditions if running in parallel
            with self._symbols_lock:
                if self._cached_symbols is None:
                    self.logger.info("Fetching and caching symbols...")
                    symbols_stream = self.get_symbols_stream()
                    self._cached_symbols = list(
                        symbols_stream.get_records(context=None)
                    )
                    self.logger.info(f"Cached {len(self._cached_symbols)} symbols.")
        return self._cached_symbols

    def get_symbols_stream(self) -> CompanySymbolsStream:
        if self._symbols_stream_instance is None:
            self.logger.info("Creating SymbolsStream instance...")
            self._symbols_stream_instance = CompanySymbolsStream(self)
        return self._symbols_stream_instance

    def discover_streams(self) -> list:
        """Return a list of discovered streams."""
        return [
            ### Search Streams ###
            StockScreenerStream(self),
            ExchangeVariantsStream(self),
            # Directory Streams
            CompanySymbolsStream(self),
            FinancialStatementSymbolsStream(self),
            CikListStream(self),
            SymbolChangesStream(self),
            ETFSymbolStream(self),
            ActivelyTradingStream(self),
            # EarningsTranscriptStream(self),
            AvailableExchangesStream(self),
            AvailableSectorsStream(self),
            AvailableIndustriesStream(self),
            AvailableCountriesStream(self),

            ### Analyst Streams ###
            AnalystEstimatesAnnualStream(self),
            # AnalystEstimatesQuarterlyStream(self),
            RatingSnapshotStream(self),
            HistoricalRatingsStream(self),
            PriceTargetSummaryStream(self),
            PriceTargetConsensusStream(self),
            # PriceTargetNewsStream(self),
            # PriceTargetLatestNewsStream(self),
            StockGradesStream(self),
            HistoricalStockGradesStream(self),
            StockGradesConsensusStream(self),
            StockGradeNewsStream(self),
            StockGradeLatestNewsStream(self),

            ### Calendar Streams ###
            DividendsCompanyStream(self),
            DividendsCalendarStream(self),
            EarningsReportStream(self),
            IPOsCalendarStream(self),
            IPOsDisclosureStream(self),
            IPOsProspectusStream(self),
            StockSplitDetailsStream(self),
            StockSplitsCalendarStream(self),
        ]

if __name__ == "__main__":
    TapFMP.cli()
