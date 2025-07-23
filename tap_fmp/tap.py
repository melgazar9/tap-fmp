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
    # PriceTargetNewsStream,
    # PriceTargetLatestNewsStream,
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
    EarningsCalendarStream,
    IPOsCalendarStream,
    IPOsDisclosureStream,
    IPOsProspectusStream,
    StockSplitDetailsStream,
    StockSplitsCalendarStream,
)

from tap_fmp.streams.company_streams import (
    CompanyProfileBySymbolStream,
    CikProfileStream,
    CompanyNotesStream,
    StockPeerComparisonStream,
    DelistedCompaniesStream,
    CompanyEmployeeCountStream,
    CompanyHistoricalEmployeeCountStream,
    CompanyMarketCapStream,
    CompanyBatchMarketCapStream,
    HistoricalMarketCapStream,
    CompanyShareAndLiquidityFloatStream,
    AllSharesFloatStream,
    LatestMergersAndAcquisitionsStream,
    SearchMergersAndAcquisitionsStream,
    CompanyExecutiveStream,
    ExecutiveCompensationStream,
    ExecutiveCompensationBenchmarkStream,
)

from tap_fmp.streams.discounted_cash_flow_streams import (
    DcfValuationStream,
    LeveredDcfStream,
    CustomDcfStream,
    CustomDcfLeveredStream,
)

from tap_fmp.streams.market_hours_streams import (
    ExchangeMarketHoursStream,
    HolidaysByExchangeStream,
    AllExchangeMarketHoursStream,
)

from tap_fmp.streams.statements_streams import (
    IncomeStatementStream,
    BalanceSheetStream,
    CashFlowStream,
    KeyMetricsStream,
    FinancialRatiosStream,
    KeyMetricsTtmStream,
    FinancialRatiosTtmStream,
    FinancialScoresStream,
    OwnerEarningsStream,
    EnterpriseValuesStream,
    IncomeStatementGrowthStream,
    BalanceSheetGrowthStream,
    CashFlowGrowthStream,
    FinancialStatementReportDatesStream,
    FinancialReportsForm10kJsonStream,
    RevenueProductSegmentationStream,
    RevenueGeographicSegmentationStream,
    AsReportedIncomeStatementsStream,
    AsReportedBalanceStatementsStream,
    AsReportedCashflowStatementsStream,
    AsReportedFinancialStatementsStream,
    BalanceSheetTtmStream,
)

# from tap_fmp.streams.form_13f_streams import (
#     InstitutionalOwnershipFilingsStream,
#     FilingsExtractStream,
#     HolderPerformanceSummaryStream,
#     HolderIndustryBreakdownStream,
#     PositionsSummaryStream,
#     IndustryPerformanceSummaryStream,
# )

from tap_fmp.streams.indexes_streams import (
    IndexListStream,
    IndexQuoteStream,
    IndexShortQuoteStream,
    AllIndexQuotesStream,
    HistoricalIndexLightChartStream,
    HistoricalIndexFullChartStream,
    Index1MinuteIntervalStream,
    Index5MinuteIntervalStream,
    Index1HourIntervalStream,
    SP500ConstituentStream,
    NasdaqConstituentStream,
    DowJonesConstituentStream,
    HistoricalSP500ConstituentStream,
    HistoricalNasdaqConstituentStream,
    HistoricalDowJonesConstituentStream,
)

from tap_fmp.streams.insider_trades_streams import (
    LatestInsiderTradingStream,
    SearchInsiderTradesStream,
    SearchInsiderTradesByReportingNameStream,
    AllInsiderTransactionTypesStream,
    InsiderTradeStatisticsStream,
    AcquisitionOwnershipStream,
)

class TapFMP(Tap):
    """FMP tap class."""

    name = "tap-fmp"

    _cached_symbols: t.List[dict] | None = None
    _symbols_stream_instance: CompanySymbolsStream | None = None
    _symbols_lock = threading.Lock()

    _cached_ciks: t.List[dict] | None = None
    _cik_stream_instance: CikListStream | None = None
    _ciks_lock = threading.Lock()

    _cached_exchanges: t.List[dict] | None = None
    _exchange_stream_instance: AvailableExchangesStream | None = None
    _exchanges_lock = threading.Lock()

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

    def get_cached_ciks(self) -> t.List[dict]:
        """Thread-safe CIK caching for parallel execution."""
        if self._cached_ciks is None:
            # prevent race conditions if running in parallel
            with self._ciks_lock:
                if self._cached_ciks is None:
                    self.logger.info("Fetching and caching CIKs...")
                    cik_stream = self.get_cik_stream()
                    self._cached_ciks = list(cik_stream.get_records(context=None))
                    self.logger.info(f"Cached {len(self._cached_ciks)} CIKs.")
        return self._cached_ciks

    def get_cik_stream(self) -> CikListStream:
        if self._cik_stream_instance is None:
            self.logger.info("Creating CikListStream instance...")
            self._cik_stream_instance = CikListStream(self)
        return self._cik_stream_instance

    def get_cached_exchanges(self) -> t.List[dict]:
        """Thread-safe exchange caching for parallel execution."""
        if self._cached_exchanges is None:
            # prevent race conditions if running in parallel
            with self._exchanges_lock:
                if self._cached_exchanges is None:
                    self.logger.info("Fetching and caching exchanges...")
                    exchange_stream = self.get_exchange_stream()
                    self._cached_exchanges = list(
                        exchange_stream.get_records(context=None)
                    )
                    self.logger.info(f"Cached {len(self._cached_exchanges)} exchanges.")
        return self._cached_exchanges

    def get_exchange_stream(self) -> AvailableExchangesStream:
        if self._exchange_stream_instance is None:
            self.logger.info("Creating AvailableExchangesStream instance...")
            self._exchange_stream_instance = AvailableExchangesStream(self)
        return self._exchange_stream_instance


    def discover_streams(self) -> list:
        """Return a list of discovered streams."""
        return [
            ### Search Streams ###

            StockScreenerStream(self),
            ExchangeVariantsStream(self),

            ### Directory Streams ###

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
            EarningsCalendarStream(self),
            IPOsCalendarStream(self),
            IPOsDisclosureStream(self),
            IPOsProspectusStream(self),
            StockSplitDetailsStream(self),
            StockSplitsCalendarStream(self),

            ### Company Streams ###
            CompanyProfileBySymbolStream(self),
            CikProfileStream(self),
            CompanyNotesStream(self),
            StockPeerComparisonStream(self),
            DelistedCompaniesStream(self),
            CompanyEmployeeCountStream(self),
            CompanyHistoricalEmployeeCountStream(self),
            CompanyMarketCapStream(self),
            CompanyBatchMarketCapStream(self),
            HistoricalMarketCapStream(self),
            CompanyShareAndLiquidityFloatStream(self),
            AllSharesFloatStream(self),
            LatestMergersAndAcquisitionsStream(self),
            SearchMergersAndAcquisitionsStream(self),
            CompanyExecutiveStream(self),
            ExecutiveCompensationStream(self),
            ExecutiveCompensationBenchmarkStream(self),

            ### DCF Streams ###

            DcfValuationStream(self),
            LeveredDcfStream(self),
            CustomDcfStream(self),
            CustomDcfLeveredStream(self),

            ### Market Hours Streams ###

            ExchangeMarketHoursStream(self),
            HolidaysByExchangeStream(self),
            AllExchangeMarketHoursStream(self),

            ### Statement Streams ###
            IncomeStatementStream(self),
            BalanceSheetStream(self),
            CashFlowStream(self),
            KeyMetricsStream(self),
            FinancialRatiosStream(self),
            KeyMetricsTtmStream(self),
            FinancialRatiosTtmStream(self),
            FinancialScoresStream(self),
            OwnerEarningsStream(self),
            EnterpriseValuesStream(self),
            IncomeStatementGrowthStream(self),
            BalanceSheetGrowthStream(self),
            CashFlowGrowthStream(self),
            FinancialStatementReportDatesStream(self),
            FinancialReportsForm10kJsonStream(self),
            RevenueProductSegmentationStream(self),
            RevenueGeographicSegmentationStream(self),
            AsReportedIncomeStatementsStream(self),
            AsReportedBalanceStatementsStream(self),
            AsReportedCashflowStatementsStream(self),
            AsReportedFinancialStatementsStream(self),
            BalanceSheetTtmStream(self),

            ### Index Streams ###

            IndexListStream(self),
            IndexQuoteStream(self),
            IndexShortQuoteStream(self),
            AllIndexQuotesStream(self),
            HistoricalIndexLightChartStream(self),
            HistoricalIndexFullChartStream(self),
            Index1MinuteIntervalStream(self),
            Index5MinuteIntervalStream(self),
            Index1HourIntervalStream(self),
            SP500ConstituentStream(self),
            NasdaqConstituentStream(self),
            DowJonesConstituentStream(self),
            HistoricalSP500ConstituentStream(self),
            HistoricalNasdaqConstituentStream(self),
            HistoricalDowJonesConstituentStream(self),

            ### Insider Trades Streams ###

            LatestInsiderTradingStream(self),
            SearchInsiderTradesStream(self),
            SearchInsiderTradesByReportingNameStream(self),
            AllInsiderTransactionTypesStream(self),
            InsiderTradeStatisticsStream(self),
            AcquisitionOwnershipStream(self),

        ]


if __name__ == "__main__":
    TapFMP.cli()