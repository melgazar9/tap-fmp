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
    AnalystEstimatesStream,
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

from tap_fmp.streams.market_performance_streams import (
    MarketSectorPerformanceSnapshotStream,
    IndustryPerformanceSnapshotStream,
    HistoricalSectorPerformanceStream,
    HistoricalIndustryPerformanceStream,
    SectorPeSnapshotStream,
    IndustryPeSnapshotStream,
    HistoricalSectorPeStream,
    HistoricalIndustryPeStream,
    BiggestStockGainersStream,
    BiggestStockLosersStream,
    TopTradedStocksStream,
)

from tap_fmp.streams.commodity_streams import (
    CommoditiesListStream,
    CommoditiesQuoteStream,
    CommoditiesQuoteShortStream,
    AllCommoditiesQuotesStream,
    CommoditiesLightChartStream,
    CommoditiesFullChartStream,
    Commodities1minStream,
    Commodities5minStream,
    Commodities1HrStream,
)

from tap_fmp.streams.fundraisers_streams import (
    LatestCrowdfundingCampaignsStream,
    CrowdfundingByCikStream,
    EquityOfferingUpdatesStream,
    EquityOfferingByCikStream,
)

from tap_fmp.streams.crypto_streams import (
    CryptoListStream,
    FullCryptoQuoteStream,
    CryptoQuoteShortStream,
    AllCryptoQuotesStream,
    HistoricalCryptoLightChartStream,
    HistoricalCryptoFullChartStream,
    Crypto1minStream,
    Crypto5minStream,
    Crypto1HrStream,
)

from tap_fmp.streams.forex_streams import (
    ForexPairsStream,
    ForexQuoteStream,
    ForexQuoteShortStream,
    BatchForexQuotesStream,
    ForexLightChartStream,
    ForexFullChartStream,
    Forex1minStream,
    Forex5minStream,
    Forex1HrStream,
)

from tap_fmp.streams.news_streams import (
    FmpArticlesStream,
    GeneralNewsStream,
    PressReleasesStream,
    StockNewsStream,
    CryptoNewsStream,
    ForexNewsStream,
    SearchPressReleasesStream,
    SearchStockNewsStream,
    SearchCryptoNewsStream,
    SearchForexNewsStream,
)

from tap_fmp.streams.technical_indicators_streams import (
    SimpleMovingAverageStream,
    ExponentialMovingAverageStream,
    WeightedMovingAverageStream,
    DoubleExponentialMovingAverageStream,
    TripleExponentialMovingAverageStream,
    RelativeStrengthIndexStream,
    StandardDeviationStream,
    WilliamsStream,
    AverageDirectionalIndexStream,
)

from tap_fmp.streams.senate_streams import (
    LatestSenateDisclosuresStream,
    LatestHouseDisclosuresStream,
    SenateTradingActivityStream,
    SenateTradesByNameStream,
    HouseTradesStream,
    HouseTradesByNameStream,
)

from tap_fmp.streams.sec_filings_streams import (
    Latest8KFilingsStream,
    LatestSecFilingsStream,
    SecFilingsByFormTypeStream,
    SecFilingsBySymbolStream,
    SecFilingsByCikStream,
    SecFilingsByNameStream,
    SecFilingsCompanySearchBySymbolStream,
    SecFilingsCompanySearchByCikStream,
    SecCompanyFullProfileStream,
    IndustryClassificationListStream,
    IndustryClassificationSearchStream,
    AllIndustryClassificationStream,
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

    _cached_industries: t.List[dict] | None = None
    _industry_stream_instance: AvailableIndustriesStream | None = None
    _industries_lock = threading.Lock()

    _cached_sectors: t.List[dict] | None = None
    _sector_stream_instance: AvailableSectorsStream | None = None
    _sectors_lock = threading.Lock()

    _cached_commodities: t.List[dict] | None = None
    _commodity_stream_instance: CommoditiesListStream | None = None
    _commodities_lock = threading.Lock()

    _cached_crypto_symbols: t.List[dict] | None = None
    _crypto_stream_instance: CryptoListStream | None = None
    _crypto_lock = threading.Lock()

    _cached_forex_pairs: t.List[dict] | None = None
    _forex_stream_instance: ForexPairsStream | None = None
    _forex_lock = threading.Lock()

    def get_cached_forex_pairs(self) -> t.List[dict]:
        """Thread-safe forex caching for parallel execution."""
        if self._cached_forex_pairs is None:
            with self._forex_lock:
                if self._cached_forex_pairs is None:
                    self.logger.info("Fetching and caching forex pairs...")
                    forex_stream = self.get_forex_stream()
                    self._cached_forex_pairs = list(
                        forex_stream.get_records(context=None)
                    )
                    self.logger.info(
                        f"Cached {len(self._cached_forex_pairs)} forex pairs."
                    )
        return self._cached_forex_pairs

    def get_forex_stream(self) -> ForexPairsStream:
        if self._forex_stream_instance is None:
            self.logger.info("Creating ForexCurrencyPairsStream instance...")
            self._forex_stream_instance = ForexPairsStream(self)
        return self._forex_stream_instance

    def get_cached_crypto_symbols(self) -> t.List[dict]:
        """Thread-safe crypto caching for parallel execution."""
        if self._cached_crypto_symbols is None:
            with self._crypto_lock:
                if self._cached_crypto_symbols is None:
                    self.logger.info("Fetching and caching crypto...")
                    crypto_stream = self.get_crypto_stream()
                    self._cached_crypto_symbols = list(
                        crypto_stream.get_records(context=None)
                    )
                    self.logger.info(
                        f"Cached {len(self._cached_crypto_symbols)} crypto."
                    )
        return self._cached_crypto_symbols

    def get_crypto_stream(self) -> CryptoListStream:
        if self._crypto_stream_instance is None:
            self.logger.info("Creating CryptoListStream instance...")
            self._crypto_stream_instance = CryptoListStream(self)
        return self._crypto_stream_instance

    def get_cached_commodities(self) -> t.List[dict]:
        """Thread-safe commodity caching for parallel execution."""
        if self._cached_commodities is None:
            with self._commodities_lock:
                if self._cached_commodities is None:
                    self.logger.info("Fetching and caching commodities...")
                    commodity_stream = self.get_commodity_stream()
                    self._cached_commodities = list(
                        commodity_stream.get_records(context=None)
                    )
                    self.logger.info(
                        f"Cached {len(self._cached_commodities)} commodities."
                    )
        return self._cached_commodities

    def get_commodity_stream(self) -> CommoditiesListStream:
        if self._commodity_stream_instance is None:
            self.logger.info("Creating CommoditiesListStream instance...")
            self._commodity_stream_instance = CommoditiesListStream(self)
        return self._commodity_stream_instance

    def get_cached_sectors(self) -> t.List[dict]:
        """Thread-safe sector caching for parallel execution."""
        if self._cached_sectors is None:
            with self._sectors_lock:
                if self._cached_sectors is None:
                    self.logger.info("Fetching and caching sectors...")
                    sector_stream = self.get_sector_stream()
                    self._cached_sectors = list(sector_stream.get_records(context=None))
                    self.logger.info(f"Cached {len(self._cached_sectors)} sectors.")
        return self._cached_sectors

    def get_sector_stream(self) -> AvailableSectorsStream:
        if self._sector_stream_instance is None:
            self.logger.info("Creating AvailableSectorsStream instance...")
            self._sector_stream_instance = AvailableSectorsStream(self)
        return self._sector_stream_instance

    def get_cached_industries(self) -> t.List[dict]:
        """Thread-safe industry caching for parallel execution."""
        if self._cached_industries is None:
            with self._industries_lock:
                if self._cached_industries is None:
                    self.logger.info("Fetching and caching industries...")
                    industry_stream = self.get_industry_stream()
                    self._cached_industries = list(
                        industry_stream.get_records(context=None)
                    )
                    self.logger.info(
                        f"Cached {len(self._cached_industries)} industries."
                    )
        return self._cached_industries

    def get_industry_stream(self) -> AvailableIndustriesStream:
        if self._industry_stream_instance is None:
            self.logger.info("Creating AvailableIndustriesStream instance...")
            self._industry_stream_instance = AvailableIndustriesStream(self)
        return self._industry_stream_instance

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
        # fmt: off
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

            AnalystEstimatesStream(self),
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


            ### Market Sector Performance Streams ###

            MarketSectorPerformanceSnapshotStream(self),
            IndustryPerformanceSnapshotStream(self),
            HistoricalSectorPerformanceStream(self),
            HistoricalIndustryPerformanceStream(self),
            SectorPeSnapshotStream(self),
            IndustryPeSnapshotStream(self),
            HistoricalSectorPeStream(self),
            HistoricalIndustryPeStream(self),
            BiggestStockGainersStream(self),
            BiggestStockLosersStream(self),
            TopTradedStocksStream(self),


            ### Commodity Streams ###

            CommoditiesListStream(self),
            CommoditiesQuoteStream(self),
            CommoditiesQuoteShortStream(self),
            AllCommoditiesQuotesStream(self),
            CommoditiesLightChartStream(self),
            CommoditiesFullChartStream(self),
            Commodities1minStream(self),
            Commodities5minStream(self),
            Commodities1HrStream(self),


            ### Fundraiser Streams ###

            LatestCrowdfundingCampaignsStream(self),
            CrowdfundingByCikStream(self),
            EquityOfferingUpdatesStream(self),
            EquityOfferingByCikStream(self),


            ### Crypto Streams ###

            CryptoListStream(self),
            FullCryptoQuoteStream(self),
            CryptoQuoteShortStream(self),
            AllCryptoQuotesStream(self),
            HistoricalCryptoLightChartStream(self),
            HistoricalCryptoFullChartStream(self),
            Crypto1minStream(self),
            Crypto5minStream(self),
            Crypto1HrStream(self),


            ### Forex Streams ###

            ForexPairsStream(self),
            ForexQuoteStream(self),
            ForexQuoteShortStream(self),
            BatchForexQuotesStream(self),
            ForexLightChartStream(self),
            ForexFullChartStream(self),
            Forex1minStream(self),
            Forex5minStream(self),
            Forex1HrStream(self),


            ### News Streams ###

            FmpArticlesStream(self),
            GeneralNewsStream(self),
            PressReleasesStream(self),
            StockNewsStream(self),
            CryptoNewsStream(self),
            ForexNewsStream(self),
            SearchPressReleasesStream(self),
            SearchStockNewsStream(self),
            SearchCryptoNewsStream(self),
            SearchForexNewsStream(self),


            ### Technical Indicator Streams ###

            SimpleMovingAverageStream(self),
            ExponentialMovingAverageStream(self),
            WeightedMovingAverageStream(self),
            DoubleExponentialMovingAverageStream(self),
            TripleExponentialMovingAverageStream(self),
            RelativeStrengthIndexStream(self),
            StandardDeviationStream(self),
            WilliamsStream(self),
            AverageDirectionalIndexStream(self),


            ### Senate Streams ###

            LatestSenateDisclosuresStream(self),
            LatestHouseDisclosuresStream(self),
            SenateTradingActivityStream(self),
            SenateTradesByNameStream(self),
            HouseTradesStream(self),
            HouseTradesByNameStream(self),


            ### SEC Filings Streams ###

            Latest8KFilingsStream(self),
            LatestSecFilingsStream(self),
            SecFilingsByFormTypeStream(self),
            SecFilingsBySymbolStream(self),
            SecFilingsByCikStream(self),
            SecFilingsByNameStream(self),
            SecFilingsCompanySearchBySymbolStream(self),
            SecFilingsCompanySearchByCikStream(self),
            SecCompanyFullProfileStream(self),
            IndustryClassificationListStream(self),
            IndustryClassificationSearchStream(self),
            AllIndustryClassificationStream(self),
        ]


if __name__ == "__main__":
    TapFMP.cli()
