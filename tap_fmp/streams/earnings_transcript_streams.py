from tap_fmp.client import (
    FmpSurrogateKeyStream,
    SymbolPartitionStream,
    SymbolYearQuarterPartitionStream,
)
from tap_fmp.mixins import TranscriptSymbolPartitionMixin
from singer_sdk.helpers.types import Context
from singer_sdk import typing as th


class LatestEarningTranscriptsStream(FmpSurrogateKeyStream):
    """Stream for pulling latest earning transcripts."""

    name = "latest_earning_transcripts"
    _paginate = True
    _paginate_key = "page"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("period", th.StringType),
        th.Property("fiscal_year", th.NumberType),
        th.Property("date", th.DateType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/earning-call-transcript-latest"


class EarningsTranscriptStream(TranscriptSymbolPartitionMixin, FmpSurrogateKeyStream, SymbolYearQuarterPartitionStream):
    """Stream for pulling earnings transcripts by symbol, year, and quarter."""

    name = "earnings_transcripts"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("period", th.StringType),
        th.Property("year", th.NumberType, required=True),
        th.Property("quarter", th.NumberType, required=True),
        th.Property("date", th.DateType, required=True),
        th.Property("content", th.StringType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/earning-call-transcript"

    def post_process(self, record: dict, context: Context | None = None) -> dict:
        if context:
            if "symbol" in context and "symbol" not in record:
                record["symbol"] = context["symbol"]
            if "year" in context and "year" not in record:
                record["year"] = context["year"]
            if "quarter" in context and "quarter" not in record:
                record["quarter"] = context["quarter"]
        return super().post_process(record, context)


class TranscriptsDatesBySymbolStream(FmpSurrogateKeyStream, TranscriptSymbolPartitionMixin, SymbolPartitionStream):
    """Stream for pulling transcript dates by symbol."""

    name = "transcripts_dates_by_symbol"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("quarter", th.NumberType),
        th.Property("fiscal_year", th.NumberType),
        th.Property("date", th.DateType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        symbol = context.get("symbol") if context else None
        return f"{self.url_base}/stable/earning-call-transcript-dates?symbol={symbol}"


class AvailableTranscriptSymbolsStream(FmpSurrogateKeyStream):
    """Stream for pulling available transcript symbols."""

    name = "available_transcript_symbols"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("company_name", th.StringType),
        th.Property("no_of_transcripts", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/earnings-transcript-list"
