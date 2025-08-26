from tap_fmp.client import FmpRestStream, SymbolPartitionStream
from singer_sdk.helpers.types import Context
from singer_sdk import typing as th
import typing as t
from datetime import datetime


class LatestEarningTranscriptsStream(FmpRestStream):
    """Stream for pulling latest earning transcripts."""

    name = "latest_earning_transcripts"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True
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


class EarningsTranscriptSymbolYearQuarterPartitionStream(FmpRestStream):
    """Base class for streams that partition by symbol, year, and quarter."""

    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True

    @property
    def partitions(self):
        config = self.config.get(self.name, {})
        query_params = config.get("query_params", {})
        other_params = config.get("other_params", {})

        quarters = (
            [query_params["quarter"]]
            if "quarter" in query_params
            else other_params.get("quarters")
        )

        years = (
            [query_params["year"]]
            if "year" in query_params
            else other_params.get("years")
        )

        if not quarters or quarters == ["*"]:
            quarters = [1, 2, 3, 4]

        if not years or years == ["*"]:
            current_year = datetime.today().year
            years = [str(y) for y in range(2020, current_year + 1)]

        return [
            {"symbol": s["symbol"], "year": y, "quarter": q}
            for s in self._tap.get_cached_company_symbols()
            for y in years
            for q in quarters
        ]

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        if context:
            self.query_params.update(context)
        return super().get_records(context)


class EarningsTranscriptStream(EarningsTranscriptSymbolYearQuarterPartitionStream):
    """Stream for pulling earnings transcripts by symbol, year, and quarter."""

    name = "earnings_transcripts"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("period", th.StringType),
        th.Property("year", th.NumberType),
        th.Property("quarter", th.NumberType),
        th.Property("date", th.DateType),
        th.Property("content", th.StringType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/earning-call-transcript"


class TranscriptsDatesBySymbolStream(SymbolPartitionStream):
    """Stream for pulling transcript dates by symbol."""

    name = "transcripts_dates_by_symbol"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True

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


class AvailableTranscriptSymbolsStream(FmpRestStream):
    """Stream for pulling available transcript symbols."""

    name = "available_transcript_symbols"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("company_name", th.StringType),
        th.Property("no_of_transcripts", th.StringType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/earnings-transcript-list"
