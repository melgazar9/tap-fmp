from tap_fmp.client import FmpRestStream, SymbolPartitionedStream
from singer_sdk import typing as th
from singer_sdk.helpers.types import Context


class AnalystEstimatesStream(FmpRestStream):
    name = "analyst_estimates"

    def get_url(self, context: Context):
        url = f"{self.url_base}/stable/analyst-estimates"
        return url


class HistoricalRatingsStream(SymbolPartitionedStream):
    """Stream for historical rating data."""

    name = "historical_ratings"
    primary_keys = ["symbol", "date"]

    _symbol_in_query_params = True

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
            f"{self.url_base}/stable/ratings-historical"
        )
