from tap_fmp.client import FmpRestStream, SymbolPartitionedStream
from singer_sdk import typing as th
from singer_sdk.helpers.types import Context


class AnalystEstimatesAnnualStream(SymbolPartitionedStream):
    name = "analyst_estimates_annual"
    _paginate = True

    schema = th.PropertiesList(
        th.Property("symbol", th.StringType, required=True),
        th.Property("date", th.DateType, required=True),
        th.Property("revenue_low", th.NumberType),
        th.Property("revenue_high", th.NumberType),
        th.Property("revenue_avg", th.NumberType),
        th.Property("ebitda_low", th.NumberType),
        th.Property("ebitda_high", th.NumberType),
        th.Property("ebitda_avg", th.NumberType),
        th.Property("ebit_low", th.NumberType),
        th.Property("ebit_high", th.NumberType),
        th.Property("ebit_avg", th.NumberType),
        th.Property("net_income_low", th.NumberType),
        th.Property("net_income_high", th.NumberType),
        th.Property("net_income_avg", th.NumberType),
        th.Property("sga_expense_low", th.NumberType),
        th.Property("sga_expense_high", th.NumberType),
        th.Property("sga_expense_avg", th.NumberType),
        th.Property("eps_avg", th.NumberType),
        th.Property("eps_high", th.NumberType),
        th.Property("eps_low", th.NumberType),
        th.Property("num_analysts_revenue", th.IntegerType),
        th.Property("num_analysts_eps", th.IntegerType),
    ).to_dict()

    def get_url(self, context: Context):
        url = f"{self.url_base}/stable/analyst-estimates"
        return url

class AnalystEstimatesQuarterlyStream(AnalystEstimatesAnnualStream):
    name = "analyst_estimates_quarterly"
    # Only need to change query_params in meltano.yml --> set period to quarterly for this stream.

class HistoricalRatingsStream(SymbolPartitionedStream):
    """Stream for historical rating data."""

    name = "historical_ratings"
    primary_keys = ["symbol", "date"]

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
