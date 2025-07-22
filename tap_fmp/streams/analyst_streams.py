from tap_fmp.client import SymbolPartitionStream
from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_fmp.helpers import generate_surrogate_key


class AnalystEstimatesAnnualStream(SymbolPartitionStream):
    """Stream for analyst estimates."""

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


class RatingSnapshotStream(SymbolPartitionStream):
    name = "rating_snapshot"
    schema = th.PropertiesList(
        th.Property("symbol", th.StringType, required=True),
        th.Property("rating", th.StringType),
        th.Property("overall_score", th.NumberType),
        th.Property("discounted_cash_flow_score", th.NumberType),
        th.Property("return_on_equity_score", th.NumberType),
        th.Property("return_on_assets_score", th.NumberType),
        th.Property("debt_to_equity_score", th.NumberType),
        th.Property("price_to_earnings_score", th.NumberType),
        th.Property("price_to_book_score", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/ratings-snapshot"


class HistoricalRatingsStream(SymbolPartitionStream):
    """Stream for historical ratings."""

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
        return f"{self.url_base}/stable/ratings-historical"


class PriceTargetSummaryStream(SymbolPartitionStream):
    name = "price_target_summary"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("last_month_count", th.NumberType),
        th.Property("last_month_avg_price_target", th.NumberType),
        th.Property("last_quarter_count", th.NumberType),
        th.Property("last_quarter_avg_price_target", th.NumberType),
        th.Property("last_year_count", th.NumberType),
        th.Property("last_year_avg_price_target", th.NumberType),
        th.Property("all_time_count", th.NumberType),
        th.Property("all_time_avg_price_target", th.NumberType),
        th.Property("publishers", th.StringType),
    ).to_dict()

    def get_url(self, context: Context) -> str:
        return f"{self.url_base}/stable/price-target-summary"

    def post_process(self, row: dict, context: Context | None = None) -> dict:
        row["publishers"] = str(row["publishers"])
        return super().post_process(row, context)


class PriceTargetConsensusStream(SymbolPartitionStream):
    """Stream for price target consensus."""

    name = "price_target_consensus"
    primary_keys = ["surrogate_key"]

    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("target_high", th.NumberType),
        th.Property("target_low", th.NumberType),
        th.Property("target_consensus", th.NumberType),
        th.Property("target_median", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context) -> str:
        return f"{self.url_base}/stable/price-target-consensus"


class PriceTargetNewsStream(SymbolPartitionStream):
    name = "price_target_news"
    primary_keys = ["surrogate_key"]
    _paginate = True
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("published_date", th.DateTimeType),
        th.Property("news_url", th.StringType),
        th.Property("news_title", th.StringType),
        th.Property("analyst_name", th.StringType),
        th.Property("price_target", th.NumberType),
        th.Property("adj_price_target", th.NumberType),
        th.Property("price_when_posted", th.NumberType),
        th.Property("news_publisher", th.StringType),
        th.Property("news_base_url", th.StringType),
        th.Property("analyst_company", th.StringType),
    ).to_dict()

    def get_url(self, context: Context) -> str:
        return f"{self.url_base}/stable/price-target-news"

    def post_process(self, row: dict, context: Context | None = None) -> dict:
        row["news_url"] = row.pop("news_u_r_l")
        row["news_base_url"] = row.pop("news_base_u_r_l")
        return super().post_process(row, context)


class PriceTargetLatestNewsStream(PriceTargetNewsStream):
    """Stream for price target latest news."""

    name = "price_target_latest_news"

    def get_url(self, context: Context) -> str:
        return f"{self.url_base}/stable/price-target-latest-news"


class StockGradesStream(SymbolPartitionStream):
    """Stream for stock grades."""

    name = "stock_grades"
    primary_keys = ["surrogate_key"]

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("date", th.DateType),
        th.Property("grading_company", th.StringType),
        th.Property("previous_grade", th.StringType),
        th.Property("new_grade", th.StringType),
        th.Property("action", th.StringType),
    ).to_dict()

    _add_surrogate_key = True

    def get_url(self, context: Context) -> str:
        return f"{self.url_base}/stable/grades"


class HistoricalStockGradesStream(SymbolPartitionStream):
    """Stream for historical stock grades."""

    name = "historical_stock_grades"
    primary_keys = ["surrogate_key"]

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("date", th.DateType),
        th.Property("analyst_ratings_strong_buy", th.NumberType),
        th.Property("analyst_ratings_buy", th.NumberType),
        th.Property("analyst_ratings_hold", th.NumberType),
        th.Property("analyst_ratings_sell", th.NumberType),
        th.Property("analyst_ratings_strong_sell", th.NumberType),
    ).to_dict()

    _add_surrogate_key = True

    def get_url(self, context: Context) -> str:
        return f"{self.url_base}/stable/grades-historical"


class StockGradesConsensusStream(SymbolPartitionStream):
    """Stream for stock grades consensus."""

    name = "stock_grades_consensus"
    primary_keys = ["surrogate_key"]

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("strong_buy", th.NumberType),
        th.Property("buy", th.NumberType),
        th.Property("hold", th.NumberType),
        th.Property("sell", th.NumberType),
        th.Property("strong_sell", th.NumberType),
        th.Property("consensus", th.StringType),
    ).to_dict()

    _add_surrogate_key = True

    def get_url(self, context: Context) -> str:
        return f"{self.url_base}/stable/grades-consensus"


class StockGradeNewsStream(SymbolPartitionStream):
    """Stream for stock grade news."""

    name = "stock_grades_news"
    primary_keys = ["surrogate_key"]
    _paginate = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("published_date", th.DateTimeType),
        th.Property("news_url", th.StringType),
        th.Property("news_title", th.StringType),
        th.Property("news_base_url", th.StringType),
        th.Property("news_publisher", th.StringType),
        th.Property("new_grade", th.StringType),
        th.Property("previous_grade", th.StringType),
        th.Property("grading_company", th.StringType),
        th.Property("action", th.StringType),
        th.Property("price_when_posted", th.NumberType),
    ).to_dict()

    _add_surrogate_key = True

    def get_url(self, context: Context) -> str:
        return f"{self.url_base}/stable/grades-news"

    def post_process(self, row: dict, context: Context | None = None) -> dict:
        row["news_base_url"] = row.pop("news_base_u_r_l")
        row["news_url"] = row.pop("news_u_r_l")
        return super().post_process(row, context)


class StockGradeLatestNewsStream(StockGradeNewsStream):
    """Stream for stock grade latest news."""

    name = "stock_grades_latest_news"

    def get_url(self, context: Context) -> str:
        return f"{self.url_base}/stable/grades-latest-news"