"""ESG (Environmental, Social, and Governance) Streams. """

import typing as t

from tap_fmp.client import SymbolPartitionStream, FmpRestStream
from singer_sdk.helpers.types import Context
from singer_sdk import typing as th
from datetime import datetime
from tap_fmp.helpers import generate_surrogate_key


class EsgStream(SymbolPartitionStream):
    primary_keys = ["surrogate_key"]

    def post_process(self, row: dict, context: Context | None = None) -> dict:
        row["surrogate_key"] = generate_surrogate_key(row)
        return row


class EsgInvestmentSearchStream(EsgStream):
    name = "esg_investment_search"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("date", th.DateType),
        th.Property("accepted_date", th.DateType),
        th.Property("cik", th.StringType),
        th.Property("company_name", th.StringType),
        th.Property("form_type", th.StringType),
        th.Property("environmental_score", th.NumberType),
        th.Property("social_score", th.NumberType),
        th.Property("governance_score", th.NumberType),
        th.Property("esg_score", th.NumberType),
        th.Property("url", th.StringType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/esg-disclosures"

class EsgRatingsStream(EsgStream):
    name = "esg_ratings"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("cik", th.StringType),
        th.Property("company_name", th.StringType),
        th.Property("industry", th.StringType),
        th.Property("fiscal_year", th.IntegerType),
        th.Property("esg_risk_rating", th.StringType),
        th.Property("industry_rank", th.StringType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/esg-ratings"

class EsgBenchmarkStream(FmpRestStream):
    name = "esg_benchmark"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("fiscal_year", th.IntegerType, required=True),
        th.Property("sector", th.StringType),
        th.Property("environmental_score", th.NumberType),
        th.Property("social_score", th.NumberType),
        th.Property("governance_score", th.NumberType),
        th.Property("esg_score", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/esg-benchmark"

    @property
    def partitions(self):
        years = [i + 1 for i in range(2000, datetime.today().year)]
        return [{"year": str(year)} for year in years]

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        self.query_params.update(context.get("year"))
        yield from super().get_records(context)