"""ESG (Environmental, Social, and Governance) Streams."""

import typing as t

from tap_fmp.client import SymbolPartitionStream, FmpRestStream
from singer_sdk.helpers.types import Context
from singer_sdk import typing as th
from datetime import datetime


class EsgStream(SymbolPartitionStream):
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True


class EsgDisclosuresStream(EsgStream):
    name = "esg_disclosures"

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
    replication_key = "fiscal_year"
    replication_method = "INCREMENTAL"
    _add_surrogate_key = True

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

    def _get_starting_year(self, context):
        starting_year = self.get_starting_replication_key_value(context)
        if not starting_year:
            starting_year = 2000
        elif isinstance(starting_year, str):
            starting_year = datetime.fromisoformat(starting_year).year
        return starting_year

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        cfg_years = self.config.get(self.name, {}).get("other_params", {}).get("years")
        if cfg_years:
            if isinstance(cfg_years, str):
                start_year = int(cfg_years)
            elif isinstance(cfg_years, list):
                start_year = min([int(y) for y in cfg_years])
            else:
                start_year = int(cfg_years)
        else:
            start_year = 2000
        starting_year = self._get_starting_year(context)
        current_year = datetime.today().year
        years = list(range(start_year, current_year + 1))
        years = [y for y in years if y >= starting_year]
        for year in years:
            self.query_params.update({"year": year})
            yield from super().get_records(context)
