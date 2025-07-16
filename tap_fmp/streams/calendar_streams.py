from tap_fmp.client import SymbolPartitionedStream
from singer_sdk.helpers.types import Context
from singer_sdk import typing as th

class EarningsReportStream(SymbolPartitionedStream):
    name = "earnings_report"
    primary_keys = ["symbol", "company_name"]

    schema = th.PropertiesList(
        th.Property("symbol", th.StringType, required=True),
        th.Property("date", th.DateType),
        th.Property("eps_actual", th.NumberType),
        th.Property("eps_estimated", th.NumberType),
        th.Property("revenue_actual", th.NumberType),
        th.Property("revenue_estimated", th.NumberType),
        th.Property("last_updated", th.DateType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/earnings"