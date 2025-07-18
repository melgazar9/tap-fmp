from tap_fmp.client import FmpRestStream, SymbolPartitionedStream, SymbolPartitionTimeSliceStream
from singer_sdk.helpers.types import Context
from singer_sdk import typing as th

class DividendsCompanyStream(SymbolPartitionedStream):
    name = "dividends_company"
    primary_keys = ["symbol", "date"]

    schema = th.PropertiesList(
        th.Property("symbol", th.StringType, required=True),
        th.Property("date", th.DateType),
        th.Property("record_date", th.DateType),
        th.Property("payment_date", th.DateType),
        th.Property("declaration_date", th.DateType),
        th.Property("adj_dividend", th.NumberType),
        th.Property("dividend", th.NumberType),
        th.Property("yield", th.NumberType),
        th.Property("frequency", th.StringType)
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/dividends"


class DividendsCalendarStream(SymbolPartitionTimeSliceStream):
    name = "dividends_calendar"
    primary_keys = ["symbol", "date"]

    schema = th.PropertiesList(
        th.Property("symbol", th.StringType, required=True),
        th.Property("date", th.DateType),
        th.Property("record_date", th.StringType),
        th.Property("payment_date", th.StringType),
        th.Property("declaration_date", th.StringType),
        th.Property("adj_dividend", th.NumberType),
        th.Property("dividend", th.NumberType),
        th.Property("yield", th.NumberType),
        th.Property("frequency", th.StringType)
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/dividends-calendar"

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

class EarningsCalendarStream(FmpRestStream):
    name = "earnings_calendar"
    primary_keys = ["symbol", "date", "last_updated"]

    schema = th.PropertiesList(
        th.Property("symbol", th.StringType, required=True),
        th.Property("date", th.DateType, required=True),
        th.Property("eps_actual", th.NumberType),
        th.Property("eps_estimated", th.NumberType),
        th.Property("revenue_actual", th.NumberType),
        th.Property("revenue_estimated", th.NumberType),
        th.Property("last_updated", th.DateType, required=True),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/earnings-calendar"

class IPOsCalendarStream(SymbolPartitionedStream):
    name = "ipos_calendar"
    primary_keys = ["symbol", "date"]

    schema = th.PropertiesList(
        th.Property("symbol", th.StringType, required=True),
        th.Property("date", th.DateType),
        th.Property("daa", th.DateTimeType),
        th.Property("company", th.StringType),
        th.Property("exchange", th.StringType),
        th.Property("actions", th.StringType),
        th.Property("shares", th.NumberType),
        th.Property("price_range", th.NumberType),
        th.Property("market_cap", th.NumberType)
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/ipos-calendar"

class IPOsDisclosureStream(SymbolPartitionedStream):
    name = "ipos_disclosure"
    primary_keys = ["symbol", "filing_date"]

    schema = th.PropertiesList(
        th.Property("symbol", th.StringType, required=True),
        th.Property("filing_date", th.DateType),
        th.Property("accepted_date", th.DateType),
        th.Property("effectiveness_date", th.DateType),
        th.Property("cik", th.StringType),
        th.Property("form", th.StringType),
        th.Property("url", th.StringType)
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/ipos-disclosure"

class IPOsProspectusStream(SymbolPartitionedStream):
    name = "ipos_prospectus"
    primary_keys = ["symbol", "filing_date"]

    schema = th.PropertiesList(
        th.Property("symbol", th.StringType, required=True),
        th.Property("accepted_date", th.DateType),
        th.Property("filing_date", th.DateType),
        th.Property("ipo_date", th.DateType),
        th.Property("cik", th.StringType),
        th.Property("price_public_per_share", th.NumberType),
        th.Property("price_public_total", th.NumberType),
        th.Property("discounts_and_commissions_per_share", th.NumberType),
        th.Property("discounts_and_commissions_total", th.NumberType),
        th.Property("proceeds_before_expenses_per_share", th.NumberType),
        th.Property("proceeds_before_expenses_total", th.NumberType),
        th.Property("form", th.StringType),
        th.Property("url", th.StringType)
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/ipos-prospectus"

class StockSplitDetailsStream(SymbolPartitionedStream):
    name = "stock_split_details"
    primary_keys = ["symbol", "date"]

    schema = th.PropertiesList(
        th.Property("symbol", th.StringType, required=True),
        th.Property("date", th.DateType),
        th.Property("numerator", th.IntegerType),
        th.Property("denominator", th.IntegerType)
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/splits"

class StockSplitsCalendarStream(SymbolPartitionedStream):
    name = "stock_splits_calendar"
    primary_keys = ["symbol", "date"]

    schema = th.PropertiesList(
        th.Property("symbol", th.StringType, required=True),
        th.Property("date", th.DateType),
        th.Property("numerator", th.IntegerType),
        th.Property("denominator", th.IntegerType)
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/splits-calendar"


