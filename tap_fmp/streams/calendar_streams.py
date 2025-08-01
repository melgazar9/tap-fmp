from tap_fmp.client import SymbolPartitionStream, TimeSliceStream
from singer_sdk.helpers.types import Context
from singer_sdk import typing as th


class CalendarStream(SymbolPartitionStream):
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True


class TimeSliceCalendarStream(TimeSliceStream):
    primary_keys = ["surrogate_key"]
    replication_key = "date"
    is_timestamp_replication_key = True
    is_sorted = False  # cannot assume data is sorted across all pages
    _add_surrogate_key = True


class DividendsCompanyStream(CalendarStream):
    name = "dividends_company"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("date", th.DateType),
        th.Property("record_date", th.StringType),
        th.Property("payment_date", th.StringType),
        th.Property("declaration_date", th.StringType),
        th.Property("adj_dividend", th.NumberType),
        th.Property("dividend", th.NumberType),
        th.Property("yield", th.NumberType),
        th.Property("frequency", th.StringType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/dividends"


class DividendsCalendarStream(TimeSliceCalendarStream):
    name = "dividends_calendar"
    replication_key = "date"
    replication_method = "INCREMENTAL"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("date", th.DateType),
        th.Property("record_date", th.StringType),
        th.Property("payment_date", th.StringType),
        th.Property("declaration_date", th.StringType),
        th.Property("adj_dividend", th.NumberType),
        th.Property("dividend", th.NumberType),
        th.Property("yield", th.NumberType),
        th.Property("frequency", th.StringType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/dividends-calendar"


class EarningsReportStream(CalendarStream):
    name = "earnings_report"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
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


class EarningsCalendarStream(TimeSliceCalendarStream):
    name = "earnings_calendar"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
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


class IPOsCalendarStream(TimeSliceCalendarStream):
    name = "ipos_calendar"
    replication_key = "date"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("date", th.DateType),
        th.Property("daa", th.DateTimeType),
        th.Property("company", th.StringType),
        th.Property("exchange", th.StringType),
        th.Property("actions", th.StringType),
        th.Property("shares", th.NumberType),
        th.Property("price_range", th.StringType),
        th.Property("market_cap", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/ipos-calendar"


class IPOsDisclosureStream(TimeSliceCalendarStream):
    name = "ipos_disclosure"
    replication_key = "filing_date"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("filing_date", th.DateType),
        th.Property("accepted_date", th.DateType),
        th.Property("effectiveness_date", th.DateType),
        th.Property("cik", th.StringType),
        th.Property("form", th.StringType),
        th.Property("url", th.StringType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/ipos-disclosure"


class IPOsProspectusStream(TimeSliceCalendarStream):
    name = "ipos_prospectus"
    replication_key = "filing_date"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
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
        th.Property("url", th.StringType),
    ).to_dict()

    @staticmethod
    def _normalize_ipo_date(raw_date: str) -> str:
        assert isinstance(raw_date, str)
        if raw_date.isdigit() and len(raw_date) == 4:
            return f"{raw_date}-01-01"
        else:
            return raw_date

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/ipos-prospectus"

    def post_process(self, record: dict, context: Context | None = None) -> dict:
        if "ipo_date" in record:
            record["ipo_date"] = self._normalize_ipo_date(record["ipo_date"])
        return super().post_process(record, context)


class StockSplitDetailsStream(CalendarStream):
    name = "stock_split_details"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("date", th.DateType),
        th.Property("numerator", th.NumberType),
        th.Property("denominator", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/splits"


class StockSplitsCalendarStream(TimeSliceCalendarStream):
    name = "stock_splits_calendar"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("date", th.DateType),
        th.Property("numerator", th.IntegerType),
        th.Property("denominator", th.IntegerType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/splits-calendar"
