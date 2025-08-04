import typing as t

from tap_fmp.client import (
    FmpRestStream,
    TimeSliceStream,
)

from singer_sdk.helpers.types import Context
from singer_sdk import typing as th
from typing import Iterable


class ExchangeMarketHoursStream(FmpRestStream):
    name = "exchange_market_hours"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("exchange", th.StringType, required=True),
        th.Property("name", th.StringType),
        th.Property("opening_hour", th.StringType),
        th.Property("closing_hour", th.StringType),
        th.Property("timezone", th.StringType),
        th.Property("is_market_open", th.BooleanType),
        th.Property("closing_additional", th.StringType),
        th.Property("opening_additional", th.StringType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/exchange-market-hours"

    @property
    def partitions(self):
        return [
            {"exchange": exchange_json.get("exchange")}
            for exchange_json in self._tap.get_cached_exchanges()
        ]

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        self.query_params.update({"exchange": context.get("exchange")})
        yield from super().get_records(context)


class HolidaysByExchangeStream(TimeSliceStream):
    name = "holidays_by_exchange"

    _use_cached_symbols_default = False
    _symbol_in_query_params = False

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("exchange", th.StringType, required=True),
        th.Property("date", th.StringType, required=True),
        th.Property("name", th.StringType),
        th.Property("is_closed", th.BooleanType),
        th.Property("adj_open_time", th.StringType),
        th.Property("adj_close_time", th.StringType),
    ).to_dict()

    @property
    def partitions(self):
        return [
            {"exchange": exchange_json.get("exchange")}
            for exchange_json in self._tap.get_cached_exchanges()
        ]

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/holidays-by-exchange"

    def get_records(self, context: Context | None) -> Iterable[dict]:
        self.query_params.update(context)
        yield from super().get_records(context)


class AllExchangeMarketHoursStream(FmpRestStream):
    name = "all_exchange_market_hours"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("exchange", th.StringType, required=True),
        th.Property("name", th.StringType),
        th.Property("opening_hour", th.StringType),
        th.Property("closing_hour", th.StringType),
        th.Property("timezone", th.StringType),
        th.Property("is_market_open", th.BooleanType),
        th.Property("closing_additional", th.StringType),
        th.Property("opening_additional", th.StringType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/all-exchange-market-hours"
