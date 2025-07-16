"""Stream type classes for tap-fmp."""

from __future__ import annotations

from singer_sdk.helpers.types import Context
from tap_fmp.client import FmpRestStream

### Skipping the below endpoints
# Stock Symbol Search
# Company Name Search
# CIK
# CUSIP
# ISIN


class StockScreenerStream(FmpRestStream):
    _use_cached_symbols_default = False

    def get_url(self, context: Context):
        return f"{self.url_base()}/stable/company-screener"


class ExchangeVariantsStream(FmpRestStream):
    _use_cached_symbols_default = False

    def get_url(self, context: Context):
        return f"{self.url_base()}/stable/search-exchange-variants"
