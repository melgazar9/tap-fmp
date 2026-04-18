from tap_fmp.client import CompanySymbolPartitionTimeSliceStream
from singer_sdk.helpers.types import Context

from tap_fmp.mixins import (
    BaseAdjustedPriceSchemaMixin,
    ChartLightMixin,
    ChartFullMixin,
    Prices1minMixin,
    Prices5minMixin,
    Prices15minMixin,
    Prices30minMixin,
    Prices1HrMixin,
    Prices4HrMixin,
)

# -------------------------
# Historical Daily Prices
# -------------------------


class CompanyChartLightStream(
    ChartLightMixin, CompanySymbolPartitionTimeSliceStream
):
    name = "company_chart_light"


class CompanyChartFullStream(ChartFullMixin, CompanySymbolPartitionTimeSliceStream):
    name = "company_chart_full"


# -------------------------
# Adjusted & Unadjusted
# -------------------------


class UnadjustedPriceMixin(BaseAdjustedPriceSchemaMixin):
    def get_url(self, context: Context):
        return f"{self.url_base}/stable/historical-price-eod/non-split-adjusted"


class DividendAdjustedPriceMixin(BaseAdjustedPriceSchemaMixin):
    def get_url(self, context: Context):
        return f"{self.url_base}/stable/historical-price-eod/dividend-adjusted"


class CompanyUnadjustedPriceStream(
    UnadjustedPriceMixin, CompanySymbolPartitionTimeSliceStream
):
    name = "company_unadjusted_price"


class CompanyDividendAdjustedPriceStream(
    DividendAdjustedPriceMixin, CompanySymbolPartitionTimeSliceStream
):
    name = "company_dividend_adjusted_prices"


# -------------------------
# Interval Prices
# -------------------------


class Company1minStream(Prices1minMixin, CompanySymbolPartitionTimeSliceStream):
    name = "company_prices_1min"


class Company5minStream(Prices5minMixin, CompanySymbolPartitionTimeSliceStream):
    name = "company_prices_5min"


class Company15minStream(Prices15minMixin, CompanySymbolPartitionTimeSliceStream):
    name = "company_prices_15min"


class Company30minStream(Prices30minMixin, CompanySymbolPartitionTimeSliceStream):
    name = "company_prices_30min"


class Company1HrStream(Prices1HrMixin, CompanySymbolPartitionTimeSliceStream):
    name = "company_prices_1h"


class Company4HrStream(Prices4HrMixin, CompanySymbolPartitionTimeSliceStream):
    name = "company_prices_4h"
