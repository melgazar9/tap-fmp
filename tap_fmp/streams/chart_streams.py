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


class SecuritiesChartLightStream(
    ChartLightMixin, CompanySymbolPartitionTimeSliceStream
):
    name = "securities_chart_light"


class SecuritiesChartFullStream(ChartFullMixin, CompanySymbolPartitionTimeSliceStream):
    name = "securities_chart_full"


# -------------------------
# Adjusted & Unadjusted
# -------------------------


class UnadjustedPriceMixin(BaseAdjustedPriceSchemaMixin):
    def get_url(self, context: Context):
        return f"{self.url_base}/stable/historical-price-eod/non-split-adjusted"


class DividendAdjustedPriceMixin(BaseAdjustedPriceSchemaMixin):
    def get_url(self, context: Context):
        return f"{self.url_base}/stable/historical-price-eod/dividend-adjusted"


class SecuritiesUnadjustedPriceStream(
    UnadjustedPriceMixin, CompanySymbolPartitionTimeSliceStream
):
    name = "securities_unadjusted_price"


class SecuritiesDividendAdjustedPriceStream(
    DividendAdjustedPriceMixin, CompanySymbolPartitionTimeSliceStream
):
    name = "securities_dividend_adjusted_prices"


# -------------------------
# Interval Prices
# -------------------------


class Securities1minStream(Prices1minMixin, CompanySymbolPartitionTimeSliceStream):
    name = "securities_prices_1min"


class Securities5minStream(Prices5minMixin, CompanySymbolPartitionTimeSliceStream):
    name = "securities_prices_5min"


class Securities15minStream(Prices15minMixin, CompanySymbolPartitionTimeSliceStream):
    name = "securities_prices_15min"


class Securities30minStream(Prices30minMixin, CompanySymbolPartitionTimeSliceStream):
    name = "securities_prices_30min"


class Securities1HrStream(Prices1HrMixin, CompanySymbolPartitionTimeSliceStream):
    name = "securities_prices_1h"


class Securities4HrStream(Prices4HrMixin, CompanySymbolPartitionTimeSliceStream):
    name = "securities_prices_4h"
