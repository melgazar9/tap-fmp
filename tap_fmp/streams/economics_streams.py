import typing as t

from tap_fmp.client import FmpRestStream, TimeSliceStream
from singer_sdk.helpers.types import Context
from singer_sdk import typing as th


class EconomicsStream(TimeSliceStream):
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True


class TreasuryRatesStream(EconomicsStream):
    name = "treasury_rates"
    primary_keys = ["surrogate_key"]

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("date", th.DateType, required=True),
        th.Property("month1", th.NumberType),
        th.Property("month2", th.NumberType),
        th.Property("month3", th.NumberType),
        th.Property("month6", th.NumberType),
        th.Property("year1", th.NumberType),
        th.Property("year2", th.NumberType),
        th.Property("year3", th.NumberType),
        th.Property("year5", th.NumberType),
        th.Property("year7", th.NumberType),
        th.Property("year10", th.NumberType),
        th.Property("year20", th.NumberType),
        th.Property("year30", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/treasury-rates"


class EconomicIndicatorsStream(EconomicsStream):
    name = "economic_indicators"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("name", th.StringType),
        th.Property("date", th.DateType, required=True),
        th.Property("value", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/economic-indicators"

    @property
    def partitions(self):
        cfg_indicator_names = self.config.get(self.name, {}).get("other_params", {}).get("indicator_names")
        if cfg_indicator_names:
            indicator_names = cfg_indicator_names
        else:
            indicator_names = [
                "GDP",
                "realGDP",
                "nominalPotentialGDP",
                "realGDPPerCapita",
                "federalFunds",
                "CPI",
                "inflationRate",
                "inflation",
                "retailSales",
                "consumerSentiment",
                "durableGoods",
                "unemploymentRate",
                "totalNonfarmPayroll",
                "initialClaims",
                "industrialProductionTotalIndex",
                "newPrivatelyOwnedHousingUnitsStartedTotalUnits",
                "totalVehicleSales",
                "retailMoneyFunds",
                "smoothedUSRecessionProbabilities",
                "3MonthOr90DayRatesAndYieldsCertificatesOfDeposit",
                "commercialBankInterestRateOnCreditCardPlansAllAccounts",
                "30YearFixedRateMortgageAverage",
                "15YearFixedRateMortgageAverage",
            ]

        return [
            {"name": indicator_name} for indicator_name in indicator_names
        ]

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        indicator_name = context.get("name") if context else None
        if indicator_name:
            self.query_params["name"] = indicator_name
        yield from super().get_records(context)


class EconomicCalendarStream(EconomicsStream):
    name = "economic_calendar"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("date", th.DateTimeType, required=True),
        th.Property("country", th.StringType),
        th.Property("event", th.StringType),
        th.Property("currency", th.StringType),
        th.Property("previous", th.NumberType),
        th.Property("estimate", th.NumberType),
        th.Property("actual", th.NumberType),
        th.Property("change", th.NumberType),
        th.Property("impact", th.StringType),
        th.Property("change_percentage", th.NumberType),
        th.Property("unit", th.StringType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/economic-calendar"


class MarketRiskPremiumStream(FmpRestStream):
    name = "market_risk_premium"
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("country", th.StringType),
        th.Property("continent", th.StringType),
        th.Property("country_risk_premium", th.NumberType),
        th.Property("total_equity_risk_premium", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/market-risk-premium"
