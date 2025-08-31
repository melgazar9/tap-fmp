from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_fmp.client import (
    FmpRestStream,
    FmpSurrogateKeyStream,
)

from tap_fmp.mixins import (
    SelectableStreamMixin,
    CompanyConfigMixin,
    FinancialStatementConfigMixin,
    CikConfigMixin,
    ExchangeConfigMixin,
)


class CompanySymbolsStream(CompanyConfigMixin, FmpSurrogateKeyStream):
    """Stream for pulling all company symbols using cached data."""

    name = "company_symbols"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("company_name", th.StringType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/stock-list"


class FinancialStatementSymbolsStream(FinancialStatementConfigMixin, FmpRestStream):
    name = "financial_statement_symbols"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("company_name", th.StringType),
        th.Property("trading_currency", th.StringType),
        th.Property("reporting_currency", th.StringType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/financial-statement-symbol-list"


class CikListStream(CikConfigMixin, FmpSurrogateKeyStream):
    name = "cik_list"
    _paginate = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("cik", th.StringType, required=True),
        th.Property("company_name", th.StringType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/cik-list"


class SymbolChangesStream(FmpSurrogateKeyStream):
    name = "symbol_changes"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("date", th.DateType, required=True),
        th.Property("company_name", th.StringType),
        th.Property("old_symbol", th.StringType),
        th.Property("new_symbol", th.StringType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/symbol-change"


class ActivelyTradingStream(FmpSurrogateKeyStream):
    name = "actively_trading"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("name", th.StringType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/actively-trading-list"


class AvailableExchangesStream(ExchangeConfigMixin, FmpSurrogateKeyStream):
    """Stream for pulling all exchanges using cached data."""

    name = "available_exchanges"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("exchange", th.StringType, required=True),
        th.Property("name", th.StringType, required=True),
        th.Property("opening_hour", th.StringType),
        th.Property("closing_hour", th.StringType),
        th.Property("timezone", th.StringType),
        th.Property("is_market_open", th.BooleanType),
        th.Property("closing_additional", th.StringType),
        th.Property("opening_additional", th.StringType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/all-exchange-market-hours"


class AvailableSectorsStream(SelectableStreamMixin, FmpRestStream):
    name = "available_sectors"
    primary_keys = ["sector"]

    schema = th.PropertiesList(
        th.Property("sector", th.StringType, required=True),
    ).to_dict()

    @property
    def selection_config_key(self) -> str:
        return "sectors"

    @property
    def selection_field_key(self) -> str:
        return "select_sectors"

    @property
    def item_name_singular(self) -> str:
        return "sector"

    @property
    def item_name_plural(self) -> str:
        return "sectors"

    def create_record_from_item(self, item: str) -> dict:
        """Create a sector record dict from a sector string."""
        return {"sector": item}

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/available-sectors"


class AvailableIndustriesStream(SelectableStreamMixin, FmpRestStream):
    name = "available_industries"
    primary_keys = ["industry"]

    schema = th.PropertiesList(
        th.Property("industry", th.StringType, required=True),
    ).to_dict()

    @property
    def selection_config_key(self) -> str:
        return "industries"

    @property
    def selection_field_key(self) -> str:
        return "select_industries"

    @property
    def item_name_singular(self) -> str:
        return "industry"

    @property
    def item_name_plural(self) -> str:
        return "industries"

    def create_record_from_item(self, item: str) -> dict:
        """Create an industry record dict from an industry string."""
        return {"industry": item}

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/available-industries"


class AvailableCountriesStream(FmpRestStream):
    name = "available_countries"
    primary_keys = ["country"]

    schema = th.PropertiesList(
        th.Property("country", th.StringType, required=True),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/available-countries"
