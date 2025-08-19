"""SEC Filings stream types classes for tap-fmp."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_fmp.client import (
    FmpRestStream,
    SymbolPartitionStream,
    TimeSliceStream,
    SymbolPartitionTimeSliceStream,
)


class BaseSecFilingTimeSliceStream(TimeSliceStream):
    """Base class for SEC filing streams with common schema and pagination."""

    primary_keys = ["surrogate_key"]
    replication_key = "filing_date"
    _add_surrogate_key = True
    _symbol_in_query_params = False
    _paginate = True
    _max_pages = 100

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("cik", th.StringType),
        th.Property("acceptance_time", th.DateTimeType),
        th.Property("period_ending", th.DateType),
        th.Property("company_name", th.StringType),
        th.Property("form_type", th.StringType),
        th.Property("filing_date", th.DateTimeType),
        th.Property("ticker", th.StringType),
        th.Property("exchange", th.StringType),
        th.Property("filename", th.StringType),
        th.Property("link", th.StringType),
        th.Property("accepted_date", th.StringType),
        th.Property("has_financials", th.BooleanType),
        th.Property("final_link", th.BooleanType),
    ).to_dict()


class Latest8KFilingsStream(BaseSecFilingTimeSliceStream):
    """Stream for Latest 8-K SEC Filings API."""

    name = "latest_8k_filings"

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/sec-filings-8k"


class LatestSecFilingsStream(BaseSecFilingTimeSliceStream):
    """Stream for Latest SEC Filings API."""

    name = "latest_sec_filings"

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/sec-filings-financials"


class SecFilingFormTypePartitionMixin(BaseSecFilingTimeSliceStream):
    """Mixin for SEC filing streams that need form type partitioning."""

    @property
    def partitions(self):
        query_params_form_type = self.query_params.get("formType")
        other_params_form_types = self.config.get("other_params", {}).get("form_types")

        assert not (query_params_form_type and other_params_form_types), (
            f"Cannot specify form_type configurations in both query_params and "
            f"other_params for stream {self.name}."
        )

        if query_params_form_type:
            return (
                [{"formType": query_params_form_type}]
                if isinstance(query_params_form_type, str)
                else query_params_form_type
            )
        elif other_params_form_types:
            return (
                [{"formType": form_type} for form_type in other_params_form_types]
                if isinstance(other_params_form_types, list)
                else other_params_form_types
            )
        else:
            default_form_types = ["10-K", "10-Q", "8-K", "DEF 14A", "13F-HR"]
            return [{"formType": form_type} for form_type in default_form_types]

    def get_records(self, context: Context | None):
        self.query_params.update(context)
        return super().get_records(context)


class SecFilingsByFormTypeStream(SecFilingFormTypePartitionMixin):
    """Stream for SEC Filings By Form Type API."""

    name = "sec_filings_by_form_type"
    primary_keys = ["surrogate_key"]
    _paginate = True
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("cik", th.StringType),
        th.Property("filing_date", th.StringType),
        th.Property("accepted_date", th.StringType),
        th.Property("form_type", th.StringType),
        th.Property("link", th.StringType),
        th.Property("final_link", th.StringType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/sec-filings-search/form-type"


class SecFilingsBySymbolStream(SymbolPartitionTimeSliceStream):
    """Stream for SEC Filings By Symbol API."""

    name = "sec_filings_by_symbol"
    primary_keys = ["surrogate_key"]
    replication_key = "filing_date"
    _add_surrogate_key = True
    _paginate = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("cik", th.StringType),
        th.Property("filing_date", th.StringType),
        th.Property("accepted_date", th.StringType),
        th.Property("form_type", th.StringType),
        th.Property("link", th.StringType),
        th.Property("final_link", th.StringType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/sec-filings-search/symbol"


class SecFilingsByCikStream(TimeSliceStream):
    """Stream for SEC Filings By CIK API."""

    name = "sec_filings_by_cik"
    primary_keys = ["surrogate_key"]
    replication_key = "filing_date"
    _add_surrogate_key = True
    _paginate = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("cik", th.StringType),
        th.Property("acceptance_time", th.DateTimeType),
        th.Property("final_link", th.StringType),
        th.Property("accepted_date", th.DateTimeType),
        th.Property("period_ending", th.DateType),
        th.Property("company_name", th.StringType),
        th.Property("form_type", th.StringType),
        th.Property("filing_date", th.DateTimeType),
        th.Property("ticker", th.StringType),
        th.Property("exchange", th.StringType),
        th.Property("filename", th.StringType),
        th.Property("link", th.StringType),
    ).to_dict()

    @property
    def partitions(self) -> list[dict] | None:
        return [{"cik": c.get("cik")} for c in self._tap.get_cached_ciks()]

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/sec-filings-search/cik"


class SecFilingsByNameStream(FmpRestStream):
    """Stream for SEC Filings By Name API."""

    name = "sec_filings_by_name"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("company", th.StringType),
        th.Property("name", th.StringType),
        th.Property("cik", th.StringType),
        th.Property("sic_code", th.StringType),
        th.Property("industry_title", th.StringType),
        th.Property("business_address", th.StringType),
        th.Property("phone_number", th.StringType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/sec-filings-company-search/name"

    @property
    def partitions(self) -> list[dict] | None:
        names = self.config.get(self.name, {}).get("other_params", {}).get("names")
        if not names:
            names = ["Berkshire"]
        return [{"company": name} for name in names]

    def get_records(self, context: Context | None):
        self.query_params.update(context)
        return super().get_records(context)


class SecFilingsCompanySearchBySymbolStream(SymbolPartitionStream):
    """Stream for SEC Filings Company Search By Symbol API."""

    name = "sec_filings_company_search_by_symbol"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("name", th.StringType),
        th.Property("cik", th.StringType),
        th.Property("sic_code", th.StringType),
        th.Property("industry_title", th.StringType),
        th.Property("business_address", th.StringType),
        th.Property("phone_number", th.StringType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/sec-filings-company-search/symbol"


class SecFilingsCompanySearchByCikStream(FmpRestStream):
    """Stream for SEC Filings Company Search By CIK API."""

    name = "sec_filings_company_search_by_cik"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType),
        th.Property("symbol", th.StringType),
        th.Property("name", th.StringType),
        th.Property("cik", th.StringType),
        th.Property("sic_code", th.StringType),
        th.Property("industry_title", th.StringType),
        th.Property("business_address", th.StringType),
        th.Property("phone_number", th.StringType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/sec-filings-company-search/cik"

    @property
    def partitions(self) -> list[dict] | None:
        return [{"cik": c.get("cik")} for c in self._tap.get_cached_ciks()]

    def get_records(self, context: Context | None):
        self.query_params.update(context)
        return super().get_records(context)


class SecCompanyFullProfileStream(SymbolPartitionStream):
    """Stream for SEC Company Full Profile API."""

    name = "sec_company_full_profile"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType),
        th.Property("symbol", th.StringType),
        th.Property("cik", th.StringType),
        th.Property("registrant_name", th.StringType),
        th.Property("sic_code", th.StringType),
        th.Property("sic_description", th.StringType),
        th.Property("sic_group", th.StringType),
        th.Property("isin", th.StringType),
        th.Property("business_address", th.StringType),
        th.Property("mailing_address", th.StringType),
        th.Property("phone_number", th.StringType),
        th.Property("postal_code", th.StringType),
        th.Property("city", th.StringType),
        th.Property("state", th.StringType),
        th.Property("country", th.StringType),
        th.Property("description", th.StringType),
        th.Property("ceo", th.StringType),
        th.Property("website", th.StringType),
        th.Property("exchange", th.StringType),
        th.Property("state_location", th.StringType),
        th.Property("state_of_incorporation", th.StringType),
        th.Property("fiscal_year_end", th.StringType),
        th.Property("ipo_date", th.StringType),
        th.Property("employees", th.IntegerType),
        th.Property("sec_filings_url", th.StringType),
        th.Property("tax_identification_number", th.StringType),
        th.Property("fifty_two_week_range", th.StringType),
        th.Property("is_active", th.BooleanType),
        th.Property("asset_type", th.StringType),
        th.Property("open_figi_composite", th.StringType),
        th.Property("price_currency", th.StringType),
        th.Property("market_sector", th.StringType),
        th.Property("security_type", th.StringType, required=False),
        th.Property("is_etf", th.BooleanType),
        th.Property("is_adr", th.BooleanType),
        th.Property("is_fund", th.BooleanType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/sec-profile"

    def post_process(self, row: dict, context: Context | None = None) -> dict:
        if "employees" in row and row["employees"]:
            row["employees"] = int(row["employees"])
        return super().post_process(row, context)


class IndustryClassificationListStream(FmpRestStream):
    """Stream for Industry Classification List API."""

    name = "industry_classification_list"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("sic", th.StringType),
        th.Property("industry_title", th.StringType),
        th.Property("major_group", th.StringType),
        th.Property("division", th.StringType),
        th.Property("office", th.StringType),
        th.Property("sic_code", th.StringType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/standard-industrial-classification-list"


class IndustryClassificationSearchStream(FmpRestStream):
    """Stream for Industry Classification Search API."""

    name = "industry_classification_search"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("cik", th.StringType),
        th.Property("sic_code", th.StringType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/industry-classification-search"


class AllIndustryClassificationStream(FmpRestStream):
    """Stream for All Industry Classification API."""

    name = "all_industry_classification"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType),
        th.Property("company_name", th.StringType),
        th.Property("sic", th.StringType),
        th.Property("industry_title", th.StringType),
        th.Property("exchange", th.StringType),
        th.Property("sector", th.StringType),
        th.Property("industry", th.StringType),
        th.Property("business_address", th.StringType),
        th.Property("business_phone", th.StringType),
        th.Property("website", th.StringType),
        th.Property("description", th.StringType),
        th.Property("phone_number", th.StringType),
        th.Property("cik", th.StringType),
        th.Property("name", th.StringType),
        th.Property("sic_code", th.StringType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/all-industry-classification"
