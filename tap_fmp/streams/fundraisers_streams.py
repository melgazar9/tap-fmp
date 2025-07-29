"""Fundraiser stream types classes for tap-fmp."""

from __future__ import annotations

import typing as t
from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_fmp.client import FmpRestStream


class LatestCrowdfundingCampaignsStream(FmpRestStream):
    """Stream for Latest Crowdfunding Campaigns API."""

    name = "latest_crowdfunding_campaigns"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True
    _paginate = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("cik", th.StringType),
        th.Property("company_name", th.StringType),
        th.Property("date", th.StringType),
        th.Property("filing_date", th.DateTimeType),
        th.Property("accepted_date", th.DateTimeType),
        th.Property("form_type", th.StringType),
        th.Property("form_signification", th.StringType),
        th.Property("name_of_issuer", th.StringType),
        th.Property("legal_status_form", th.StringType),
        th.Property("jurisdiction_organization", th.StringType),
        th.Property("issuer_street", th.StringType),
        th.Property("issuer_city", th.StringType),
        th.Property("issuer_state_or_country", th.StringType),
        th.Property("issuer_zip_code", th.StringType),
        th.Property("issuer_website", th.StringType),
        th.Property("intermediary_company_name", th.StringType),
        th.Property("intermediary_commission_cik", th.StringType),
        th.Property("intermediary_commission_file_number", th.StringType),
        th.Property("compensation_amount", th.StringType),
        th.Property("financial_interest", th.StringType),
        th.Property("security_offered_type", th.StringType),
        th.Property("security_offered_other_description", th.StringType),
        th.Property("number_of_security_offered", th.IntegerType),
        th.Property("offering_price", th.NumberType),
        th.Property("offering_amount", th.NumberType),
        th.Property("over_subscription_accepted", th.StringType),
        th.Property("over_subscription_allocation_type", th.StringType),
        th.Property("maximum_offering_amount", th.NumberType),
        th.Property("offering_deadline_date", th.StringType),
        th.Property("current_number_of_employees", th.NumberType),
        th.Property("total_asset_most_recent_fiscal_year", th.NumberType),
        th.Property("total_asset_prior_fiscal_year", th.NumberType),
        th.Property("cash_and_cash_equi_valent_most_recent_fiscal_year", th.NumberType),
        th.Property("cash_and_cash_equi_valent_prior_fiscal_year", th.NumberType),
        th.Property("accounts_receivable_most_recent_fiscal_year", th.NumberType),
        th.Property("accounts_receivable_prior_fiscal_year", th.NumberType),
        th.Property("short_term_debt_most_recent_fiscal_year", th.NumberType),
        th.Property("short_term_debt_prior_fiscal_year", th.NumberType),
        th.Property("long_term_debt_most_recent_fiscal_year", th.NumberType),
        th.Property("long_term_debt_prior_fiscal_year", th.NumberType),
        th.Property("revenue_most_recent_fiscal_year", th.NumberType),
        th.Property("revenue_prior_fiscal_year", th.NumberType),
        th.Property("cost_goods_sold_most_recent_fiscal_year", th.NumberType),
        th.Property("cost_goods_sold_prior_fiscal_year", th.NumberType),
        th.Property("taxes_paid_most_recent_fiscal_year", th.NumberType),
        th.Property("taxes_paid_prior_fiscal_year", th.NumberType),
        th.Property("net_income_most_recent_fiscal_year", th.NumberType),
        th.Property("net_income_prior_fiscal_year", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/crowdfunding-offerings-latest"


# class CrowdfundingCampaignSearchStream(FmpRestStream):
#     """Stream for Crowdfunding Campaign Search API."""
#
#     name = "crowdfunding_campaign_search"
#     primary_keys = ["surrogate_key"]
#     _add_surrogate_key = True
#
#     schema = th.PropertiesList(
#         th.Property("surrogate_key", th.StringType, required=True),
#         th.Property("cik", th.StringType),
#         th.Property("name", th.StringType),
#         th.Property("date", th.DateTimeType),
#     ).to_dict()
#
#     def get_url(self, context: Context | None = None) -> str:
#         return f"{self.url_base}/stable/crowdfunding-offerings-search"
#
#     @property
#     def partitions(self) -> list[dict] | None:
#         return [{"name": n["name"]} for n in self.get_names()]


class CrowdfundingByCikStream(FmpRestStream):
    """Stream for Crowdfunding By CIK API."""

    name = "crowdfunding_by_cik"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("cik", th.StringType),
        th.Property("company_name", th.StringType),
        th.Property("date", th.StringType),
        th.Property("filing_date", th.DateTimeType),
        th.Property("accepted_date", th.DateTimeType),
        th.Property("form_type", th.StringType),
        th.Property("form_signification", th.StringType),
        th.Property("name_of_issuer", th.StringType),
        th.Property("legal_status_form", th.StringType),
        th.Property("jurisdiction_organization", th.StringType),
        th.Property("issuer_street", th.StringType),
        th.Property("issuer_city", th.StringType),
        th.Property("issuer_state_or_country", th.StringType),
        th.Property("issuer_zip_code", th.StringType),
        th.Property("issuer_website", th.StringType),
        th.Property("intermediary_company_name", th.StringType),
        th.Property("intermediary_commission_cik", th.StringType),
        th.Property("intermediary_commission_file_number", th.StringType),
        th.Property("compensation_amount", th.StringType),
        th.Property("financial_interest", th.StringType),
        th.Property("security_offered_type", th.StringType),
        th.Property("security_offered_other_description", th.StringType),
        th.Property("number_of_security_offered", th.IntegerType),
        th.Property("offering_price", th.NumberType),
        th.Property("offering_amount", th.NumberType),
        th.Property("over_subscription_accepted", th.StringType),
        th.Property("over_subscription_allocation_type", th.StringType),
        th.Property("maximum_offering_amount", th.NumberType),
        th.Property("offering_deadline_date", th.StringType),
        th.Property("current_number_of_employees", th.NumberType),
        th.Property("total_asset_most_recent_fiscal_year", th.NumberType),
        th.Property("total_asset_prior_fiscal_year", th.NumberType),
        th.Property("cash_and_cash_equi_valent_most_recent_fiscal_year", th.NumberType),
        th.Property("cash_and_cash_equi_valent_prior_fiscal_year", th.NumberType),
        th.Property("accounts_receivable_most_recent_fiscal_year", th.NumberType),
        th.Property("accounts_receivable_prior_fiscal_year", th.NumberType),
        th.Property("short_term_debt_most_recent_fiscal_year", th.NumberType),
        th.Property("short_term_debt_prior_fiscal_year", th.NumberType),
        th.Property("long_term_debt_most_recent_fiscal_year", th.NumberType),
        th.Property("long_term_debt_prior_fiscal_year", th.NumberType),
        th.Property("revenue_most_recent_fiscal_year", th.NumberType),
        th.Property("revenue_prior_fiscal_year", th.NumberType),
        th.Property("cost_goods_sold_most_recent_fiscal_year", th.NumberType),
        th.Property("cost_goods_sold_prior_fiscal_year", th.NumberType),
        th.Property("taxes_paid_most_recent_fiscal_year", th.NumberType),
        th.Property("taxes_paid_prior_fiscal_year", th.NumberType),
        th.Property("net_income_most_recent_fiscal_year", th.NumberType),
        th.Property("net_income_prior_fiscal_year", th.NumberType),
    ).to_dict()

    @property
    def partitions(self) -> list[dict] | None:
        return [{"cik": c.get("cik")} for c in self._tap.get_cached_ciks()]

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/crowdfunding-offerings"

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        self.query_params.update(context)
        return super().get_records(context)


class EquityOfferingUpdatesStream(FmpRestStream):
    """Stream for Equity Offering Updates API."""

    name = "equity_offering_updates"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True
    _paginate = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("cik", th.StringType),
        th.Property("company_name", th.StringType),
        th.Property("date", th.StringType),
        th.Property("filing_date", th.DateTimeType),
        th.Property("accepted_date", th.DateTimeType),
        th.Property("form_type", th.StringType),
        th.Property("form_signification", th.StringType),
        th.Property("entity_name", th.StringType),
        th.Property("issuer_street", th.StringType),
        th.Property("issuer_city", th.StringType),
        th.Property("issuer_state_or_country", th.StringType),
        th.Property("issuer_state_or_country_description", th.StringType),
        th.Property("issuer_zip_code", th.StringType),
        th.Property("issuer_phone_number", th.StringType),
        th.Property("jurisdiction_of_incorporation", th.StringType),
        th.Property("entity_type", th.StringType),
        th.Property("incorporated_within_five_years", th.BooleanType),
        th.Property("year_of_incorporation", th.StringType),
        th.Property("related_person_first_name", th.StringType),
        th.Property("related_person_last_name", th.StringType),
        th.Property("related_person_street", th.StringType),
        th.Property("related_person_city", th.StringType),
        th.Property("related_person_state_or_country", th.StringType),
        th.Property("related_person_state_or_country_description", th.StringType),
        th.Property("related_person_zip_code", th.StringType),
        th.Property("related_person_relationship", th.StringType),
        th.Property("industry_group_type", th.StringType),
        th.Property("revenue_range", th.StringType),
        th.Property("federal_exemptions_exclusions", th.StringType),
        th.Property("is_amendment", th.BooleanType),
        th.Property("date_of_first_sale", th.StringType),
        th.Property("duration_of_offering_is_more_than_year", th.BooleanType),
        th.Property("securities_offered_are_of_equity_type", th.BooleanType),
        th.Property("is_business_combination_transaction", th.BooleanType),
        th.Property("minimum_investment_accepted", th.NumberType),
        th.Property("total_offering_amount", th.NumberType),
        th.Property("total_amount_sold", th.NumberType),
        th.Property("total_amount_remaining", th.NumberType),
        th.Property("has_non_accredited_investors", th.BooleanType),
        th.Property("total_number_already_invested", th.IntegerType),
        th.Property("sales_commissions", th.NumberType),
        th.Property("finders_fees", th.NumberType),
        th.Property("gross_proceeds_used", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/fundraising-latest"

    @property
    def partitions(self):
        if (
            self.config.get(self.name, {})
            .get("other_params", {})
            .get("use_cached_ciks")
        ):
            return [{"cik": c["cik"]} for c in self._tap.get_cached_ciks()]
        return {}

    def get_records(self, context: Context | None):
        if context:
            self.query_params.update(context)
        return super().get_records(context)


# class EquityOfferingSearchStream(FmpRestStream):
#     """Stream for Equity Offering Search API."""
#
#     name = "equity_offering_search"
#     primary_keys = ["surrogate_key"]
#     _add_surrogate_key = True
#
#     schema = th.PropertiesList(
#         th.Property("surrogate_key", th.StringType, required=True),
#         th.Property("cik", th.StringType),
#         th.Property("name", th.StringType),
#         th.Property("date", th.DateTimeType),
#     ).to_dict()
#
#     def get_url(self, context: Context | None = None) -> str:
#         return f"{self.url_base}/stable/fundraising-search"
#
#     @property
#     def partitions(self) -> list[dict] | None:
#         return [{"name": n["cik"]} for n in self.get_names()]


class EquityOfferingByCikStream(FmpRestStream):
    """Stream for Equity Offering By CIK API."""

    name = "equity_offering_by_cik"
    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("cik", th.StringType),
        th.Property("company_name", th.StringType),
        th.Property("date", th.StringType),
        th.Property("filing_date", th.DateTimeType),
        th.Property("accepted_date", th.DateTimeType),
        th.Property("form_type", th.StringType),
        th.Property("form_signification", th.StringType),
        th.Property("entity_name", th.StringType),
        th.Property("issuer_street", th.StringType),
        th.Property("issuer_city", th.StringType),
        th.Property("issuer_state_or_country", th.StringType),
        th.Property("issuer_state_or_country_description", th.StringType),
        th.Property("issuer_zip_code", th.StringType),
        th.Property("issuer_phone_number", th.StringType),
        th.Property("jurisdiction_of_incorporation", th.StringType),
        th.Property("entity_type", th.StringType),
        th.Property("incorporated_within_five_years", th.BooleanType),
        th.Property("year_of_incorporation", th.StringType),
        th.Property("related_person_first_name", th.StringType),
        th.Property("related_person_last_name", th.StringType),
        th.Property("related_person_street", th.StringType),
        th.Property("related_person_city", th.StringType),
        th.Property("related_person_state_or_country", th.StringType),
        th.Property("related_person_state_or_country_description", th.StringType),
        th.Property("related_person_zip_code", th.StringType),
        th.Property("related_person_relationship", th.StringType),
        th.Property("industry_group_type", th.StringType),
        th.Property("revenue_range", th.StringType),
        th.Property("federal_exemptions_exclusions", th.StringType),
        th.Property("is_amendment", th.BooleanType),
        th.Property("date_of_first_sale", th.StringType),
        th.Property("duration_of_offering_is_more_than_year", th.BooleanType),
        th.Property("securities_offered_are_of_equity_type", th.BooleanType),
        th.Property("is_business_combination_transaction", th.BooleanType),
        th.Property("minimum_investment_accepted", th.NumberType),
        th.Property("total_offering_amount", th.NumberType),
        th.Property("total_amount_sold", th.NumberType),
        th.Property("total_amount_remaining", th.NumberType),
        th.Property("has_non_accredited_investors", th.BooleanType),
        th.Property("total_number_already_invested", th.IntegerType),
        th.Property("sales_commissions", th.NumberType),
        th.Property("finders_fees", th.NumberType),
        th.Property("gross_proceeds_used", th.NumberType),
    ).to_dict()

    @property
    def partitions(self):
        """Partition by CIK."""
        return [{"cik": c.get("cik")} for c in self._tap.get_cached_ciks()]

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/fundraising"

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        self.query_params.update(context)
        return super().get_records(context)
