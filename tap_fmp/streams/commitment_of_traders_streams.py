from tap_fmp.client import SymbolPartitionTimeSliceStream, FmpRestStream
from singer_sdk.helpers.types import Context
from singer_sdk import typing as th

from tap_fmp.helpers import generate_surrogate_key



class CotReportStream(SymbolPartitionTimeSliceStream):
    name = "cot_report"

    schema = th.PropertiesList(
        th.Property("surrogate_key", th.StringType, required=True),
        th.Property("symbol", th.StringType, required=True),
        th.Property("date", th.DateTimeType),
        th.Property("name", th.StringType),
        th.Property("sector", th.StringType),
        th.Property("market_and_exchange_names", th.StringType),
        th.Property("cftc_contract_market_code", th.StringType),
        th.Property("cftc_market_code", th.StringType),
        th.Property("cftc_region_code", th.StringType),
        th.Property("cftc_commodity_code", th.StringType),
        th.Property("open_interest_all", th.NumberType),
        th.Property("noncomm_positions_long_all", th.NumberType),
        th.Property("noncomm_positions_short_all", th.NumberType),
        th.Property("noncomm_positions_spread_all", th.NumberType),
        th.Property("comm_positions_long_all", th.NumberType),
        th.Property("comm_positions_short_all", th.NumberType),
        th.Property("tot_rept_positions_long_all", th.NumberType),
        th.Property("tot_rept_positions_short_all", th.NumberType),
        th.Property("nonrept_positions_long_all", th.NumberType),
        th.Property("nonrept_positions_short_all", th.NumberType),
        th.Property("open_interest_old", th.NumberType),
        th.Property("noncomm_positions_long_old", th.NumberType),
        th.Property("noncomm_positions_short_old", th.NumberType),
        th.Property("noncomm_positions_spread_old", th.NumberType),
        th.Property("comm_positions_long_old", th.NumberType),
        th.Property("comm_positions_short_old", th.NumberType),
        th.Property("tot_rept_positions_long_old", th.NumberType),
        th.Property("tot_rept_positions_short_old", th.NumberType),
        th.Property("nonrept_positions_long_old", th.NumberType),
        th.Property("nonrept_positions_short_old", th.NumberType),
        th.Property("open_interest_other", th.NumberType),
        th.Property("noncomm_positions_long_other", th.NumberType),
        th.Property("noncomm_positions_short_other", th.NumberType),
        th.Property("noncomm_positions_spread_other", th.NumberType),
        th.Property("comm_positions_long_other", th.NumberType),
        th.Property("comm_positions_short_other", th.NumberType),
        th.Property("tot_rept_positions_long_other", th.NumberType),
        th.Property("tot_rept_positions_short_other", th.NumberType),
        th.Property("nonrept_positions_long_other", th.NumberType),
        th.Property("nonrept_positions_short_other", th.NumberType),
        th.Property("change_in_open_interest_all", th.NumberType),
        th.Property("change_in_noncomm_long_all", th.NumberType),
        th.Property("change_in_noncomm_short_all", th.NumberType),
        th.Property("change_in_noncomm_spead_all", th.NumberType),
        th.Property("change_in_comm_long_all", th.NumberType),
        th.Property("change_in_comm_short_all", th.NumberType),
        th.Property("change_in_tot_rept_long_all", th.NumberType),
        th.Property("change_in_tot_rept_short_all", th.NumberType),
        th.Property("change_in_nonrept_long_all", th.NumberType),
        th.Property("change_in_nonrept_short_all", th.NumberType),
        th.Property("pct_of_open_interest_all", th.NumberType),
        th.Property("pct_of_oi_noncomm_long_all", th.NumberType),
        th.Property("pct_of_oi_noncomm_short_all", th.NumberType),
        th.Property("pct_of_oi_noncomm_spread_all", th.NumberType),
        th.Property("pct_of_oi_comm_long_all", th.NumberType),
        th.Property("pct_of_oi_comm_short_all", th.NumberType),
        th.Property("pct_of_oi_tot_rept_long_all", th.NumberType),
        th.Property("pct_of_oi_tot_rept_short_all", th.NumberType),
        th.Property("pct_of_oi_nonrept_long_all", th.NumberType),
        th.Property("pct_of_oi_nonrept_short_all", th.NumberType),
        th.Property("pct_of_open_interest_ol", th.NumberType),
        th.Property("pct_of_oi_noncomm_long_ol", th.NumberType),
        th.Property("pct_of_oi_noncomm_short_ol", th.NumberType),
        th.Property("pct_of_oi_noncomm_spread_ol", th.NumberType),
        th.Property("pct_of_oi_comm_long_ol", th.NumberType),
        th.Property("pct_of_oi_comm_short_ol", th.NumberType),
        th.Property("pct_of_oi_tot_rept_long_ol", th.NumberType),
        th.Property("pct_of_oi_tot_rept_short_ol", th.NumberType),
        th.Property("pct_of_oi_nonrept_long_ol", th.NumberType),
        th.Property("pct_of_oi_nonrept_short_ol", th.NumberType),
        th.Property("pct_of_open_interest_other", th.NumberType),
        th.Property("pct_of_oi_noncomm_long_other", th.NumberType),
        th.Property("pct_of_oi_noncomm_short_other", th.NumberType),
        th.Property("pct_of_oi_noncomm_spread_other", th.NumberType),
        th.Property("pct_of_oi_comm_long_other", th.NumberType),
        th.Property("pct_of_oi_comm_short_other", th.NumberType),
        th.Property("pct_of_oi_tot_rept_long_other", th.NumberType),
        th.Property("pct_of_oi_tot_rept_short_other", th.NumberType),
        th.Property("pct_of_oi_nonrept_long_other", th.NumberType),
        th.Property("pct_of_oi_nonrept_short_other", th.NumberType),
        th.Property("traders_tot_all", th.NumberType),
        th.Property("traders_noncomm_long_all", th.NumberType),
        th.Property("traders_noncomm_short_all", th.NumberType),
        th.Property("traders_noncomm_spread_all", th.NumberType),
        th.Property("traders_comm_long_all", th.NumberType),
        th.Property("traders_comm_short_all", th.NumberType),
        th.Property("traders_tot_rept_long_all", th.NumberType),
        th.Property("traders_tot_rept_short_all", th.NumberType),
        th.Property("traders_tot_ol", th.NumberType),
        th.Property("traders_noncomm_long_ol", th.NumberType),
        th.Property("traders_noncomm_short_ol", th.NumberType),
        th.Property("traders_noncomm_spead_ol", th.NumberType),
        th.Property("traders_comm_long_ol", th.NumberType),
        th.Property("traders_comm_short_ol", th.NumberType),
        th.Property("traders_tot_rept_long_ol", th.NumberType),
        th.Property("traders_tot_rept_short_ol", th.NumberType),
        th.Property("traders_tot_other", th.NumberType),
        th.Property("traders_noncomm_long_other", th.NumberType),
        th.Property("traders_noncomm_short_other", th.NumberType),
        th.Property("traders_noncomm_spread_other", th.NumberType),
        th.Property("traders_comm_long_other", th.NumberType),
        th.Property("traders_comm_short_other", th.NumberType),
        th.Property("traders_tot_rept_long_other", th.NumberType),
        th.Property("traders_tot_rept_short_other", th.NumberType),
        th.Property("conc_gross_le4_tdr_long_all", th.NumberType),
        th.Property("conc_gross_le4_tdr_short_all", th.NumberType),
        th.Property("conc_gross_le8_tdr_long_all", th.NumberType),
        th.Property("conc_gross_le8_tdr_short_all", th.NumberType),
        th.Property("conc_net_le4_tdr_long_all", th.NumberType),
        th.Property("conc_net_le4_tdr_short_all", th.NumberType),
        th.Property("conc_net_le8_tdr_long_all", th.NumberType),
        th.Property("conc_net_le8_tdr_short_all", th.NumberType),
        th.Property("conc_gross_le4_tdr_long_ol", th.NumberType),
        th.Property("conc_gross_le4_tdr_short_ol", th.NumberType),
        th.Property("conc_gross_le8_tdr_long_ol", th.NumberType),
        th.Property("conc_gross_le8_tdr_short_ol", th.NumberType),
        th.Property("conc_net_le4_tdr_long_ol", th.NumberType),
        th.Property("conc_net_le4_tdr_short_ol", th.NumberType),
        th.Property("conc_net_le8_tdr_long_ol", th.NumberType),
        th.Property("conc_net_le8_tdr_short_ol", th.NumberType),
        th.Property("conc_gross_le4_tdr_long_other", th.NumberType),
        th.Property("conc_gross_le4_tdr_short_other", th.NumberType),
        th.Property("conc_gross_le8_tdr_long_other", th.NumberType),
        th.Property("conc_gross_le8_tdr_short_other", th.NumberType),
        th.Property("conc_net_le4_tdr_long_other", th.NumberType),
        th.Property("conc_net_le4_tdr_short_other", th.NumberType),
        th.Property("conc_net_le8_tdr_long_other", th.NumberType),
        th.Property("conc_net_le8_tdr_short_other", th.NumberType),
        th.Property("contract_units", th.StringType)
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/commitment-of-traders-report"

    def post_process(self, row: dict, context: Context | None = None) -> dict:
        row["surrogate_key"] = generate_surrogate_key(row)
        return row


class CotAnalysisByDateStream(SymbolPartitionTimeSliceStream):
    name = "cot_analysis_by_date"

    schema = th.PropertiesList(
        th.Property("symbol", th.StringType, required=True),
        th.Property("date", th.DateTimeType),
        th.Property("name", th.StringType),
        th.Property("sector", th.StringType),
        th.Property("exchange", th.StringType),
        th.Property("current_long_market_situation", th.NumberType),
        th.Property("current_short_market_situation", th.NumberType),
        th.Property("market_situation", th.StringType),
        th.Property("previous_long_market_situation", th.NumberType),
        th.Property("previous_short_market_situation", th.NumberType),
        th.Property("previous_market_situation", th.StringType),
        th.Property("net_position", th.NumberType),
        th.Property("previous_net_position", th.NumberType),
        th.Property("change_in_net_position", th.NumberType),
        th.Property("market_sentiment", th.StringType),
        th.Property("reversal_trend", th.BooleanType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/commitment-of-traders-analysis"

    def post_process(self, row: dict, context: Context | None = None) -> dict:
        if "net_postion" in row:
            row["net_position"] = row.pop("net_postion")
        return super().post_process(row, context)

class CotReportListStream(FmpRestStream):
    name = "cot_report_list"
    primary_keys = ["symbol", "name"]

    schema = th.PropertiesList(
        th.Property("symbol", th.StringType, required=True),
        th.Property("name", th.StringType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/commitment-of-traders-list"