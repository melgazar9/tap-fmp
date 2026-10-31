"""Guards on what the tap requests, since every request spends the plan's
30-day bandwidth: the US-only filter never silently falls back to the whole
worldwide universe, already-loaded 13F years are not re-requested unless a
backfill lists them, the 10-K stream honours its configured years and
periods, and eod_bulk never skips a date it failed to read.
"""

from __future__ import annotations

import logging
from datetime import datetime
from unittest.mock import patch

import pytest
import requests

from tap_fmp.client import FmpRestStream, YearWindow
from tap_fmp.streams.bulk_streams import EodBulkStream
from tap_fmp.streams.form_13f_streams import (
    FilingsExtractStream,
    Form13fFilingExtractsWithAnalyticsStream,
)
from tap_fmp.streams.statements_streams import FinancialReportsForm10kJsonStream
from tap_fmp.tap import TapFMP


GLOBAL_START = "1990-01-01T00:00:00Z"


class _FakeTap:
    def get_cached_ciks(self):
        return [{"cik": "0001067983"}]

    def get_cached_company_symbols(self):
        return [{"symbol": "AAPL"}]

    def get_cached_financial_statement_symbols(self):
        return [{"symbol": "AAPL"}]


def _stub(stream_cls, other_params: dict, query_params: dict | None = None):
    """A stream instance that bypasses Singer SDK construction."""

    class _Stub(stream_cls):
        def __init__(self):
            self._tap = _FakeTap()
            self.query_params = dict(query_params or {})
            self.other_params = dict(other_params)
            self.path_params = {}
            self._stream_config = {
                "query_params": dict(query_params or {}),
                "other_params": dict(other_params),
            }

        @property
        def stream_config(self):
            return self._stream_config

        @property
        def config(self):
            return {"start_date": GLOBAL_START}

        @property
        def logger(self):
            return logging.getLogger("tap-fmp.test")

    return _Stub()


def _trailing_years() -> list[int]:
    this_year = datetime.today().year
    return list(range(this_year - YearWindow.TRAILING_YEARS + 1, this_year + 1))


class _UnfilterableTap(TapFMP):
    def __init__(self):
        pass

    @property
    def config(self):
        return {"filter_countries": ["US"], "filter_currencies": ["USD"]}

    @property
    def logger(self):
        return logging.getLogger("tap-fmp.test")

    def get_cached_company_profiles_by_symbol(self):
        raise RuntimeError("company profiles unavailable from any configured source")


def test_the_symbol_filter_refuses_to_fall_back_to_every_listing():
    symbols = [{"symbol": "AAPL"}, {"symbol": "0A1O.L"}]

    with pytest.raises(
        RuntimeError, match="refusing to run on the unfiltered 2 symbols"
    ):
        _UnfilterableTap()._apply_country_currency_filtering(symbols, "company_symbols")


@pytest.mark.parametrize("configured", [None, "*", ["*"], []])
def test_no_configured_years_means_only_the_trailing_window(configured):
    assert YearWindow.resolve(configured, GLOBAL_START) == _trailing_years()


def test_configured_years_are_a_backfill_and_kept_as_listed():
    assert YearWindow.resolve([2013, "2014"], GLOBAL_START) == [2013, 2014]
    assert YearWindow.resolve("2015", GLOBAL_START) == [2015]


def test_years_before_the_global_start_date_are_never_requested():
    assert YearWindow.resolve([1988, 1990, 1994], GLOBAL_START) == [1990, 1994]
    assert YearWindow.resolve([2013, 2014], "2014-01-01T00:00:00Z") == [2014]


@pytest.mark.parametrize(
    "stream_cls,entity",
    [
        (FilingsExtractStream, "cik"),
        (Form13fFilingExtractsWithAnalyticsStream, "symbol"),
    ],
)
def test_13f_streams_request_only_the_trailing_window_by_default(stream_cls, entity):
    partitions = _stub(stream_cls, {"quarters": ["*"]}).partitions

    assert sorted({p["year"] for p in partitions}) == _trailing_years()
    assert {p["quarter"] for p in partitions} == {1, 2, 3, 4}
    assert all(entity in p for p in partitions)


def test_13f_streams_backfill_the_years_listed():
    partitions = _stub(
        FilingsExtractStream, {"quarters": [4], "years": [2013, 2014]}
    ).partitions

    assert partitions == [
        {"cik": "0001067983", "year": 2013, "quarter": 4},
        {"cik": "0001067983", "year": 2014, "quarter": 4},
    ]


def test_10k_stream_requests_the_configured_years_and_periods():
    partitions = _stub(
        FinancialReportsForm10kJsonStream, {"years": [1994, 1995], "period": ["FY"]}
    ).partitions

    assert partitions == [
        {"symbol": "AAPL", "period": "FY", "year": "1994"},
        {"symbol": "AAPL", "period": "FY", "year": "1995"},
    ]


def test_10k_stream_without_years_requests_only_the_trailing_window():
    partitions = _stub(FinancialReportsForm10kJsonStream, {"period": "FY"}).partitions

    assert [p["year"] for p in partitions] == [str(y) for y in _trailing_years()]
    assert {p["period"] for p in partitions} == {"FY"}


def _eod_bulk(date_range: list[str]):
    stream = _stub(EodBulkStream, {"date_range": date_range})
    stream.get_starting_timestamp = lambda context: date_range[0]
    return stream


def test_eod_bulk_resumes_from_its_bookmark():
    assert EodBulkStream.is_sorted is True


def test_eod_bulk_stops_on_a_failed_date_instead_of_skipping_it():
    stream = _eod_bulk(["2024-01-02", "2024-01-03"])
    fetched = []

    def fetch(self, context):
        fetched.append(self.query_params["date"])
        if self.query_params["date"] == "2024-01-02":
            raise requests.exceptions.HTTPError("503 Client Error: Service Unavailable")
        return iter([{"symbol": "AAPL", "date": "2024-01-03"}])

    with patch.object(FmpRestStream, "get_records", fetch):
        with pytest.raises(requests.exceptions.HTTPError):
            list(stream.get_records(None))

    assert fetched == ["2024-01-02"]


def test_eod_bulk_refuses_a_row_from_another_date():
    stream = _eod_bulk(["2024-01-02", "2024-01-02"])

    def fetch(self, context):
        return iter(
            [
                {"symbol": "AAPL", "date": "2024-01-02"},
                {"symbol": "MSFT", "date": "2024-01-05"},
            ]
        )

    with patch.object(FmpRestStream, "get_records", fetch):
        records = stream.get_records(None)
        assert next(records)["symbol"] == "AAPL"
        with pytest.raises(
            ValueError,
            match="requested 2024-01-02 but FMP returned MSFT dated 2024-01-05",
        ):
            next(records)
