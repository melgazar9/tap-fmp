"""The global start_date is the floor for every stream: a stream's own `from`
applies only when it is later, so one setting caps how far back the whole tap
reaches. A bookmark wins over both. The global end_date is the matching
ceiling for a backfill window, and a stream it cannot bound refuses to run.
Bulk streams request exactly the years configured from that floor on, never
the literal "*". An endpoint whose `from` excludes that day is asked from the
day before, so a window's first day is kept.
"""

from __future__ import annotations

import logging
from datetime import datetime
from unittest.mock import patch

import pytest
from singer_sdk.exceptions import ConfigValidationError

from tap_fmp.client import FmpRestStream, TimeSliceStream
from tap_fmp.streams.bulk_streams import (
    EarningsSurprisesBulkStream,
    IncomeStatementBulkStream,
)
from tap_fmp.tap import TapFMP


class _IncrementalTimeSliceStream(TimeSliceStream):
    """Test-only subclass that bypasses Singer SDK construction; the state is what the SDK writes before reading."""

    name = "test_stream"
    schema = {"properties": {"date": {"type": "string"}}}
    replication_method = "INCREMENTAL"

    def __init__(
        self,
        global_start: str,
        stream_from: str | None,
        bookmark: str | None = None,
        global_end: str | None = None,
        stream_to: str | None = None,
    ):
        self.query_params = {}
        self.path_params = {}
        self.other_params = {"max_records_per_request": 10, "time_slice_days": 3650}
        self.logger = logging.getLogger("tap-fmp.test_stream")
        self._config = {"start_date": global_start}
        if global_end:
            self._config["end_date"] = global_end
        self._state = {"starting_replication_value": bookmark or global_start}
        if bookmark:
            self._state["replication_key_value"] = bookmark
        stream_query_params = {"from": stream_from, "to": stream_to}
        self._stream_config = {
            "query_params": {k: v for k, v in stream_query_params.items() if v},
            "other_params": self.other_params,
        }

    @property
    def stream_config(self):
        return self._stream_config

    @property
    def config(self):
        return self._config

    def get_context_state(self, context):
        return self._state


def _first_request(stream) -> str:
    return stream.create_time_slice_chunks(None)[0][0]


@pytest.mark.parametrize(
    "global_start,stream_from,expected",
    [
        ("2000-01-01T00:00:00Z", "1980-01-01", "2000-01-01"),
        ("1990-01-01T00:00:00Z", "2015-01-01", "2015-01-01"),
        ("1990-01-01T00:00:00Z", None, "1990-01-01"),
    ],
)
def test_a_cold_start_begins_at_the_later_of_start_date_and_the_stream_start(
    global_start, stream_from, expected
):
    assert (
        _first_request(_IncrementalTimeSliceStream(global_start, stream_from))
        == expected
    )


def test_the_bookmark_wins_over_both_starts():
    stream = _IncrementalTimeSliceStream(
        "1990-01-01T00:00:00Z", "1962-05-15", bookmark="2026-06-04"
    )

    assert _first_request(stream) == "2026-06-04"


@pytest.mark.parametrize(
    "global_end,stream_to,expected_end",
    [("1989-12-31", None, "1989-12-31"), ("1989-12-31", "1975-06-30", "1975-06-30")],
)
def test_a_backfill_window_ends_at_the_earlier_of_end_date_and_the_stream_end(
    global_end, stream_to, expected_end
):
    slices = _IncrementalTimeSliceStream(
        "1900-01-01T00:00:00Z",
        "1962-05-15",
        global_end=global_end,
        stream_to=stream_to,
    ).create_time_slice_chunks(None)

    assert slices[0][0] == "1962-05-15"
    assert slices[-1][1] == expected_end


def test_an_end_date_before_the_stream_start_requests_nothing():
    stream = _IncrementalTimeSliceStream(
        "1900-01-01T00:00:00Z", "1990-01-02", global_end="1989-12-31"
    )

    assert stream.create_time_slice_chunks(None) == []


def _tap_selecting(selected: set[str]) -> TapFMP:
    tap = TapFMP(
        config={
            "api_key": "unused",
            "start_date": "1900-01-01T00:00:00Z",
            "end_date": "1989-12-31",
        },
        parse_env_config=False,
        validate_config=False,
    )
    for name, stream in tap.streams.items():
        stream.selected = name in selected
    return tap


def test_end_date_refuses_a_selected_stream_it_cannot_bound():
    tap = _tap_selecting({"dividends_calendar", "company_screener"})

    with pytest.raises(ConfigValidationError, match="company_screener"):
        tap._refuse_streams_end_date_cannot_bound()


def test_end_date_runs_when_every_selected_stream_is_time_sliced():
    _tap_selecting(
        {"dividends_calendar", "company_chart_full"}
    )._refuse_streams_end_date_cannot_bound()


def _bulk(stream_cls, other_params: dict, bookmark: str | None = None):
    class _Stub(stream_cls):
        def __init__(self):
            self.query_params = {}
            self.other_params = dict(other_params)
            self._stream_config = {"other_params": dict(other_params)}
            self._state = {"replication_key_value": bookmark} if bookmark else {}

        @property
        def stream_config(self):
            return self._stream_config

        @property
        def config(self):
            return {"start_date": "1990-01-01T00:00:00Z"}

        def get_context_state(self, context):
            return self._state

    return _Stub()


def _requested(stream) -> list[tuple]:
    requested = []

    def fetch(self, context):
        requested.append((self.query_params["year"], self.query_params.get("period")))
        return iter([])

    with patch.object(FmpRestStream, "get_records", fetch):
        list(stream.get_records(None))
    return requested


def _years_since_start() -> list[int]:
    return list(range(1990, datetime.today().year + 1))


@pytest.mark.parametrize("configured", ["*", ["*"], None])
def test_earnings_surprises_bulk_requests_every_year_never_the_wildcard(configured):
    other_params = {} if configured is None else {"years": configured}
    requested = _requested(_bulk(EarningsSurprisesBulkStream, other_params))

    assert [year for year, _ in requested] == [str(y) for y in _years_since_start()]


def test_bulk_years_before_start_date_are_not_requested():
    requested = _requested(
        _bulk(EarningsSurprisesBulkStream, {"years": [1985, 1990, 1991]})
    )

    assert [year for year, _ in requested] == ["1990", "1991"]


def test_statement_bulk_honours_the_configured_years():
    requested = _requested(
        _bulk(IncomeStatementBulkStream, {"years": [2020, 2021], "periods": ["FY"]})
    )

    assert requested == [(2020, "FY"), (2021, "FY")]


def test_statement_bulk_resumes_at_its_bookmark_year():
    requested = _requested(
        _bulk(
            IncomeStatementBulkStream,
            {"years": "*", "periods": ["FY"]},
            bookmark="2024-06-30",
        )
    )

    assert [year for year, _ in requested] == list(
        range(2024, datetime.today().year + 1)
    )


def test_statement_bulk_cold_start_begins_at_start_date():
    requested = _requested(
        _bulk(IncomeStatementBulkStream, {"years": "*", "periods": ["FY"]})
    )

    assert [year for year, _ in requested] == _years_since_start()


@pytest.mark.parametrize(
    "exclusive_from,expected_from", [(False, "1990-01-01"), (True, "1989-12-31")]
)
def test_an_endpoint_whose_from_excludes_that_day_is_asked_from_the_day_before(
    exclusive_from, expected_from
):
    stream = _IncrementalTimeSliceStream("1990-01-01T00:00:00Z", None)
    stream._exclusive_from = exclusive_from
    requested = []

    def fetch(url, query_params):
        requested.append(dict(query_params))
        return []

    stream._fetch_with_retry = fetch
    list(
        stream.fetch_window(
            "https://example.invalid", {}, "1990-01-01", "1990-03-31", 10
        )
    )

    assert requested == [{"from": expected_from, "to": "1990-03-31"}]
