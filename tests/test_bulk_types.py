"""Bulk CSVs carry every value as text, and the SDK reads any non-empty text,
"false" included, as true; each column converts by its schema type instead.
FMP may restrict a key that keeps calling a bulk file, so its 429s retry at a
slow even pace while other failures back off as on any stream.
"""

from __future__ import annotations

import time
from datetime import date
from decimal import Decimal

import pytest

from tap_fmp.client import FmpQuotaExhaustedError
from tap_fmp.helpers import iso_date, whole_count
from tap_fmp.streams.bulk_streams import (
    BaseBulkStream,
    CompanyProfileBulkStream,
    EodBulkStream,
    StockRatingBulkStream,
)
from tests.test_quota_exhaustion import _ScriptedSession, _StubStream, _fetch
from tests.test_statement_periods import _tap

BULK_LIMIT_BODY = b'{"Error Message": "Limit Reach. This is a bulk endpoint ..."}'


def _profile_bulk_stream() -> CompanyProfileBulkStream:
    stream = CompanyProfileBulkStream(_tap())
    stream.query_params["part"] = 0
    return stream


@pytest.mark.parametrize(
    "sent,stored",
    [
        ("164000", 164000),
        ("382 ", 382),
        ("1710.67", 1711),
        ("1657.5", 1658),
        ("", None),
        ("1-10", None),
        (None, None),
        (12, 12),
    ],
)
def test_a_headcount_is_a_whole_number_or_unknown(sent, stored):
    assert whole_count(sent) == stored


def test_a_headcount_fmp_never_sends_stops_the_stream():
    with pytest.raises(ValueError, match="not a headcount"):
        whole_count("about 40")


def test_listing_dates_are_dates():
    assert iso_date("1980-12-12") == date(1980, 12, 12)
    assert iso_date("1980-12-12 00:00:00") == date(1980, 12, 12)
    assert iso_date(" ") is None
    with pytest.raises(ValueError):
        iso_date("today")


def test_a_profile_row_from_csv_text_gets_its_column_types():
    row = {
        "symbol": "AAPL",
        "price": "270.37",
        "is_etf": "false",
        "is_actively_trading": "True",
        "full_time_employees": "324.5",
        "ipo_date": "1980-12-12",
        "filing_date": "",
    }

    record = _profile_bulk_stream().post_process(row)

    assert record["price"] == Decimal("270.37")
    assert record["is_etf"] is False
    assert record["is_actively_trading"] is True
    assert record["full_time_employees"] == 325
    assert record["ipo_date"] == date(1980, 12, 12)
    assert record["filing_date"] is None
    assert record["_part"] == 0


def test_a_value_that_fits_no_column_names_the_column():
    with pytest.raises(ValueError, match="column is_etf"):
        _profile_bulk_stream().post_process({"symbol": "AAPL", "is_etf": "maybe"})


def test_an_integer_column_refuses_a_fraction():
    stream = StockRatingBulkStream(_tap())
    integer_column = next(
        column
        for column, prop in stream.schema["properties"].items()
        if "integer" in prop.get("type", [])
    )

    with pytest.raises(ValueError, match=f"column {integer_column}"):
        stream.post_process({"symbol": "AAPL", integer_column: "3.5"})


def test_eod_bulk_converts_by_its_schema():
    stream = EodBulkStream(_tap())
    record = stream.post_process(
        {"symbol": "AAPL", "date": "2026-10-30", "close": "270.37", "volume": "1"}
    )

    assert record["date"] == date(2026, 10, 30)
    assert record["close"] == Decimal("270.37")
    with pytest.raises(ValueError, match="column close"):
        stream.post_process({"symbol": "AAPL", "date": "2026-10-30", "close": "n/a"})


def test_every_run_reads_every_part():
    assert CompanyProfileBulkStream(_tap()).replication_key is None


def test_configured_parts_are_each_requested(monkeypatch):
    tap = _tap(company_profile_bulk={"other_params": {"parts": [0, 1]}})
    stream = CompanyProfileBulkStream(tap)
    requested = []

    def fetch(url, query_params, page=None):
        query_params["part"] = page
        requested.append(page)
        return [{"symbol": f"S{page}"}]

    monkeypatch.setattr(stream, "_fetch_with_retry", fetch)

    records = list(stream.get_records(None))

    assert requested == [0, 1]
    assert [r["_part"] for r in records] == [0, 1]


class _BulkStub(_StubStream):
    _quota_retry_interval_seconds = BaseBulkStream._quota_retry_interval_seconds
    _quota_retry_waits = BaseBulkStream._quota_retry_waits


@pytest.fixture
def fake_clock(monkeypatch):
    """Sleeps advance a fake monotonic clock, so the retry pacing runs instantly."""
    now = [0.0]
    sleeps = []

    def sleep(seconds):
        sleeps.append(seconds)
        now[0] += seconds

    monkeypatch.setattr(time, "monotonic", lambda: now[0])
    monkeypatch.setattr(time, "sleep", sleep)
    return sleeps


def _bulk_stub(script) -> tuple[_BulkStub, _ScriptedSession]:
    session = _ScriptedSession(script)
    stream = _BulkStub(
        session,
        quota_exhausted_after_seconds=BaseBulkStream._quota_exhausted_after_seconds,
    )
    return stream, session


def test_a_refused_bulk_call_is_retried_evenly_then_stops(fake_clock):
    stream, session = _bulk_stub([(429, BULK_LIMIT_BODY)])
    interval = BaseBulkStream._quota_retry_interval_seconds
    retries = int(BaseBulkStream._quota_exhausted_after_seconds // interval)

    with pytest.raises(FmpQuotaExhaustedError, match="Limit Reach"):
        _fetch(stream)

    assert fake_clock == [interval] * retries
    assert session.calls == retries + 1


def test_a_bulk_call_that_clears_on_a_retry_loads(fake_clock):
    stream, session = _bulk_stub([(429, BULK_LIMIT_BODY), (200, b"[]")])

    assert _fetch(stream) == []
    assert session.calls == 2


def test_a_bulk_gateway_error_backs_off_exponentially_not_at_the_quota_pace(
    fake_clock,
):
    stream, session = _bulk_stub([(502, b"bad gateway"), (200, b"[]")])

    assert _fetch(stream) == []
    assert fake_clock[0] < BaseBulkStream._quota_retry_interval_seconds
