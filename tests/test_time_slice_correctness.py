"""Correctness tests for time-slice streams.

Verifies invariants critical for Singer state resumption:

1. When a window's `fetch_window` raises, no records from THAT window or any
   LATER windows are emitted by `get_records`. The exception propagates so
   Singer's per-record state-commit logic leaves the bookmark at the last
   fully-completed window. The next run resumes from the failed window.

2. When `fetch_window` detects truncation (max-records hit at 1-day floor,
   FMP's silent per-request cap, or pagination ceiling reached with data
   still flowing), it emits a `*** DATA_TRUNCATED ***` ERROR log so the
   Dagster sensor can flag the partial window.

If either invariant breaks, time-slice streams will silently lose data.
"""

from __future__ import annotations

import logging
from unittest.mock import patch

import pytest

from tap_fmp.client import DATA_TRUNCATED_TOKEN, TimeSliceStream


class _StubTimeSliceStream(TimeSliceStream):
    """Test-only subclass that bypasses Singer SDK construction.
    The real `__init__` requires a Tap instance + ConfigParser; we build the
    minimum surface our two methods touch."""

    name = "test_stream"
    schema = {"properties": {"date": {"type": "string"}}}

    def __init__(self):
        self.query_params = {}
        self.path_params = {}
        self.other_params = {}
        self._replication_key_starting_name = "from"
        self._replication_key_ending_name = "to"
        self.logger = logging.getLogger("tap-fmp.test_stream")
        self._fake_stream_config = {"other_params": {"max_records_per_request": 10}}

    @property
    def stream_config(self):
        return self._fake_stream_config


class _PaginatedStubTimeSliceStream(_StubTimeSliceStream):
    """Stub that opts into pagination, with a low _max_pages for fast tests."""

    _paginate = True
    _max_pages = 3


def _make_stream():
    return _StubTimeSliceStream()


def _make_paginated_stream():
    return _PaginatedStubTimeSliceStream()


def test_window_failure_propagates_and_no_later_records_emit():
    """If fetch_window raises mid-iteration, no records from later windows
    must reach the consumer. State stays at the last good window."""
    stream = _make_stream()

    yielded: list[dict] = []
    call_log: list[str] = []

    def fake_fetch_window(url, qp, from_date, to_date, max_records, context):
        call_log.append(f"{from_date}..{to_date}")
        if from_date == "2024-01-01":
            yield {"date": "2024-01-15"}
        elif from_date == "2024-04-01":
            raise RuntimeError("simulated FMP outage")
        else:
            # 2024-07-01, 2024-10-01 — must NEVER run
            pytest.fail(f"window {from_date} should not have been fetched")

    with (
        patch.object(stream, "fetch_window", side_effect=fake_fetch_window),
        patch.object(
            stream,
            "create_time_slice_chunks",
            return_value=[
                ("2024-01-01", "2024-04-01"),
                ("2024-04-01", "2024-07-01"),
                ("2024-07-01", "2024-10-01"),
            ],
        ),
        patch.object(stream, "get_url", return_value="http://example/test"),
    ):
        with pytest.raises(RuntimeError, match="simulated FMP outage"):
            for record in stream.get_records(context=None):
                yielded.append(record)

    assert yielded == [{"date": "2024-01-15"}], (
        "Records from windows after the failed one must not be yielded; "
        "doing so would advance Singer's bookmark past the gap."
    )
    assert call_log == [
        "2024-01-01..2024-04-01",
        "2024-04-01..2024-07-01",
    ], "Subsequent windows must not even be attempted after a failure."


def test_max_records_truncation_emits_alert(caplog):
    """When fetch_window hits the limit at 1-day granularity, an ERROR with
    the *** DATA_TRUNCATED *** token must be logged so Dagster can alert."""
    stream = _make_stream()

    # 10 records (the cap) — triggers the "can't split further" branch
    fake_records = [{"date": f"2024-01-15T{h:02d}:00:00"} for h in range(10)]

    with (
        patch.object(stream, "_fetch_with_retry", return_value=fake_records),
        caplog.at_level(logging.ERROR, logger="tap-fmp.test_stream"),
    ):
        list(
            stream.fetch_window(
                url="http://example/test",
                query_params={"symbol": "AAPL"},
                from_date="2024-01-15",
                to_date="2024-01-16",
                max_records=10,
                context=None,
            )
        )

    drift_lines = [r for r in caplog.records if DATA_TRUNCATED_TOKEN in r.message]
    assert (
        len(drift_lines) == 1
    ), f"Expected exactly one DATA_TRUNCATED ERROR, got {len(drift_lines)}"
    msg = drift_lines[0].message
    assert "stream=test_stream" in msg
    assert "window=2024-01-15..2024-01-16" in msg
    assert "symbol=AAPL" in msg
    assert "records_returned=10" in msg
    assert drift_lines[0].levelname == "ERROR"


def test_fetch_window_under_limit_no_truncation_alert(caplog):
    """Sanity: when fetch returns fewer records than the limit, no alert."""
    stream = _make_stream()

    with (
        patch.object(
            stream, "_fetch_with_retry", return_value=[{"date": "2024-01-15"}]
        ),
        caplog.at_level(logging.ERROR, logger="tap-fmp.test_stream"),
    ):
        records = list(
            stream.fetch_window(
                url="http://example/test",
                query_params={"symbol": "AAPL"},
                from_date="2024-01-15",
                to_date="2024-01-16",
                max_records=10,
                context=None,
            )
        )

    assert records == [{"date": "2024-01-15"}]
    drift_lines = [r for r in caplog.records if DATA_TRUNCATED_TOKEN in r.message]
    assert drift_lines == []


def _run_fetch_window(stream, records, from_date, to_date, caplog, *, symbol="AAPL"):
    with (
        patch.object(stream, "_fetch_with_retry", return_value=records),
        caplog.at_level(logging.ERROR, logger="tap-fmp.test_stream"),
    ):
        out = list(
            stream.fetch_window(
                url="http://example/test",
                query_params={"symbol": symbol},
                from_date=from_date,
                to_date=to_date,
                max_records=10,
                context=None,
            )
        )
    drift = [r for r in caplog.records if DATA_TRUNCATED_TOKEN in r.message]
    return out, drift


@pytest.mark.parametrize(
    "records,from_date,to_date,fires",
    [
        # 88-day gap: the index_1min failure mode (records cluster at end of window).
        (
            [
                {"date": "2024-03-29 09:30:00"},
                {"date": "2024-03-30 09:30:00"},
                {"date": "2024-03-31 09:30:00"},
            ],
            "2024-01-01",
            "2024-04-01",
            True,
        ),
        # 7-day gap: within tolerance (long-weekend / holiday).
        (
            [{"date": "2024-01-08 09:30:00"}, {"date": "2024-01-09 09:30:00"}],
            "2024-01-01",
            "2024-01-10",
            False,
        ),
        # 8-day gap: just past tolerance, must fire.
        ([{"date": "2024-01-09 09:30:00"}], "2024-01-01", "2024-01-15", True),
        # Empty response is legitimate (delisted symbol, future window).
        ([], "2024-01-01", "2024-04-01", False),
        # Malformed dates: parse error caught, no crash, no fire.
        (
            [{"date": "not-a-date"}, {"date": "also bad"}],
            "2024-01-01",
            "2024-04-01",
            False,
        ),
        # Records without a "date" field (news uses published_date) — skipped silently.
        (
            [
                {"published_date": "2024-03-29 09:30:00", "title": "x"},
                {"published_date": "2024-03-30 09:30:00", "title": "y"},
            ],
            "2024-01-01",
            "2024-04-01",
            False,
        ),
    ],
    ids=[
        "88-day-gap-fires",
        "within-7-day-tolerance",
        "8-day-just-past-tolerance",
        "empty-response",
        "malformed-dates",
        "no-date-field",
    ],
)
def test_silent_truncation_alert(records, from_date, to_date, fires, caplog):
    """Detect when FMP silently caps responses below max_records and only
    returns the trailing portion of the requested window. Records still
    yield — alert is detection-only, not data dropping."""
    out, drift = _run_fetch_window(_make_stream(), records, from_date, to_date, caplog)
    assert out == records
    assert len(drift) == (1 if fires else 0)


def test_silent_truncation_alert_message_structure(caplog):
    """Pin the ERROR message fields the Dagster sensor matches on."""
    fake_records = [
        {"date": "2024-03-29 09:30:00"},
        {"date": "2024-03-30 09:30:00"},
    ]
    _, drift = _run_fetch_window(
        _make_stream(),
        fake_records,
        from_date="2024-01-01",
        to_date="2024-04-01",
        caplog=caplog,
        symbol="^GSPC",
    )
    assert len(drift) == 1
    msg = drift[0].message
    assert drift[0].levelname == "ERROR"
    for fragment in (
        "stream=test_stream",
        "window=2024-01-01..2024-04-01",
        "symbol=^GSPC",
        "earliest_returned=2024-03-29",
        "reason=fmp_silent_cap_below_max_records",
        "action=set_smaller_time_slice_days_for_this_stream",
    ):
        assert fragment in msg, f"missing {fragment!r} in {msg!r}"


def _run_paginated_fetch_window(stream, page_returns, from_date, to_date, caplog):
    """Drive `fetch_window` against a paginated stream stub. `page_returns`
    is a list indexed by page; `_fetch_with_retry` is patched to return
    `page_returns[page]` (or `[]` for pages beyond the list)."""

    def fake(url, query_params, page=None):
        if page is None or page >= len(page_returns):
            return []
        return page_returns[page]

    with (
        patch.object(stream, "_fetch_with_retry", side_effect=fake),
        caplog.at_level(logging.ERROR, logger="tap-fmp.test_stream"),
    ):
        out = list(
            stream.fetch_window(
                url="http://example/test",
                query_params={"symbol": "AAPL"},
                from_date=from_date,
                to_date=to_date,
                max_records=10,
                context=None,
            )
        )
    drift = [r for r in caplog.records if DATA_TRUNCATED_TOKEN in r.message]
    return out, drift


def test_paginated_window_aggregates_pages_and_stops_on_empty(caplog):
    """fetch_window iterates pages and aggregates records until 2
    consecutive empty pages signal natural exhaustion."""
    out, _ = _run_paginated_fetch_window(
        _make_paginated_stream(),
        page_returns=[
            [{"date": "2024-03-29", "id": 1}, {"date": "2024-03-30", "id": 2}],
            [{"date": "2024-03-27", "id": 3}, {"date": "2024-03-28", "id": 4}],
            [],
        ],
        from_date="2024-01-01",
        to_date="2024-04-01",
        caplog=caplog,
    )
    assert [r["id"] for r in out] == [1, 2, 3, 4]


def test_paginated_window_hits_page_ceiling_logs_truncation(caplog):
    """Exhausting _max_pages while pages still return data signals FMP
    has more than we can retrieve in this window — log with the
    hit_page_ceiling reason so the operator can lower time_slice_days."""
    full_page = [{"date": "2024-03-29 09:30", "id": i} for i in range(5)]
    # _max_pages=3 → loop runs page 0..3 → 4 pages of data → ceiling hit.
    _, drift = _run_paginated_fetch_window(
        _make_paginated_stream(),
        page_returns=[full_page] * 4,
        from_date="2024-01-01",
        to_date="2024-04-01",
        caplog=caplog,
    )
    assert len(drift) == 1
    msg = drift[0].message
    assert "reason=hit_page_ceiling_with_data" in msg
    assert "max_pages=3" in msg


def test_paginated_window_natural_exhaustion_no_ceiling_alert(caplog):
    """Natural exhaustion (2 consecutive empty pages) means we got
    everything FMP has for this window — no `hit_page_ceiling` alert.
    The earliest-date safety net is orthogonal and may still fire if
    records cluster late in the window."""
    out, drift = _run_paginated_fetch_window(
        _make_paginated_stream(),
        page_returns=[[{"date": "2024-03-29", "id": 1}], [], []],
        from_date="2024-03-25",
        to_date="2024-04-01",
        caplog=caplog,
    )
    assert [r["id"] for r in out] == [1]
    assert drift == []
    # Defensive: even outside tolerance, the page-ceiling reason must not fire
    assert not any("hit_page_ceiling" in r.message for r in caplog.records)
