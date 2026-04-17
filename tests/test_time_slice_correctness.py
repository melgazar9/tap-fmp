"""Correctness tests for time-slice streams.

Verifies two invariants critical for Singer state resumption:

1. When a window's `fetch_window` raises, no records from THAT window or any
   LATER windows are emitted by `get_records`. The exception propagates so
   Singer's per-record state-commit logic leaves the bookmark at the last
   fully-completed window. The next run resumes from the failed window.

2. When `fetch_window` hits the max-records limit at 1-day granularity, it
   emits a `*** DATA_TRUNCATED ***` ERROR log so a Dagster sensor can flag
   the partial window for refetch.

If either invariant breaks, time-slice streams will silently lose data —
the exact failure mode that produced "sec_filings_by_cik had EMPTY state
but 18M rows" historically.
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
        self._replication_key_starting_name = "from"
        self._replication_key_ending_name = "to"
        self.logger = logging.getLogger("tap-fmp.test_stream")
        self._fake_stream_config = {"other_params": {"max_records_per_request": 10}}

    @property
    def stream_config(self):
        return self._fake_stream_config


def _make_stream():
    return _StubTimeSliceStream()


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
