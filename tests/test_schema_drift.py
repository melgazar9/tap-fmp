"""FMP changes payloads without notice: a field appears, stops arriving, stops
being filled, or changes type. Every sync must name those fields, and must name
them near the end of its log, because stream logs keep only their tail.
"""

from __future__ import annotations

import json
import logging
from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal

import pytest

from tap_fmp.schema_drift import SchemaDriftMonitor
from tap_fmp.streams.calendar_streams import EarningsCalendarStream
from tap_fmp.streams.forex_streams import ForexPairsStream
from tests.test_quota_exhaustion import _ScriptedSession
from tests.test_statement_periods import _tap

# the response FMP's earnings-calendar docs show for the default request
DOCUMENTED_ROW = {
    "symbol": "GRG.L",
    "date": "2026-07-29",
    "epsActual": 0.549,
    "epsEstimated": 0.501,
    "revenueActual": 1101500000,
    "revenueEstimated": 1086300000,
    "lastUpdated": "2026-07-30",
}
REPORT_TIME_FIELDS = {
    "time": "amc",
    "periodEnding": "2026-06-30",
    "fiscalPeriod": "Q2",
    "fiscalYear": 2026,
    "confirmed": True,
}
COMPLETE_ROW = {**DOCUMENTED_ROW, **REPORT_TIME_FIELDS}


@contextmanager
def _captured(logger: logging.Logger, caplog):
    """Building a tap reconfigures logging, which drops caplog's handler from the root logger."""
    logger.addHandler(caplog.handler)
    try:
        yield
    finally:
        logger.removeHandler(caplog.handler)


def _drift_lines(caplog) -> list[str]:
    return [
        r.getMessage()
        for r in caplog.records
        if SchemaDriftMonitor.TOKEN in r.getMessage()
    ]


def _sync_earnings_calendar(rows: list[dict], caplog) -> list[str]:
    tap = _tap(
        earnings_calendar={"query_params": {"from": "2026-07-28", "to": "2026-07-29"}}
    )
    stream = EarningsCalendarStream(tap)
    stream._requests_session = _ScriptedSession([(200, json.dumps(rows).encode())])
    with _captured(stream.logger, caplog):
        stream.sync()
    return _drift_lines(caplog)


def _final_line(lines: list[str]) -> str:
    assert lines, "the sync logged no drift"
    assert (
        "records=" in lines[-1]
    ), f"the last drift line is not the end-of-sync report: {lines[-1]}"
    return lines[-1]


def test_a_complete_payload_logs_no_drift(caplog):
    assert _sync_earnings_calendar([COMPLETE_ROW], caplog) == []


def test_an_empty_reply_says_nothing_about_the_schema(caplog):
    assert _sync_earnings_calendar([], caplog) == []


def test_fields_fmp_stopped_sending_are_named_at_the_end_of_the_sync(caplog):
    report = _final_line(_sync_earnings_calendar([DOCUMENTED_ROW], caplog))

    assert "records=1" in report
    assert (
        "absent_fields=['confirmed', 'fiscal_period', 'fiscal_year', 'period_ending', 'time']"
        in report
    )


def test_a_field_fmp_added_is_named_once_when_seen_and_again_at_the_end(caplog):
    row = {**COMPLETE_ROW, "reportUrl": "https://example.com"}

    lines = _sync_earnings_calendar(
        [row, {**row, "symbol": "AAA"}, {**row, "symbol": "BBB"}], caplog
    )

    assert sum("new_field=report_url" in line for line in lines) == 1
    assert "new_fields=['report_url']" in _final_line(lines)


def test_a_new_field_sent_as_null_is_still_new(caplog):
    lines = _sync_earnings_calendar([{**COMPLETE_ROW, "reportUrl": None}], caplog)

    assert "new_fields=['report_url']" in _final_line(lines)


def test_a_value_whose_type_changed_is_named_once_when_seen_and_again_at_the_end(
    caplog,
):
    row = {**COMPLETE_ROW, "epsActual": "0.549"}

    lines = _sync_earnings_calendar([row, {**row, "symbol": "AAA"}], caplog)

    assert sum("changed_type=eps_actual:number->str" in line for line in lines) == 1
    assert "changed_types=['eps_actual:number->str']" in _final_line(lines)


def test_a_bool_is_not_a_number(caplog):
    lines = _sync_earnings_calendar([{**COMPLETE_ROW, "revenueActual": True}], caplog)

    assert "changed_types=['revenue_actual:number->bool']" in _final_line(lines)


def test_a_field_fmp_still_sends_but_never_fills_is_named(caplog):
    report = _final_line(
        _sync_earnings_calendar([{**COMPLETE_ROW, "revenueActual": None}], caplog)
    )

    assert "always_null_fields=['revenue_actual']" in report
    assert "absent_fields=[]" in report


def test_a_field_filled_in_only_some_rows_is_not_drift(caplog):
    rows = [{**COMPLETE_ROW, "epsActual": None}, {**COMPLETE_ROW, "symbol": "AAA"}]

    assert _sync_earnings_calendar(rows, caplog) == []


@pytest.mark.parametrize(
    "declared,value",
    [
        ({"type": ["string", "null"], "format": "date"}, date(2026, 11, 5)),
        (
            {"type": ["string", "null"], "format": "date-time"},
            datetime(2026, 11, 5, 9, 30),
        ),
        ({"type": ["number", "null"]}, Decimal("1.5")),
        ({"type": ["number", "null"]}, 12),
        ({"type": ["integer", "null"]}, 12),
        ({"type": "string"}, "text"),
        ({"type": ["object", "null"]}, {"nested": 1}),
        ({"type": ["array", "null"]}, [1]),
        ({"anyOf": [{"type": "string"}, {"type": "number"}]}, 1.5),
    ],
)
def test_values_a_stream_converts_itself_are_not_type_changes(declared, value, caplog):
    logger = logging.getLogger("schema_drift_test")
    monitor = SchemaDriftMonitor(
        "bulk_stream", {"properties": {"field": declared}}, logger
    )

    with _captured(logger, caplog):
        monitor.observe({"field": value})
        monitor.report()

    assert _drift_lines(caplog) == []


def test_records_built_from_config_are_not_judged_as_fmp_payloads(caplog):
    stream = ForexPairsStream(_tap(forex_pairs={"select_forex_pairs": ["EURUSD"]}))

    with _captured(stream.logger, caplog):
        stream.sync()

    assert _drift_lines(caplog) == []


def test_every_stream_schema_can_be_monitored():
    for stream in _tap().discover_streams():
        SchemaDriftMonitor(stream.name, stream.schema, stream.logger)
