"""FMP answers one period kind per request and defaults to annual, so a
statement stream that asks once per symbol never sees a quarter. Statement
streams ask once per symbol per kind, over the statement-symbol universe, and
keep the fiscal period FMP labels each row with.
"""

from __future__ import annotations

from unittest.mock import patch

from tap_fmp.streams.analyst_streams import AnalystEstimatesStream
from tap_fmp.streams.statements_streams import (
    FinancialReportsForm10kJsonStream,
    IncomeStatementStream,
)
from tap_fmp.tap import TapFMP

STATEMENT_SYMBOLS = [{"symbol": "AAPL"}, {"symbol": "GEV"}]


def _tap(**stream_config) -> TapFMP:
    return TapFMP(
        config={
            "api_key": "unused",
            "start_date": "1990-01-01T00:00:00Z",
            **stream_config,
        },
        parse_env_config=False,
        validate_config=False,
    )


def _partitions(stream_cls, tap: TapFMP) -> list[dict]:
    with patch.object(
        TapFMP,
        "get_cached_financial_statement_symbols",
        return_value=STATEMENT_SYMBOLS,
    ):
        return stream_cls(tap).partitions


def test_statements_request_each_symbol_annually_and_quarterly():
    assert _partitions(IncomeStatementStream, _tap()) == [
        {"symbol": "AAPL", "period": "annual"},
        {"symbol": "AAPL", "period": "quarter"},
        {"symbol": "GEV", "period": "annual"},
        {"symbol": "GEV", "period": "quarter"},
    ]


def test_a_configured_period_narrows_the_request():
    tap = _tap(income_statement={"query_params": {"period": "quarter"}})

    assert _partitions(IncomeStatementStream, tap) == [
        {"symbol": "AAPL", "period": "quarter"},
        {"symbol": "GEV", "period": "quarter"},
    ]


def test_the_row_keeps_fmps_fiscal_period_over_the_requested_kind():
    stream = IncomeStatementStream(_tap())
    row = {"symbol": "AAPL", "date": "2026-06-27", "period": "Q3", "revenue": 1.0}

    processed = stream.post_process(row, {"symbol": "AAPL", "period": "quarter"})

    assert processed["period"] == "Q3"


def test_analyst_estimates_ask_for_both_kinds_only():
    with patch.object(
        TapFMP, "get_cached_company_symbols", return_value=[{"symbol": "AAPL"}]
    ):
        partitions = AnalystEstimatesStream(_tap()).partitions

    assert [p["period"] for p in partitions] == ["annual", "quarter"]


def test_the_10k_defaults_to_the_annual_report():
    assert FinancialReportsForm10kJsonStream._get_periods(None) == ["FY"]
