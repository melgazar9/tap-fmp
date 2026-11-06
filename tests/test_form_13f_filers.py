"""The 13F filer streams ask about known filers, not every SEC entity. A filer that
any 13F table has seen stays on the list, and a feed that would quietly shorten the
list stops the run instead.
"""

from __future__ import annotations

import logging
import typing as t
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import psycopg2
import pytest

from tap_fmp.helpers import Form13fFilerRegistry
from tap_fmp.streams.company_streams import CikProfileStream
from tap_fmp.streams.form_13f_streams import (
    FilingsExtractStream,
    Form13fFilingDatesStream,
    HolderIndustryBreakdownStream,
    HolderPerformanceSummaryStream,
)
from tap_fmp.tap import TapFMP
from tests.test_statement_periods import _tap


class _Warehouse:
    """Answers the registry's queries the way the warehouse would."""

    DB_CONFIG: t.ClassVar[dict] = {
        "host": "h",
        "database": "d",
        "username": "u",
        "password": "p",
        "schema": "tap_fmp_production",
    }

    def __init__(
        self, newest_filing, ciks=("0000000001",), tables=Form13fFilerRegistry.TABLES
    ):
        self.tables = tables
        self.newest_filing = newest_filing
        self.ciks = ciks
        self.queries: list[tuple[str, tuple | None]] = []
        self.connect_kwargs: dict = {}
        self._rows: list[tuple] = []

    def execute(self, query, params=None):
        text = repr(query)
        self.queries.append((text, params))
        if "information_schema" in text:
            self._rows = [(table,) for table in self.tables]
        elif "max(" in text.lower():
            self._rows = [(self.newest_filing,)]
        else:
            self._rows = [(cik,) for cik in self.ciks]

    def fetchone(self):
        return self._rows[0]

    def fetchall(self):
        return self._rows

    def connect(self, **kwargs):
        self.connect_kwargs = kwargs
        connection = MagicMock()
        connection.__enter__.return_value = connection
        connection.cursor.return_value.__enter__.return_value = self
        return connection


def _load(warehouse: _Warehouse, logger=None) -> list[str]:
    registry = Form13fFilerRegistry(
        _Warehouse.DB_CONFIG, logger or logging.getLogger("test")
    )
    with patch("tap_fmp.helpers.psycopg2.connect", side_effect=warehouse.connect):
        return registry.load_ciks()


def _days_old(days: int) -> datetime:
    return datetime.now() - timedelta(days=days)


def test_the_list_is_the_union_of_every_13f_table():
    warehouse = _Warehouse(_days_old(1))

    _load(warehouse)

    union = warehouse.queries[-1][0]
    for table in Form13fFilerRegistry.TABLES:
        assert table in union
    assert "' UNION '" in union


def test_the_registry_connects_and_looks_up_tables_with_the_configured_login():
    warehouse = _Warehouse(_days_old(1))

    _load(warehouse)

    assert warehouse.connect_kwargs["user"] == "u"
    assert warehouse.queries[0][1] == (
        "tap_fmp_production",
        list(Form13fFilerRegistry.TABLES),
    )


def test_the_list_is_sorted():
    warehouse = _Warehouse(_days_old(1), ("0000000002", "0000000001"))

    assert _load(warehouse) == ["0000000001", "0000000002"]


def test_a_padding_difference_cannot_split_one_filer_in_two():
    warehouse = _Warehouse(_days_old(1), ("123", "0000000123"))

    assert _load(warehouse) == ["0000000123"]


@pytest.mark.parametrize("value", ["", "12345678901", "ABC123", "12 34"])
def test_a_stored_value_that_is_not_a_cik_stops_the_run(value):
    warehouse = _Warehouse(_days_old(1), ("0000000001", value))

    with pytest.raises(RuntimeError, match="not CIKs"):
        _load(warehouse)


def test_a_missing_feed_stops_the_run_and_names_the_stream_that_fixes_it():
    tables = Form13fFilerRegistry.TABLES[1:]

    with pytest.raises(RuntimeError, match=Form13fFilerRegistry.FEED_TABLE):
        _load(_Warehouse(_days_old(1), tables=tables))


def test_an_empty_feed_stops_the_run():
    with pytest.raises(RuntimeError, match="no filings"):
        _load(_Warehouse(None))


def test_a_feed_past_the_age_limit_stops_the_run_rather_than_dropping_new_filers():
    stale = _days_old(Form13fFilerRegistry.MAX_FEED_AGE_DAYS + 1)

    with pytest.raises(RuntimeError, match="stale"):
        _load(_Warehouse(stale))


def test_a_feed_exactly_at_the_age_limit_is_accepted():
    at_limit = _days_old(Form13fFilerRegistry.MAX_FEED_AGE_DAYS)

    assert _load(_Warehouse(at_limit)) == ["0000000001"]


def test_no_ciks_at_all_stops_the_run():
    with pytest.raises(RuntimeError, match="no CIKs"):
        _load(_Warehouse(_days_old(1), ()))


def test_incomplete_database_config_stops_the_run_without_connecting():
    with patch("tap_fmp.helpers.psycopg2.connect") as connect:
        registry = Form13fFilerRegistry({"host": "h"}, logging.getLogger("test"))
        with pytest.raises(RuntimeError, match="database_config"):
            registry.load_ciks()

    connect.assert_not_called()


def test_a_database_error_is_never_swallowed():
    registry = Form13fFilerRegistry(_Warehouse.DB_CONFIG, logging.getLogger("test"))

    with patch(
        "tap_fmp.helpers.psycopg2.connect",
        side_effect=psycopg2.OperationalError("connection refused"),
    ):
        with pytest.raises(psycopg2.OperationalError):
            registry.load_ciks()


def test_a_table_not_loaded_yet_is_skipped_and_named():
    tables = [Form13fFilerRegistry.FEED_TABLE, "holder_performance_summary"]
    logger = MagicMock()
    warehouse = _Warehouse(_days_old(1), tables=tables)

    assert _load(warehouse, logger=logger) == ["0000000001"]

    assert "form_13f_filing_dates" not in warehouse.queries[-1][0]
    assert "form_13f_filing_dates" in logger.warning.call_args.args[0]


@pytest.mark.parametrize(
    "stream_class",
    [
        FilingsExtractStream,
        HolderIndustryBreakdownStream,
        HolderPerformanceSummaryStream,
    ],
)
def test_the_filer_streams_ask_only_known_filers(stream_class):
    filers = ["0000000001", "0000000002"]

    with (
        patch.object(TapFMP, "get_cached_13f_filer_ciks", return_value=filers),
        patch.object(
            TapFMP,
            "get_cached_ciks",
            side_effect=AssertionError("asked every SEC entity"),
        ),
    ):
        partitions = stream_class(_tap()).partitions

    assert {p["cik"] for p in partitions} == set(filers)


@pytest.mark.parametrize("stream_class", [Form13fFilingDatesStream, CikProfileStream])
def test_every_other_cik_stream_still_asks_the_full_list(stream_class):
    everyone = [{"cik": "0000000009"}]

    with (
        patch.object(TapFMP, "get_cached_ciks", return_value=everyone),
        patch.object(
            TapFMP,
            "get_cached_13f_filer_ciks",
            side_effect=AssertionError("filter reached a stream it was not asked to"),
        ),
    ):
        partitions = stream_class(_tap()).partitions

    assert {p["cik"] for p in partitions} == {"0000000009"}


def test_a_cik_selection_in_config_wins_and_reads_no_database():
    tap = _tap(ciks={"select_ciks": ["0000000123"]})

    with patch("tap_fmp.helpers.psycopg2.connect") as connect:
        ciks = tap.get_cached_13f_filer_ciks()

    connect.assert_not_called()
    assert ciks == ["0000000123"]


def test_the_warehouse_is_read_once_per_process():
    tap = _tap(database_config=_Warehouse.DB_CONFIG)
    warehouse = _Warehouse(_days_old(1))

    with patch("tap_fmp.helpers.psycopg2.connect", side_effect=warehouse.connect) as c:
        first = tap.get_cached_13f_filer_ciks()
        second = tap.get_cached_13f_filer_ciks()

    assert first == second == ["0000000001"]
    assert c.call_count == 1
