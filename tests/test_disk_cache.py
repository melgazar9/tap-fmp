"""The disk cache shares one run's raw API replies across its stream processes.
It must never turn an upstream failure into a silent retry, must fail fast on a
directory it can't write, and a universe is filtered after the cache, so a
cached reply can't carry another run's selection or country filter.
"""

from __future__ import annotations

import threading
from unittest.mock import patch

import pytest
import requests

from tap_fmp.disk_cache import DiskCache
from tap_fmp.tap import TapFMP
from tests.test_statement_periods import _tap


class _Fetch:
    def __init__(self, *results):
        self._results = list(results)
        self.calls = 0

    def __call__(self):
        self.calls += 1
        result = self._results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


class _UniverseStream:
    """The list stream a universe comes from: its API reply, or a selection written in config."""

    name = "company_symbols"
    query_params: dict = {}
    other_params: dict = {}

    def __init__(self, records, selected=None):
        self._records, self._selected = records, selected
        self.fetches = 0

    def get_records(self, context):
        self.fetches += 1
        return iter(self._records)

    def get_selected_items_list(self):
        return self._selected


def _universe(tap: TapFMP, stream: _UniverseStream, filtered: bool = False):
    tap._test_universe = None
    return tap._get_cached_data(
        {
            "cache_attr": "_test_universe",
            "lock": threading.Lock(),
            "stream_getter": lambda: stream,
            "data_type": "company symbols",
            "sort_key": "symbol",
            "apply_filtering": filtered,
            "filter_config_key": "company_symbols",
        }
    )


def test_a_fetch_failure_propagates_once_instead_of_refetching(tmp_path):
    fetch = _Fetch(requests.exceptions.HTTPError("503 Service Unavailable"))

    with pytest.raises(requests.exceptions.HTTPError):
        DiskCache(str(tmp_path), "tap_fmp").get_or_fetch("symbols/x", fetch)

    assert fetch.calls == 1


def test_a_cached_reply_is_served_without_fetching(tmp_path):
    DiskCache(str(tmp_path), "tap_fmp").get_or_fetch(
        "symbols/x", _Fetch([{"symbol": "A"}])
    )
    fetch = _Fetch()

    assert DiskCache(str(tmp_path), "tap_fmp").get_or_fetch("symbols/x", fetch) == [
        {"symbol": "A"}
    ]
    assert fetch.calls == 0


def test_an_unwritable_cache_dir_fails_fast(tmp_path):
    blocker = tmp_path / "file"
    blocker.write_text("not a directory")

    with pytest.raises(OSError):
        DiskCache(str(blocker), "tap_fmp")


def test_a_filesystem_without_flock_fetches_directly(tmp_path):
    fetch = _Fetch([{"symbol": "A"}])

    with patch("tap_fmp.disk_cache.fcntl.flock", side_effect=OSError("no locks")):
        assert DiskCache(str(tmp_path), "tap_fmp").get_or_fetch("symbols/x", fetch) == [
            {"symbol": "A"}
        ]


def test_a_selection_written_in_config_never_touches_the_disk_cache(tmp_path):
    tap = _tap()
    tap._disk_cache = DiskCache(str(tmp_path), "tap_fmp")
    _universe(tap, _UniverseStream([{"symbol": "AAPL"}, {"symbol": "MSFT"}], ["AAPL"]))

    full = _UniverseStream([{"symbol": "AAPL"}, {"symbol": "MSFT"}])
    assert [s["symbol"] for s in _universe(_tap(), full)] == ["AAPL", "MSFT"]


def test_the_filter_runs_after_the_cache_so_a_cached_reply_is_filtered_anew(tmp_path):
    reply = [{"symbol": "AAPL"}, {"symbol": "VOD.L"}]
    first = _tap()
    first._disk_cache = DiskCache(str(tmp_path), "tap_fmp")
    _universe(first, _UniverseStream(reply))

    second = _tap()
    second._disk_cache = DiskCache(str(tmp_path), "tap_fmp")
    stream = _UniverseStream(reply)

    def us_only(symbols, config_key):
        return [s for s in symbols if "." not in s["symbol"]]

    with patch.object(TapFMP, "_apply_profile_filtering", side_effect=us_only):
        kept = _universe(second, stream, filtered=True)

    assert [s["symbol"] for s in kept] == ["AAPL"]
    assert stream.fetches == 0


def test_an_empty_api_reply_stops_the_run():
    with pytest.raises(RuntimeError, match="FMP returned no company symbols"):
        _universe(_tap(), _UniverseStream([]))
