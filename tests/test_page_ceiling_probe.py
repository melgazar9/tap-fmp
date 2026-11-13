"""A paged window that overflows the page ceiling is split in half and both halves are fetched
again, so the pages it served before the split are thrown away. Asking the ceiling page as soon
as an overflow is likely finds that out without the pages in between. The windows, the rows
served and the truncation alerts must stay exactly what paging through every page gives.
"""

from __future__ import annotations

import logging
import random
from datetime import date, timedelta
from unittest.mock import patch

import pytest

from tap_fmp.client import DATA_TRUNCATED_TOKEN, TimeSliceStream
from tap_fmp.streams.sec_filings_streams import (
    Latest8KFilingsStream,
    LatestSecFilingsStream,
    SecFilingsByCikStream,
    SecFilingsByFormTypeStream,
    SecFilingsBySymbolStream,
)

PAGE_SIZE = 5


class _PagedWindowStream(TimeSliceStream):
    """Test-only subclass that bypasses Singer SDK construction."""

    name = "test_stream"
    schema = {"properties": {"date": {"type": "string"}}}
    replication_method = "FULL_TABLE"
    _paginate = True
    _sparse_history = True
    _max_pages = 3

    def __init__(self, probe: bool, max_pages: int = 3):
        self.query_params = {}
        self.path_params = {}
        self.other_params = {}
        self._probe_page_ceiling = probe
        self._max_pages = max_pages
        self.logger = logging.getLogger("tap-fmp.test_stream")

    @property
    def config(self):
        return {}


class _FakeFmp:
    """FMP's paged reply: the rows dated inside the window, `limit` per page, and a log of every page asked."""

    def __init__(self, rows: list[dict]):
        self.rows = rows
        self.asked: list[tuple[str, str, int]] = []
        self.rows_served = 0

    def __call__(self, url, query_params, page=None):
        self.asked.append((query_params["from"], query_params["to"], page))
        inside = [
            row
            for row in self.rows
            if query_params["from"] <= row["date"] <= query_params["to"]
        ]
        limit = int(query_params.get("limit", PAGE_SIZE))
        served = inside[page * limit : (page + 1) * limit]
        self.rows_served += len(served)
        return served


def _fetch(stream, rows, first, last, caplog, *, limit=PAGE_SIZE):
    fmp = _FakeFmp(rows)
    query_params = {} if limit is None else {"limit": limit}
    with (
        patch.object(stream, "_fetch_with_retry", side_effect=fmp),
        caplog.at_level(logging.ERROR, logger="tap-fmp.test_stream"),
    ):
        out = list(stream.fetch_window("http://x", query_params, first, last, 10))
    truncations = [
        r.message for r in caplog.records if DATA_TRUNCATED_TOKEN in r.message
    ]
    caplog.clear()
    return out, truncations, fmp


def _filings(days_and_counts: dict[str, int]) -> list[dict]:
    return [
        {"date": day, "id": f"{day}#{n}"}
        for day, count in sorted(days_and_counts.items(), reverse=True)
        for n in range(count)
    ]


def _random_filer(seed: int) -> list[dict]:
    """A filer's history: quiet years, busy years and the odd day too big for one window."""
    rng = random.Random(seed)
    first = date(1994, 1, 1)
    counts: dict[str, int] = {}
    for _ in range(rng.choice([0, 3, 40, 150, 600])):
        day = first + timedelta(days=rng.randrange(0, 365 * 30))
        counts[day.isoformat()] = counts.get(day.isoformat(), 0) + rng.choice(
            [1, 1, 2, 5, 9]
        )
    if rng.random() < 0.2:
        counts["2011-03-15"] = 40  # one day past the ceiling: unsplittable
    return _filings(counts)


@pytest.mark.parametrize("seed", range(40))
def test_probing_serves_exactly_what_paging_through_serves(seed, caplog):
    rows = _random_filer(seed)

    paged, paged_alerts, _ = _fetch(
        _PagedWindowStream(probe=False), rows, "1990-01-01", "2026-11-13", caplog
    )
    probed, probed_alerts, _ = _fetch(
        _PagedWindowStream(probe=True), rows, "1990-01-01", "2026-11-13", caplog
    )

    assert probed == paged
    assert probed_alerts == paged_alerts


def test_an_overflowing_window_skips_the_pages_before_its_ceiling(caplog):
    rows = _filings({f"2024-01-{d:02d}": 1 for d in range(1, 31)})

    _, _, fmp = _fetch(
        _PagedWindowStream(probe=True), rows, "2024-01-01", "2024-01-31", caplog
    )

    assert [
        page for f, t, page in fmp.asked if (f, t) == ("2024-01-01", "2024-01-31")
    ] == [0, 3]


def test_a_half_of_an_overflowed_window_asks_its_ceiling_first(caplog):
    rows = _filings({f"2024-01-{d:02d}": 1 for d in range(1, 31)})

    _, _, fmp = _fetch(
        _PagedWindowStream(probe=True), rows, "2024-01-01", "2024-01-31", caplog
    )

    first_ask = {}
    for f, t, page in fmp.asked:
        first_ask.setdefault((f, t), page)
    halves = [w for w in first_ask if w != ("2024-01-01", "2024-01-31")]
    assert halves and all(first_ask[w] == 3 for w in halves)


def test_probing_downloads_fewer_rows_and_asks_fewer_pages_on_a_big_filer(caplog):
    rows = _filings(
        {(date(2000, 1, 1) + timedelta(days=7 * i)).isoformat(): 3 for i in range(900)}
    )

    paged, _, paged_fmp = _fetch(
        _PagedWindowStream(probe=False, max_pages=20),
        rows,
        "1990-01-01",
        "2026-11-13",
        caplog,
    )
    probed, _, probed_fmp = _fetch(
        _PagedWindowStream(probe=True, max_pages=20),
        rows,
        "1990-01-01",
        "2026-11-13",
        caplog,
    )

    assert probed == paged
    assert len(probed_fmp.asked) < len(paged_fmp.asked)
    assert probed_fmp.rows_served < paged_fmp.rows_served


def test_a_window_that_fits_asks_its_ceiling_page_once_and_keeps_every_row(caplog):
    rows = _filings({f"2024-01-{d:02d}": 1 for d in range(1, 8)})

    out, alerts, fmp = _fetch(
        _PagedWindowStream(probe=True, max_pages=6),
        rows,
        "2024-01-01",
        "2024-01-31",
        caplog,
    )

    assert [page for _, _, page in fmp.asked] == [0, 6, 1, 2, 3]
    assert out == rows
    assert alerts == []


def test_a_short_first_page_never_asks_the_ceiling(caplog):
    rows = _filings({"2024-01-02": 1, "2024-01-03": 2})

    _, _, fmp = _fetch(
        _PagedWindowStream(probe=True), rows, "2024-01-01", "2024-01-31", caplog
    )

    assert [page for _, _, page in fmp.asked] == [0, 1, 2]


def test_without_a_limit_a_full_page_cannot_be_told_so_it_never_asks_the_ceiling(
    caplog,
):
    rows = _filings({f"2024-01-{d:02d}": 1 for d in range(1, 8)})

    _, _, fmp = _fetch(
        _PagedWindowStream(probe=True, max_pages=6),
        rows,
        "2024-01-01",
        "2024-01-31",
        caplog,
        limit=None,
    )

    assert 6 not in [page for _, _, page in fmp.asked]


def test_a_one_day_window_past_the_ceiling_pages_through_and_keeps_its_rows(caplog):
    rows = _filings({"2024-03-29": 30})

    out, alerts, fmp = _fetch(
        _PagedWindowStream(probe=True), rows, "2024-03-29", "2024-03-30", caplog
    )

    assert [page for _, _, page in fmp.asked] == [0, 1, 2, 3]
    assert len(out) == 20
    assert len(alerts) == 1 and "reason=hit_page_ceiling_with_data" in alerts[0]


def test_whole_history_cik_windows_probe_and_firehose_windows_do_not():
    assert SecFilingsByCikStream._probe_page_ceiling is True
    for stream_cls in (
        Latest8KFilingsStream,
        LatestSecFilingsStream,
        SecFilingsByFormTypeStream,
        SecFilingsBySymbolStream,
    ):
        assert stream_cls._probe_page_ceiling is False
