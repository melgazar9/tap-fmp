"""sec_filings_by_cik fetches the CIKs after the one being written on worker threads. The SDK
must still see what a one-at-a-time run gives it: the same records in the same order, the same
bookmarks, every CIK requested from its own bookmark, nothing written past a CIK whose fetch failed,
and memory that stays bounded however far the workers run ahead.
"""

from __future__ import annotations

import io
import json
import logging
import random
import threading
import time
import typing as t
from contextlib import redirect_stdout
from copy import deepcopy

import pytest
import requests

from tap_fmp.client import ConcurrentPartitionTimeSliceStream, FmpErrorReplyError
from tap_fmp.prefetch import PartitionPlan, PartitionPrefetcher
from tap_fmp.tap import TapFMP


class _FakeFmp:
    """sec-filings-search/cik from rows per CIK: an inclusive filing-date window, `limit` per page."""

    def __init__(self, rows_by_cik, *, seconds_per_request=0.0, error_ciks=()):
        self._rows_by_cik = rows_by_cik
        self._seconds_per_request = seconds_per_request
        self._error_ciks = set(error_ciks)
        self._lock = threading.Lock()
        self.asked: list[tuple[str, str, str, int]] = []
        self.in_flight = 0
        self.most_in_flight = 0

    def mount(self, prefix, adapter):
        pass

    def get(self, url, params=None, timeout=None):
        cik, first, last, page = (
            params["cik"],
            params["from"],
            params["to"],
            params["page"],
        )
        with self._lock:
            self.asked.append((cik, first, last, page))
            self.in_flight += 1
            self.most_in_flight = max(self.most_in_flight, self.in_flight)
        try:
            time.sleep(self._seconds_per_request)
            inside = [
                r
                for r in self._rows_by_cik.get(cik, [])
                if first <= r["filingDate"][:10] <= last
            ]
            limit = int(params["limit"])
            body = (
                {"Error Message": "Something went wrong on our side."}
                if cik in self._error_ciks
                else inside[page * limit : (page + 1) * limit]
            )
            response = requests.Response()
            response.status_code = 200
            response._content = json.dumps(body).encode()
            response.request = requests.Request("GET", url, params=params).prepare()
            return response
        finally:
            with self._lock:
                self.in_flight -= 1


class _CikRun:
    """A whole tap run of sec_filings_by_cik over made-up filers, with its Singer output."""

    STREAM: t.ClassVar[str] = "sec_filings_by_cik"
    # rows per page; small, so a few hundred rows overflow the page ceiling and split
    PAGE_SIZE: t.ClassVar[int] = 5

    @staticmethod
    def ciks(n: int) -> list[str]:
        return [f"{1000 + i:010d}" for i in range(n)]

    @staticmethod
    def filer_rows(cik: str, seed: int) -> list[dict]:
        rng = random.Random(seed)
        rows = []
        for n in range(rng.choice([0, 0, 2, 4, 9, 30, 160])):
            day = f"{rng.randrange(1994, 2027)}-{rng.randrange(1, 13):02d}-{rng.randrange(1, 29):02d}"
            rows.append(
                {
                    "symbol": "",
                    "cik": cik,
                    "filingDate": f"{day} 00:00:00",
                    "acceptedDate": f"{day} 16:05:00",
                    "formType": rng.choice(["4", "8-K", "497K"]),
                    "link": f"https://www.sec.gov/{cik}/{n}-index.htm",
                    "finalLink": f"https://www.sec.gov/{cik}/{n}.htm",
                }
            )
        return sorted(rows, key=lambda r: r["acceptedDate"], reverse=True)

    @classmethod
    def rows(cls, ciks: list[str]) -> dict[str, list[dict]]:
        return {cik: cls.filer_rows(cik, seed) for seed, cik in enumerate(ciks)}

    @classmethod
    def tap(
        cls,
        ciks,
        workers,
        state=None,
        *,
        start_date="1990-01-01T00:00:00Z",
        stream_from="1990-01-01",
        throttle=0.0,
    ) -> TapFMP:
        tap = TapFMP(
            config={
                "api_key": "unused",
                "start_date": start_date,
                "min_throttle_seconds": throttle,
                "ciks": {"select_ciks": ciks},
                cls.STREAM: {
                    "query_params": {"from": stream_from, "limit": cls.PAGE_SIZE},
                    "other_params": {"partition_workers": workers},
                },
            },
            state=deepcopy(state),
            parse_env_config=False,
            validate_config=False,
        )
        for name, stream in tap.streams.items():
            stream.selected = name == cls.STREAM
        return tap

    @classmethod
    def sync(cls, tap, fmp) -> list[dict]:
        """Singer messages a run writes, without the extraction time every record carries."""
        tap.streams[cls.STREAM]._requests_session = fmp
        out = io.StringIO()
        with redirect_stdout(out):
            tap.sync_all()
        messages = [json.loads(line) for line in out.getvalue().splitlines()]
        for message in messages:
            message.pop("time_extracted", None)
        return messages

    @staticmethod
    def records(messages) -> list[dict]:
        return [m["record"] for m in messages if m["type"] == "RECORD"]

    @classmethod
    def bookmarks(cls, tap) -> dict[str, dict]:
        partitions = tap.state["bookmarks"][cls.STREAM]["partitions"]
        return {
            p["context"]["cik"]: {k: v for k, v in p.items() if k != "context"}
            for p in partitions
        }

    @classmethod
    def saved_state(cls, bookmarks: dict[str, str]) -> dict:
        partitions = [
            {
                "context": {"cik": cik},
                "replication_key": "filing_date",
                "replication_key_value": value,
            }
            for cik, value in bookmarks.items()
        ]
        return {"bookmarks": {cls.STREAM: {"partitions": partitions}}}


@pytest.mark.parametrize("workers", [2, 6])
def test_fetching_ahead_writes_what_a_one_at_a_time_run_writes(workers):
    ciks = _CikRun.ciks(40)
    rows = _CikRun.rows(ciks)
    saved = _CikRun.saved_state(
        {ciks[3]: "2015-06-01 00:00:00", ciks[7]: "2030-01-01 00:00:00"}
    )

    one_at_a_time = _CikRun.tap(ciks, workers=1, state=saved)
    expected_fmp = _FakeFmp(rows)
    expected = _CikRun.sync(one_at_a_time, expected_fmp)
    ahead = _CikRun.tap(ciks, workers=workers, state=saved)
    fmp = _FakeFmp(rows)
    written = _CikRun.sync(ahead, fmp)

    assert _CikRun.records(written) == _CikRun.records(expected)
    assert len(_CikRun.records(written)) > 100
    assert ahead.state == one_at_a_time.state
    assert sorted(fmp.asked) == sorted(expected_fmp.asked)


def test_every_cik_is_asked_from_its_own_bookmark():
    ciks = _CikRun.ciks(6)
    saved = _CikRun.saved_state({ciks[1]: "2015-06-01 00:00:00"})
    fmp = _FakeFmp(_CikRun.rows(ciks))
    _CikRun.sync(_CikRun.tap(ciks, workers=3, state=saved), fmp)

    first_from = {}
    for cik, first, _, _ in fmp.asked:
        first_from.setdefault(cik, first)
    assert first_from[ciks[1]] == "2015-06-01"
    assert {first_from[c] for c in ciks if c != ciks[1]} == {"1990-01-01"}


def test_a_start_date_after_the_streams_own_start_is_the_floor_for_ciks_fetched_ahead():
    ciks = _CikRun.ciks(5)
    tap = _CikRun.tap(
        ciks, workers=3, start_date="2005-01-01T00:00:00Z", stream_from="1990-01-01"
    )
    fmp = _FakeFmp(_CikRun.rows(ciks))
    _CikRun.sync(tap, fmp)

    assert {first for _, first, _, _ in fmp.asked if first <= "2005-01-01"} == {
        "2005-01-01"
    }


def test_requests_for_different_ciks_overlap():
    ciks = _CikRun.ciks(12)
    fmp = _FakeFmp(_CikRun.rows(ciks), seconds_per_request=0.02)
    _CikRun.sync(_CikRun.tap(ciks, workers=4), fmp)

    assert fmp.most_in_flight > 1


def test_one_worker_fetches_each_cik_when_the_sdk_reaches_it():
    ciks = _CikRun.ciks(6)
    fmp = _FakeFmp(_CikRun.rows(ciks), seconds_per_request=0.01)
    _CikRun.sync(_CikRun.tap(ciks, workers=1), fmp)

    assert fmp.most_in_flight == 1


def test_workers_share_the_throttle_so_the_request_rate_is_unchanged():
    ciks = _CikRun.ciks(8)
    fmp = _FakeFmp({cik: [] for cik in ciks})
    tap = _CikRun.tap(ciks, workers=4, throttle=0.05)
    start = time.monotonic()
    _CikRun.sync(tap, fmp)

    assert len(fmp.asked) == 16
    assert time.monotonic() - start >= 15 * 0.05


def test_a_failed_cik_stops_the_run_with_every_earlier_cik_bookmarked_and_none_after():
    ciks = _CikRun.ciks(30)
    rows = _CikRun.rows(ciks)
    tap = _CikRun.tap(ciks, workers=4)

    with pytest.raises(FmpErrorReplyError):
        _CikRun.sync(tap, _FakeFmp(rows, error_ciks=[ciks[12]]))

    one_at_a_time = _CikRun.tap(ciks[:12], workers=1)
    _CikRun.sync(one_at_a_time, _FakeFmp(rows))
    bookmarks = _CikRun.bookmarks(tap)
    assert {c: bookmarks[c] for c in ciks[:12]} == _CikRun.bookmarks(one_at_a_time)
    assert all("replication_key_value" not in bookmarks.get(c, {}) for c in ciks[12:])


def test_records_of_a_cik_after_the_failed_one_are_never_written():
    ciks = _CikRun.ciks(30)
    rows = _CikRun.rows(ciks)
    tap = _CikRun.tap(ciks, workers=4)
    tap.streams[_CikRun.STREAM]._requests_session = _FakeFmp(
        rows, error_ciks=[ciks[12]]
    )
    out = io.StringIO()

    with redirect_stdout(out), pytest.raises(FmpErrorReplyError):
        tap.sync_all()

    written = {
        json.loads(line)["record"]["cik"]
        for line in out.getvalue().splitlines()
        if '"RECORD"' in line
    }
    assert written == {c for c in ciks[:12] if rows[c]}


def test_each_cik_is_synced_once_though_the_list_repeats_it():
    ciks = _CikRun.ciks(4)
    tap = _CikRun.tap(ciks + [ciks[1]], workers=2)

    assert tap.streams[_CikRun.STREAM].partitions == [{"cik": c} for c in ciks]


def test_state_messages_go_out_between_ciks_on_a_clock_not_per_record(monkeypatch):
    ciks = _CikRun.ciks(10)
    rows = _CikRun.rows(ciks)
    monkeypatch.setattr(
        ConcurrentPartitionTimeSliceStream, "STATE_MESSAGE_SECONDS", 0.0
    )
    every_cik = _CikRun.sync(_CikRun.tap(ciks, workers=3), _FakeFmp(rows))
    monkeypatch.setattr(
        ConcurrentPartitionTimeSliceStream, "STATE_MESSAGE_SECONDS", 3600.0
    )
    hourly = _CikRun.sync(_CikRun.tap(ciks, workers=3), _FakeFmp(rows))

    def states(messages):
        return [m["value"] for m in messages if m["type"] == "STATE"]

    assert len(states(every_cik)) > len(states(hourly))
    for state in states(every_cik):
        for partition in state["bookmarks"][_CikRun.STREAM]["partitions"]:
            assert "progress_markers" not in partition


def _prefetcher(
    contexts, fetch, *, workers=3, max_buffered_records=1000, max_queued_partitions=50
):
    return PartitionPrefetcher(
        contexts,
        plan=lambda context: PartitionPlan(context, None, {}, []),
        fetch=fetch,
        workers=workers,
        max_buffered_records=max_buffered_records,
        max_queued_partitions=max_queued_partitions,
        name="test",
        logger=logging.getLogger("tap-fmp.test"),
    )


def test_the_writer_reads_partitions_in_order_whichever_finishes_first():
    contexts = [{"n": n} for n in range(8)]

    def fetch(plan):
        time.sleep(0.01 * (8 - plan.context["n"]))
        yield [plan.context["n"]]

    prefetcher = _prefetcher(contexts, fetch, workers=8)

    assert [b for c in contexts for b in prefetcher.batches(c, None)] == [
        [n] for n in range(8)
    ]
    assert prefetcher.closed


def test_a_partition_asked_out_of_order_is_refused():
    contexts = [{"n": n} for n in range(3)]
    prefetcher = _prefetcher(contexts, lambda plan: iter([[1]]))

    with pytest.raises(RuntimeError, match="next fetched partition"):
        list(prefetcher.batches({"n": 1}, None))
    prefetcher.close()


def test_rows_held_for_partitions_not_yet_written_stay_under_the_cap():
    contexts = [{"n": n} for n in range(40)]
    held = []

    class _Watched(PartitionPrefetcher):
        def _hand_over(self, pages, batch):
            super()._hand_over(pages, batch)
            with self._cond:
                head = self._queued[0] if self._queued else None
                held.append(
                    sum(
                        len(b) for p in self._queued if p is not head for b in p.batches
                    )
                )

    def fetch(plan):
        for _ in range(5):
            yield list(range(10))

    prefetcher = _Watched(
        contexts,
        plan=lambda context: PartitionPlan(context, None, {}, []),
        fetch=fetch,
        workers=6,
        max_buffered_records=25,
        max_queued_partitions=40,
        name="test",
        logger=logging.getLogger("tap-fmp.test"),
    )
    for context in contexts:
        for _ in prefetcher.batches(context, None):
            time.sleep(0.001)

    assert held and max(held) <= 25


def test_a_partition_bigger_than_the_cap_is_still_written_whole():
    contexts = [{"n": n} for n in range(3)]

    def fetch(plan):
        for _ in range(6):
            yield [plan.context["n"]] * 10

    prefetcher = _prefetcher(contexts, fetch, max_buffered_records=5)

    for context in contexts:
        assert sum(len(b) for b in prefetcher.batches(context, None)) == 60
