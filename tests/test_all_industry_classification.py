"""FMP pages the company→SIC directory only at page 0: every later page answers
with the whole directory whatever the limit, so paging on would never end."""

from __future__ import annotations

import json

import pytest

from tap_fmp.streams.sec_filings_streams import AllIndustryClassificationStream
from tap_fmp.tap import TapFMP
from tests.test_quota_exhaustion import _ScriptedSession


class _RecordingSession(_ScriptedSession):
    """A scripted session that keeps each request's params."""

    def __init__(self, script: list[tuple[int, bytes]]):
        super().__init__(script)
        self.params: list[dict] = []

    def get(self, url, params=None, timeout=None):
        self.params.append(dict(params or {}))
        return super().get(url, params=params, timeout=timeout)


def _rows(first: int, count: int) -> bytes:
    return json.dumps(
        [
            {"symbol": f"S{n}", "cik": f"{n:010d}", "sicCode": "6021"}
            for n in range(first, first + count)
        ]
    ).encode()


def _symbols(count: int) -> list[str]:
    return sorted(f"S{n}" for n in range(count))


@pytest.fixture
def answering(monkeypatch):
    """The stream, with each GET answered from `script`, a page holding three rows."""
    monkeypatch.setattr(AllIndustryClassificationStream, "_rows_per_page", 3)

    def stream_answering(script):
        session = _RecordingSession(script)
        monkeypatch.setattr(
            AllIndustryClassificationStream,
            "requests_session",
            property(lambda self: session),
        )
        stream = TapFMP(
            config={"api_key": "unused", "start_date": "1990-01-01T00:00:00Z"},
            parse_env_config=False,
            validate_config=False,
        ).streams["all_industry_classification"]
        return stream, session

    return stream_answering


def test_the_whole_directory_past_page_zero_is_read_once(answering):
    stream, session = answering([(200, _rows(0, 3)), (200, _rows(0, 8))])

    records = list(stream.get_records(None))

    assert sorted(record["symbol"] for record in records) == _symbols(8)
    assert session.calls == 2


def test_pages_are_read_until_one_comes_back_short(answering):
    stream, session = answering(
        [(200, _rows(0, 3)), (200, _rows(3, 3)), (200, _rows(6, 1))]
    )

    records = list(stream.get_records(None))

    assert sorted(record["symbol"] for record in records) == _symbols(7)
    assert [params["page"] for params in session.params] == [0, 1, 2]
    assert {params["limit"] for params in session.params} == {3}


def test_a_directory_that_never_ends_stops_the_stream(answering, monkeypatch):
    monkeypatch.setattr(AllIndustryClassificationStream, "_max_pages", 2)
    stream, session = answering([(200, _rows(0, 3))])

    with pytest.raises(RuntimeError, match="never ended"):
        list(stream.get_records(None))
    assert session.calls == 3
