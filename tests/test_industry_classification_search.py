"""FMP refuses an industry classification search without a value, so each request
carries one: the value set in config, else each SIC code FMP lists."""

from __future__ import annotations

import json

import pytest

from tap_fmp.streams.sec_filings_streams import IndustryClassificationSearchStream
from tap_fmp.tap import TapFMP
from tests.test_all_industry_classification import _RecordingSession


def _companies(sic_code: str, count: int) -> bytes:
    return json.dumps(
        [
            {"symbol": f"{sic_code}-{n}", "cik": f"{n:010d}", "sicCode": sic_code}
            for n in range(count)
        ]
    ).encode()


@pytest.fixture
def search(monkeypatch):
    """The stream, its tap listing `sic_codes`, each GET answered from `script`."""

    def stream_answering(script, sic_codes=(), query_params=None):
        session = _RecordingSession(script)
        monkeypatch.setattr(
            IndustryClassificationSearchStream,
            "requests_session",
            property(lambda self: session),
        )
        stream_config = {"query_params": query_params} if query_params else {}
        tap = TapFMP(
            config={
                "api_key": "unused",
                "start_date": "1990-01-01T00:00:00Z",
                "industry_classification_search": stream_config,
            },
            parse_env_config=False,
            validate_config=False,
        )
        tap._cached_sic_codes = [{"sic_code": code} for code in sic_codes]
        return tap.streams["industry_classification_search"], session

    return stream_answering


def _records(stream) -> list[dict]:
    return [
        record
        for context in stream.partitions or [None]
        for record in stream.get_records(context)
    ]


def test_every_listed_sic_code_is_searched(search):
    stream, session = search(
        [(200, _companies("1000", 2)), (200, _companies("6021", 3))],
        sic_codes=("1000", "6021"),
    )

    records = _records(stream)

    assert [params["sicCode"] for params in session.params] == ["1000", "6021"]
    assert len(records) == 5


def test_a_configured_search_value_is_searched_alone(search):
    stream, session = search(
        [(200, _companies("6021", 1))],
        sic_codes=("1000", "6021"),
        query_params={"symbol": "BAC"},
    )

    records = _records(stream)

    assert stream.partitions is None
    assert [params["symbol"] for params in session.params] == ["BAC"]
    assert "sicCode" not in session.params[0]
    assert len(records) == 1
