"""A transcript backfill names its companies: a configured list replaces FMP's
transcript symbol list, so a run fills the holes of those companies only."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from tap_fmp.client import FmpSurrogateKeyStream
from tap_fmp.tap import TapFMP


class TranscriptBackfill:
    """A tap configured to backfill the named companies' transcripts."""

    SYMBOLS = ["NEP", "JBT"]

    def __init__(self, **stream_config):
        self.tap = TapFMP(
            config={
                "api_key": "unused",
                "start_date": "1990-01-01T00:00:00Z",
                "transcript_symbols": {"select_transcript_symbols": self.SYMBOLS},
                **stream_config,
            },
            parse_env_config=False,
            validate_config=False,
        )


@pytest.fixture
def fmp_unreachable():
    with patch.object(
        FmpSurrogateKeyStream,
        "get_records",
        side_effect=AssertionError("asked FMP for its transcript symbol list"),
    ):
        yield


def test_a_named_list_is_the_transcript_universe_without_asking_fmp(fmp_unreachable):
    run = TranscriptBackfill()

    symbols = [s["symbol"] for s in run.tap.get_cached_transcript_symbols()]

    assert symbols == sorted(TranscriptBackfill.SYMBOLS)


def test_a_backfill_asks_every_quarter_of_the_configured_years_for_the_named_companies_only(
    fmp_unreachable,
):
    run = TranscriptBackfill(
        earnings_transcripts={"other_params": {"years": [2012, 2013]}}
    )

    partitions = run.tap.streams["earnings_transcripts"].partitions

    assert partitions == [
        {"symbol": symbol, "year": year, "quarter": quarter}
        for symbol in sorted(TranscriptBackfill.SYMBOLS)
        for year in (2012, 2013)
        for quarter in (1, 2, 3, 4)
    ]
