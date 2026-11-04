"""Transcripts resume per symbol: a bookmarked symbol is asked only the fiscal
years a call after its bookmark can carry, a new one its whole history, and a
company whose fiscal year runs ahead of the calendar is asked next year too."""

from __future__ import annotations

from datetime import date
from unittest.mock import patch

from tap_fmp.streams.earnings_transcript_streams import EarningsTranscriptStream
from tap_fmp.tap import TapFMP


class TranscriptPartitions:
    """Partitions of a transcript stream whose tap holds a bookmark for WMT only."""

    SYMBOLS = [{"symbol": "WMT"}, {"symbol": "NEWCO"}]
    WMT_BOOKMARK = "2026-05-14"
    START_DATE = "1990-01-01T00:00:00Z"

    def __init__(self, **stream_config):
        self.tap = TapFMP(
            config={
                "api_key": "unused",
                "start_date": self.START_DATE,
                **stream_config,
            },
            state={
                "bookmarks": {
                    "earnings_transcripts": {
                        "partitions": [
                            {
                                "context": {"symbol": "WMT"},
                                "replication_key": "date",
                                "replication_key_value": self.WMT_BOOKMARK,
                            }
                        ]
                    }
                }
            },
            parse_env_config=False,
            validate_config=False,
        )
        self.stream = self.tap.streams["earnings_transcripts"]
        with patch.object(
            TapFMP, "get_cached_transcript_symbols", return_value=self.SYMBOLS
        ):
            self.partitions = self.stream.partitions

    def years(self, symbol: str) -> list[int]:
        return sorted({p["year"] for p in self.partitions if p["symbol"] == symbol})

    def quarters(self, symbol: str, year: int) -> list[int]:
        return sorted(
            p["quarter"]
            for p in self.partitions
            if p["symbol"] == symbol and p["year"] == year
        )

    @staticmethod
    def fiscal_years_from(call_date: str) -> list[int]:
        """Every fiscal year a call on or after this date can carry, through next fiscal year."""
        return list(
            range(
                int(call_date[:4]) - EarningsTranscriptStream.FISCAL_YEAR_LAG,
                date.today().year + EarningsTranscriptStream.FISCAL_YEAR_LEAD + 1,
            )
        )


def test_a_bookmarked_symbol_is_asked_every_quarter_of_only_the_years_a_newer_call_can_carry():
    run = TranscriptPartitions()
    next_fiscal_year = date.today().year + EarningsTranscriptStream.FISCAL_YEAR_LEAD

    assert run.years("WMT") == run.fiscal_years_from(run.WMT_BOOKMARK)
    assert run.quarters("WMT", next_fiscal_year) == [1, 2, 3, 4]


def test_a_symbol_without_a_bookmark_is_asked_its_whole_history():
    run = TranscriptPartitions()

    assert run.years("NEWCO") == run.fiscal_years_from(run.START_DATE)


def test_configured_years_are_a_backfill_for_every_symbol():
    run = TranscriptPartitions(
        earnings_transcripts={"other_params": {"years": ["2023"], "quarters": [3]}}
    )

    assert run.partitions == [
        {"symbol": "WMT", "year": 2023, "quarter": 3},
        {"symbol": "NEWCO", "year": 2023, "quarter": 3},
    ]


def test_one_configured_year_is_that_year_not_its_digits():
    run = TranscriptPartitions(earnings_transcripts={"other_params": {"years": "2023"}})

    assert run.years("WMT") == [2023]


def test_every_partition_of_a_symbol_resumes_from_its_one_bookmark():
    run = TranscriptPartitions()
    wmt = next(p for p in run.partitions if p["symbol"] == "WMT")

    assert (
        run.stream.get_context_state(wmt)["replication_key_value"] == run.WMT_BOOKMARK
    )


def test_a_request_carries_its_partition_and_leaves_the_configured_params_alone():
    run = TranscriptPartitions()
    partition = run.partitions[0]

    with patch.object(
        EarningsTranscriptStream, "_fetch_with_retry", return_value=[]
    ) as fetch:
        list(run.stream.get_records(partition))

    sent = fetch.call_args.args[1]
    assert {key: sent[key] for key in partition} == partition
    assert "year" not in run.stream.query_params
    assert "quarter" not in run.stream.query_params
