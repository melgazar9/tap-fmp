"""A stream's partition state: the entry the SDK would find, without scanning every saved partition per record."""

from copy import deepcopy
from unittest.mock import patch

import pytest
from singer_sdk.helpers import _state
from singer_sdk.streams import RESTStream

from tap_fmp.client import FmpRestStream
from tap_fmp.tap import TapFMP

STREAM = "company_prices_1h"


def _saved_state(*symbols):
    partitions = [
        {
            "context": {"symbol": symbol},
            "replication_key": "date",
            "replication_key_value": "2026-06-01 15:30:00",
        }
        for symbol in symbols
    ]
    return {"bookmarks": {STREAM: {"partitions": partitions}}}


def _tap_and_stream(state):
    tap = TapFMP(
        config={"api_key": "unused", "start_date": "1990-01-01T00:00:00Z"},
        state=deepcopy(state),
        parse_env_config=False,
        validate_config=False,
    )
    return tap, tap.streams[STREAM]


def _partitions(tap):
    return tap.state["bookmarks"][STREAM]["partitions"]


def test_a_saved_partition_resolves_to_its_own_entry():
    tap, stream = _tap_and_stream(_saved_state("A", "B"))

    entry = stream.get_context_state({"symbol": "B"})

    assert entry is _partitions(tap)[1]
    assert entry["replication_key_value"] == "2026-06-01 15:30:00"


def test_a_new_partition_is_added_once_and_keeps_resolving_to_it():
    tap, stream = _tap_and_stream(_saved_state("A"))

    first = stream.get_context_state({"symbol": "NEW"})
    second = stream.get_context_state({"symbol": "NEW"})

    assert first is second
    assert first is _partitions(tap)[1]
    assert len(_partitions(tap)) == 2


def test_the_order_of_a_contexts_keys_does_not_make_a_second_entry():
    tap, stream = _tap_and_stream({})

    first = stream.get_context_state({"symbol": "A", "period": "annual"})
    second = stream.get_context_state({"period": "annual", "symbol": "A"})

    assert first is second
    assert len(_partitions(tap)) == 1


def test_a_stream_without_a_partition_gets_the_stream_state():
    tap, stream = _tap_and_stream(_saved_state("A"))

    assert stream.get_context_state(None) is tap.state["bookmarks"][STREAM]


def test_duplicate_saved_entries_still_fail_as_the_sdk_fails_them():
    state = _saved_state("A", "A")
    _, stream = _tap_and_stream(state)

    with pytest.raises(ValueError, match="duplicate entries"):
        stream.get_context_state({"symbol": "A"})


def test_records_do_not_rescan_the_saved_partitions():
    _, stream = _tap_and_stream(_saved_state(*[f"S{i}" for i in range(50)]))
    scan = _state._find_in_partitions_list

    with patch.object(_state, "_find_in_partitions_list", wraps=scan) as scans:
        for hour in range(500):
            stream._increment_stream_state(
                {"symbol": "S7", "date": f"2026-07-01 {hour % 24:02d}:30:00"},
                context={"symbol": "S7"},
            )

    assert scans.call_count == 1


def _replay(stream):
    """What a run does to state: bookmarks resume, new symbols start, every partition finalizes."""
    for symbol in ("S1", "NEW1", "S3", "NEW2"):
        context = {"symbol": symbol}
        stream._write_starting_replication_value(context)
        for day in range(1, 29):
            stream._increment_stream_state(
                {"symbol": symbol, "date": f"2026-07-{day:02d} 15:30:00"},
                context=context,
            )
        stream.finalize_state_progress_markers(stream.get_context_state(context))


def test_a_run_leaves_exactly_the_state_the_sdk_would_have():
    saved = _saved_state("S0", "S1", "S2", "S3")
    tap, stream = _tap_and_stream(saved)
    _replay(stream)
    with patch.object(FmpRestStream, "get_context_state", RESTStream.get_context_state):
        sdk_tap, sdk_stream = _tap_and_stream(saved)
        _replay(sdk_stream)

    assert tap.state == sdk_tap.state
    assert tap.state["bookmarks"][STREAM]["partitions"][1]["replication_key_value"] == (
        "2026-07-28 15:30:00"
    )
