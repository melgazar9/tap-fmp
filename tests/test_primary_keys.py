"""Primary-key invariants for streams migrated off the whole-row surrogate key.

The loader upserts on `primary_keys`. If a volatile field (estimate, actual, price) feeds the key, a
re-fetch of the same logical row inserts a duplicate instead of updating it. See docs/PRIMARY_KEY_AUDIT.md.
"""

from __future__ import annotations

import pytest

from tap_fmp.streams.economics_streams import EconomicCalendarStream


# Same release fetched twice: before the print (estimate only) and after (actual + revised previous).
ECONOMIC_CALENDAR_PRE_RELEASE = {
    "date": "2026-07-29 03:30:00",
    "country": "SG",
    "event": "Import Prices YoY (Jun)",
    "currency": "SGD",
    "previous": 18.4,
    "estimate": 21,
    "actual": None,
    "change": None,
    "impact": "Low",
    "change_percentage": None,
    "unit": "%",
}
ECONOMIC_CALENDAR_POST_RELEASE = {
    **ECONOMIC_CALENDAR_PRE_RELEASE,
    "previous": 18.5,
    "actual": 13.6,
    "change": -4.9,
    "change_percentage": -26.486,
}


@pytest.mark.parametrize(
    "stream_cls,pre,post",
    [
        (
            EconomicCalendarStream,
            ECONOMIC_CALENDAR_PRE_RELEASE,
            ECONOMIC_CALENDAR_POST_RELEASE,
        )
    ],
)
def test_refetch_with_changed_values_keeps_the_same_key(stream_cls, pre, post):
    stream = object.__new__(stream_cls)  # post_process needs no tap/config
    pre_out = stream.post_process(dict(pre))
    post_out = stream.post_process(dict(post))

    assert "surrogate_key" not in pre_out
    assert "surrogate_key" not in post_out
    assert [pre_out[k] for k in stream_cls.primary_keys] == [
        post_out[k] for k in stream_cls.primary_keys
    ]


@pytest.mark.parametrize("stream_cls", [EconomicCalendarStream])
def test_primary_key_fields_are_required_and_surrogate_is_gone(stream_cls):
    properties = stream_cls.schema["properties"]
    required = set(stream_cls.schema.get("required", []))

    assert "surrogate_key" not in properties
    for key in stream_cls.primary_keys:
        assert (
            key in properties
        ), f"{stream_cls.name}: primary key {key!r} missing from schema"
        assert (
            key in required
        ), f"{stream_cls.name}: primary key {key!r} must be required (non-null)"


def test_economic_calendar_key_is_release_identity():
    assert EconomicCalendarStream.primary_keys == ["date", "country", "event"]
