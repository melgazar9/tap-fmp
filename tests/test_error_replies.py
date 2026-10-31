"""FMP answers some requests with `{"Error Message": ...}` under a 200. That
reply is never a record: its no-data message means the endpoint holds nothing
for the request, and any other message is a fault that stops the stream
instead of landing in the warehouse as a row.
"""

from __future__ import annotations

import pytest

from tap_fmp.client import FmpErrorReplyError
from tests.test_quota_exhaustion import SECRET, _fetch, _ScriptedSession, _StubStream

NO_DATA_BODY = (
    b'{"Error Message": "No Data for this symbol or invalid API call. Please retry or visit '
    b'our documentation at https://financialmodelingprep.com/developer/docs."}'
)
FAULT_BODY = (
    b'{"Error Message": "Exclusive Endpoint : This endpoint is not available under your '
    b"current subscription apikey=" + SECRET.encode() + b'"}'
)


def test_the_no_data_reply_is_an_empty_result_not_a_record():
    stream = _StubStream(
        _ScriptedSession([(200, NO_DATA_BODY)]), quota_exhausted_after_seconds=0
    )

    assert _fetch(stream) == []


def test_any_other_error_reply_stops_the_stream_without_leaking_the_key():
    session = _ScriptedSession([(200, FAULT_BODY)])
    stream = _StubStream(session, quota_exhausted_after_seconds=0)

    with pytest.raises(FmpErrorReplyError) as exc_info:
        _fetch(stream)

    assert session.calls == 1
    assert "Exclusive Endpoint" in str(exc_info.value)
    assert SECRET not in str(exc_info.value)


def test_a_plain_object_reply_is_still_one_record():
    stream = _StubStream(
        _ScriptedSession([(200, b'{"symbol": "AAPL", "price": 1.5}')]),
        quota_exhausted_after_seconds=0,
    )

    assert _fetch(stream) == [{"symbol": "AAPL", "price": 1.5}]
