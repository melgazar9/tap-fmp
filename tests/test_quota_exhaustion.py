"""FMP answers 429 both for its per-minute request limit and once the plan's
30-day bandwidth cap is spent. The first clears within a minute; the second
never does, so retrying it only stalls every worker until max_time. A 429
that outlasts the per-minute window must stop the stream, and every HTTP
error must carry FMP's response body so the two can be told apart.
"""

from __future__ import annotations

import threading

import pytest
import requests

from tap_fmp.client import FmpQuotaExhaustedError, FmpRestStream

SECRET = "SuperSecretApiKey123"
QUOTA_BODY = b'{"Error Message": "Limit Reach. Please upgrade your plan"}'


class _ScriptedSession:
    """Answers each GET with the next (status, body) in the script, the last one repeating."""

    def __init__(self, script: list[tuple[int, bytes]]):
        self._script = list(script)
        self.calls = 0

    def get(self, url, params=None, timeout=None):
        status, body = self._script[min(self.calls, len(self._script) - 1)]
        self.calls += 1
        response = requests.Response()
        response.request = requests.Request("GET", url, params=params).prepare()
        response.url = response.request.url
        response.status_code = status
        response.reason = {
            200: "OK",
            402: "Payment Required",
            429: "Too Many Requests",
        }[status]
        response._content = body
        return response


class _StubStream(FmpRestStream):
    """Test-only subclass that bypasses Singer SDK construction."""

    name = "test_stream"
    schema = {"properties": {}}

    def __init__(self, session: _ScriptedSession, quota_exhausted_after_seconds: float):
        self._session = session
        self.other_params = {}
        self._min_interval = 0.0
        self._throttle_lock = threading.Lock()
        self._last_call_ts = 0.0
        self._quota_exhausted_after_seconds = quota_exhausted_after_seconds

    @property
    def requests_session(self):
        return self._session

    def get_url(self, context):
        return "https://financialmodelingprep.com/stable/company-screener"


def _fetch(stream: _StubStream):
    return stream._fetch_with_retry(
        stream.get_url(None), {"limit": 10, "apikey": SECRET}
    )


def test_a_429_that_outlasts_the_window_stops_the_stream_with_fmps_reason():
    session = _ScriptedSession([(429, QUOTA_BODY)])
    stream = _StubStream(session, quota_exhausted_after_seconds=0)

    with pytest.raises(FmpQuotaExhaustedError) as exc_info:
        _fetch(stream)

    message = str(exc_info.value)
    assert session.calls == 1
    assert "Limit Reach" in message
    assert "apikey=<REDACTED>" in message
    assert SECRET not in message


def test_a_429_that_clears_inside_the_window_is_retried():
    session = _ScriptedSession([(429, QUOTA_BODY), (200, b'[{"symbol": "AAPL"}]')])
    stream = _StubStream(session, quota_exhausted_after_seconds=300)

    assert _fetch(stream) == [{"symbol": "AAPL"}]
    assert session.calls == 2


def test_a_refused_endpoint_fails_at_once_with_its_body():
    session = _ScriptedSession([(402, b'{"Error Message": "Restricted Endpoint"}')])
    stream = _StubStream(session, quota_exhausted_after_seconds=300)

    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        _fetch(stream)

    assert not isinstance(exc_info.value, FmpQuotaExhaustedError)
    assert session.calls == 1
    assert "Restricted Endpoint" in str(exc_info.value)
