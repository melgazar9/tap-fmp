"""The FMP API key travels in the `apikey` query param, so every URL the tap
requests contains it. Any exception that escapes the tap is printed with its
full traceback, including chained exceptions, so no exception in that chain
may carry the raw key.
"""

from __future__ import annotations

import threading
import traceback

import pytest
import requests

from tap_fmp.client import FmpRestStream

SECRET = "SuperSecretApiKey123"


class _FakeSession:
    """Builds a real `requests.Response` without touching the network."""

    def __init__(self, status_code: int, reason: str):
        self.status_code = status_code
        self.reason = reason

    def get(self, url, params=None, timeout=None):
        response = requests.Response()
        response.request = requests.Request("GET", url, params=params).prepare()
        response.url = response.request.url
        response.status_code = self.status_code
        response.reason = self.reason
        response._content = b'{"Error Message": "Payment Required"}'
        return response


class _StubStream(FmpRestStream):
    """Test-only subclass that bypasses Singer SDK construction."""

    name = "test_stream"
    schema = {"properties": {}}

    def __init__(self, session: _FakeSession):
        self._session = session
        self.other_params = {}
        self._min_interval = 0.0
        self._throttle_lock = threading.Lock()
        self._last_call_ts = 0.0

    @property
    def requests_session(self):
        return self._session

    def get_url(self, context):
        return "https://financialmodelingprep.com/stable/company-screener"


def test_http_error_traceback_does_not_leak_api_key():
    stream = _StubStream(_FakeSession(402, "Payment Required"))

    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        stream._fetch_with_retry(stream.get_url(None), {"limit": 10, "apikey": SECRET})

    printed = "".join(traceback.format_exception(exc_info.value))
    assert "402" in printed
    assert "apikey=<REDACTED>" in printed
    assert SECRET not in printed
