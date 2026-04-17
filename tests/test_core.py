"""Standard Singer SDK test battery.

Generates ~4000+ tests across all 251 streams via `get_tap_test_class`:
- Stream discovery
- Schema validation per stream
- Record-conformance per stream
- Primary-key uniqueness per stream
- `_sdc_*` metadata column presence per stream

Most of these tests hit the FMP API (discovery requires symbol-list calls)
and take several minutes against the full stream catalog. They're opt-in
via `RUN_LIVE_TAP_TESTS=1` so a routine `pytest` doesn't spam FMP. Run with:

    RUN_LIVE_TAP_TESTS=1 FMP_API_KEY=... uv run pytest tests/test_core.py

For day-to-day development, the lightweight tests in `test_helpers.py`,
`test_time_slice_correctness.py`, etc. cover the unit-test surface.
"""

from __future__ import annotations

import datetime
import os

import pytest

from singer_sdk.testing import SuiteConfig, get_tap_test_class

from tap_fmp.tap import TapFMP

if not (os.environ.get("RUN_LIVE_TAP_TESTS") and os.environ.get("FMP_API_KEY")):
    pytest.skip(
        "Set RUN_LIVE_TAP_TESTS=1 and FMP_API_KEY=... to run the live "
        "tap-test battery against FMP.",
        allow_module_level=True,
    )

SAMPLE_CONFIG = {
    "start_date": (
        datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=7)
    ).strftime("%Y-%m-%d"),
    "api_key": os.environ["FMP_API_KEY"],
    "company_symbols": {"select_symbols": ["AAPL"]},
    "financial_statement_symbols": {"select_symbols": ["AAPL"]},
    "etf_symbols": {"select_etf_symbols": ["SPY"]},
    "index_symbols": {"select_index_symbols": ["^GSPC"]},
}

# `max_records_limit` keeps the test battery quick for a smoke run.
TestTapFMP = get_tap_test_class(
    tap_class=TapFMP,
    config=SAMPLE_CONFIG,
    suite_config=SuiteConfig(max_records_limit=10),
)
