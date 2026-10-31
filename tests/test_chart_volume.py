"""FMP sends garbage volumes on some daily bars, past what the bigint volume
column holds. The bar's prices are real, so only that volume is dropped; a
stream map can't do this, since the SDK rejects a map on any stream the
catalog leaves unselected and every single-stream run would fail at startup.
"""

from __future__ import annotations

import pytest

from tap_fmp.streams.chart_streams import CompanyChartFullStream

BAR = {"symbol": "AAPL", "date": "2020-01-02", "close": 75.0875}


@pytest.mark.parametrize(
    "volume,stored",
    [(135480400, 135480400), (2**63 - 1, 2**63 - 1), (2**63, None), (None, None)],
)
def test_only_a_volume_past_bigint_is_dropped(volume, stored):
    stream = object.__new__(CompanyChartFullStream)  # post_process needs no tap/config
    record = stream.post_process({**BAR, "volume": volume})

    assert record["volume"] == stored
    assert record["close"] == BAR["close"]
