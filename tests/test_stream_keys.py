"""FMP's key-executives and stock-peers replies omit the company the request
asked about. The requested symbol is part of the row's identity: without it a
row can't be tied to its company, and two companies' identical replies would
hash to one surrogate key.
"""

from __future__ import annotations

import pytest

from tap_fmp.streams.company_streams import (
    CompanyExecutiveStream,
    StockPeerComparisonStream,
)


@pytest.mark.parametrize(
    "stream_cls,reply,company_field",
    [
        (CompanyExecutiveStream, {"name": "Jane Doe", "title": "CFO"}, "symbol"),
        (
            StockPeerComparisonStream,
            {"symbol": "MSFT", "company_name": "Microsoft", "price": 1.0},
            "requested_symbol",
        ),
    ],
)
def test_the_company_asked_about_is_on_the_row_and_in_its_key(
    stream_cls, reply, company_field
):
    stream = object.__new__(stream_cls)  # post_process needs no tap/config
    for_apple = stream.post_process(dict(reply), {"symbol": "AAPL"})
    for_google = stream.post_process(dict(reply), {"symbol": "GOOGL"})

    assert for_apple[company_field] == "AAPL"
    assert for_apple["surrogate_key"] != for_google["surrogate_key"]
