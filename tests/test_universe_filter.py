"""The country/currency filter reads each listing's country from the company
profiles. A symbol the source lacks is dropped, so a partly loaded source
silently shrinks every per-symbol universe; the filter refuses to run on one,
and names how many listings it drops otherwise. The profile stream feeds the
filter, so it requests every listing rather than the filtered few.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from tap_fmp.streams.company_streams import CompanyProfileBySymbolStream
from tap_fmp.tap import TapFMP


def _tap() -> TapFMP:
    return TapFMP(
        config={
            "api_key": "unused",
            "start_date": "1990-01-01T00:00:00Z",
            "filter_countries": ["US"],
            "filter_currencies": ["USD"],
        },
        parse_env_config=False,
        validate_config=False,
    )


def _filter(tap: TapFMP, universe: list[str], profiles: dict[str, tuple[str, str]]):
    raw = [{"symbol": s} for s in universe]
    by_symbol = {
        s: {"symbol": s, "country": country, "currency": currency}
        for s, (country, currency) in profiles.items()
    }
    with (
        patch.object(TapFMP, "get_cached_raw_company_symbols", return_value=raw),
        patch.object(
            TapFMP, "get_cached_company_profiles_by_symbol", return_value=by_symbol
        ),
    ):
        return [
            s["symbol"]
            for s in tap._apply_country_currency_filtering(raw, "company_symbols")
        ]


def test_a_partly_loaded_profile_source_stops_the_run():
    universe = [f"S{i}" for i in range(100)]
    loaded = {s: ("US", "USD") for s in universe[:39]}

    with pytest.raises(RuntimeError, match="partial load"):
        _filter(_tap(), universe, loaded)


def test_a_complete_source_filters_and_names_what_it_drops():
    universe = [f"S{i}" for i in range(100)]
    profiles = {s: ("US", "USD") for s in universe[:97]}
    profiles["S0"] = ("GB", "GBP")
    tap = _tap()

    with patch.object(tap.logger, "warning") as warning:
        kept = _filter(tap, universe, profiles)

    assert kept == universe[1:97]
    assert (
        "3 of 100 company_symbols have no company profile" in warning.call_args.args[0]
    )


def test_the_profile_stream_requests_every_listing_not_the_filtered_ones():
    raw = [{"symbol": "ABNB"}, {"symbol": "0A1O.L"}]

    with (
        patch.object(TapFMP, "get_cached_raw_company_symbols", return_value=raw),
        patch.object(
            TapFMP,
            "get_cached_company_symbols",
            side_effect=AssertionError("the filtered universe feeds on this stream"),
        ),
    ):
        partitions = CompanyProfileBySymbolStream(_tap()).partitions

    assert [p["symbol"] for p in partitions] == ["ABNB", "0A1O.L"]
