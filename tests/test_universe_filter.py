"""The universe filter reads each listing's country, currency and exchange from
the company profiles. A symbol the source lacks is dropped, so a partly loaded
source silently shrinks every per-symbol universe; the filter refuses to run on
one, and names how many listings it drops otherwise. A US company's listing
abroad carries the company's US profile, so only its exchange can drop it. The
profile stream feeds the filter, so it requests every listing rather than the
filtered few.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from tap_fmp.helpers import CompanyProfilesManager
from tap_fmp.streams.company_streams import CompanyProfileBySymbolStream
from tap_fmp.tap import TapFMP

US_EXCHANGES = ["NASDAQ", "NYSE", "AMEX", "CBOE", "OTC"]


def _tap(**config) -> TapFMP:
    return TapFMP(
        config={
            "api_key": "unused",
            "start_date": "1990-01-01T00:00:00Z",
            "filter_countries": ["US"],
            "filter_currencies": ["USD"],
            **config,
        },
        parse_env_config=False,
        validate_config=False,
    )


def _filter(tap: TapFMP, universe: list[str], profiles: dict[str, tuple]):
    raw = [{"symbol": s} for s in universe]
    by_symbol = {
        s: {"symbol": s, **dict(zip(("country", "currency", "exchange"), profile))}
        for s, profile in profiles.items()
    }
    with (
        patch.object(TapFMP, "get_cached_raw_company_symbols", return_value=raw),
        patch.object(
            TapFMP, "get_cached_company_profiles_by_symbol", return_value=by_symbol
        ),
    ):
        return [
            s["symbol"] for s in tap._apply_profile_filtering(raw, "company_symbols")
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


def test_a_us_companys_listing_abroad_is_not_a_us_ticker():
    profiles = {
        "CDNS": ("US", "USD", "NASDAQ"),
        "0HS2.L": ("US", "USD", "LSE"),
        "HOM-U.TO": ("US", "USD", "TSX"),
        "AABB": ("US", "USD", "OTC"),
    }

    kept = _filter(_tap(filter_exchanges=US_EXCHANGES), list(profiles), profiles)

    assert kept == ["CDNS", "AABB"]


def test_a_foreign_company_on_a_us_exchange_still_fails_on_its_country():
    profiles = {"CDNS": ("US", "USD", "NASDAQ"), "BABA": ("CN", "USD", "NYSE")}

    kept = _filter(_tap(filter_exchanges=US_EXCHANGES), list(profiles), profiles)

    assert kept == ["CDNS"]


def test_a_profile_without_an_exchange_cannot_be_placed_as_a_us_ticker():
    profiles = {"CDNS": ("US", "USD", "NASDAQ"), "NOEX": ("US", "USD", None)}

    kept = _filter(_tap(filter_exchanges=US_EXCHANGES), list(profiles), profiles)

    assert kept == ["CDNS"]


def test_listings_abroad_stay_when_no_exchange_filter_is_configured():
    profiles = {"CDNS": ("US", "USD", "NASDAQ"), "0HS2.L": ("US", "USD", "LSE")}

    assert _filter(_tap(), list(profiles), profiles) == ["CDNS", "0HS2.L"]


def test_the_exchange_filter_filters_on_its_own():
    tap = TapFMP(
        config={
            "api_key": "unused",
            "start_date": "1990-01-01T00:00:00Z",
            "filter_exchanges": US_EXCHANGES,
        },
        parse_env_config=False,
        validate_config=False,
    )
    profiles = {"CDNS": ("US", "USD", "NASDAQ"), "0HS2.L": ("US", "USD", "LSE")}

    assert _filter(tap, list(profiles), profiles) == ["CDNS"]


def test_the_database_source_reads_each_listings_exchange():
    connection = MagicMock()
    cursor = (
        connection.__enter__.return_value.cursor.return_value.__enter__.return_value
    )
    cursor.fetchall.return_value = []
    manager = CompanyProfilesManager({"database_config": {"schema": "s", "table": "t"}})

    with patch("tap_fmp.helpers.psycopg2.connect", return_value=connection):
        manager._load_from_database()

    assert "symbol, country, currency, exchange" in repr(
        cursor.execute.call_args.args[0]
    )


def test_the_csv_source_files_each_listing_under_its_short_exchange_name(tmp_path):
    csv_path = tmp_path / "profiles.csv"
    csv_path.write_text(
        "symbol,currency,exchange,exchange_short_name,country\n"
        "GVCIW,USD,NASDAQ Global Market,NASDAQ,US\n"
    )
    source = {"use_csv": True, "csv_path": str(csv_path)}

    rows = CompanyProfilesManager({"exchange_variants_source": source})._load_from_csv()

    assert rows[0]["exchange"] == "NASDAQ"
