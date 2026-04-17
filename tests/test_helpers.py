"""Tests for tap_fmp.helpers.

These lock in the snake_case converter. Regressions here cause silent column
drops in Postgres (the target has additionalProperties: true, so a wrong key
flows through as an orphan column while the canonical schema column stays
NULL). Every time the acronym list changes, add a test.
"""

from __future__ import annotations

import pytest

from tap_fmp.helpers import clean_strings


@pytest.mark.parametrize(
    "raw,expected",
    [
        # Smashed acronyms (the original bug — EBITDA + TTM with no boundary)
        ("evToEBITDATTM", "ev_to_ebitda_ttm"),
        ("netDebtToEBITDATTM", "net_debt_to_ebitda_ttm"),
        ("EPSTTM", "eps_ttm"),
        ("ROETTM", "roe_ttm"),
        ("ROICTTM", "roic_ttm"),
        ("netIncomePerEBTTTM", "net_income_per_ebt_ttm"),
        # Standalone acronyms must NOT be split
        ("roic", "roic"),
        ("ROIC", "roic"),
        ("EBT", "ebt"),
        ("evToEBITDA", "ev_to_ebitda"),
        ("EBITDAMargin", "ebitda_margin"),
        # Innocent substrings that contain an acronym spelling must not be mangled
        ("debit", "debit"),
        ("Debit", "debit"),
        # Standard camelCase paths still work
        ("marketCap", "market_cap"),
        ("symbol", "symbol"),
        ("isActivelyTrading", "is_actively_trading"),
        ("grossProfitMarginTTM", "gross_profit_margin_ttm"),
        ("returnOnEquityTTM", "return_on_equity_ttm"),
        (
            "researchAndDevelopementToRevenueTTM",
            "research_and_development_to_revenue_ttm",
        ),
        # TitleCase acronyms (FMP occasionally sends "Ttm")
        ("EBITDATtm", "ebitda_ttm"),
        # Non-alphanumeric separators (FMP's DCF bulk emits "Stock Price")
        ("Stock Price", "stock_price"),
        # Non-alphanumeric adjacent to smashed acronym — must still split
        ("ev/EBITDATTM", "ev_ebitda_ttm"),
        ("ev EBITDATTM", "ev_ebitda_ttm"),
        # Acronym glued to a lowercase-word continuation (no case change)
        ("EBITstart", "ebit_start"),
        ("EBITDAstream", "ebitda_stream"),
        ("ADRholdings", "adr_holdings"),
        ("EPScalculated", "eps_calculated"),
        # FMP-side typos and malformed keys, normalized via _FMP_KEY_RENAMES
        ("researchAndDevelopementToRevenue", "research_and_development_to_revenue"),
        (
            "researchAndDevelopementToRevenueTTM",
            "research_and_development_to_revenue_ttm",
        ),
        ("ebitgrowth", "ebit_growth"),
        ("epsgrowth", "eps_growth"),
        ("epsdilutedGrowth", "eps_diluted_growth"),
        ("rdexpenseGrowth", "rd_expense_growth"),
        ("sgaexpensesGrowth", "sga_expenses_growth"),
        ("bookValueperShareGrowth", "book_value_per_share_growth"),
        (
            "tenYDividendperShareGrowthPerShare",
            "ten_y_dividend_per_share_growth_per_share",
        ),
        ("costofDebt", "cost_of_debt"),
        ("netPostion", "net_position"),
    ],
)
def test_clean_strings(raw, expected):
    assert clean_strings([raw]) == [expected]


def test_clean_strings_batches():
    """clean_strings takes a list and returns a list of the same length."""
    out = clean_strings(["marketCap", "symbol", "EBITDATTM"])
    assert out == ["market_cap", "symbol", "ebitda_ttm"]


def test_clean_strings_empty_list():
    assert clean_strings([]) == []


def test_fmp_key_renames_dont_collide_with_declared_schema_fields():
    """Safety: the LHS of every _FMP_KEY_RENAMES entry is the buggy form FMP
    emits. If any stream has a legitimate schema field matching a LHS, that
    field would be silently renamed and never populate. Should never happen.
    """
    import ast
    from pathlib import Path

    from tap_fmp.helpers import _FMP_KEY_RENAMES

    streams_dir = Path(__file__).parent.parent / "tap_fmp" / "streams"
    declared_fields: set[str] = set()
    for f in streams_dir.glob("*.py"):
        if f.name == "__init__.py":
            continue
        tree = ast.parse(f.read_text())
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "Property"
                and node.args
                and isinstance(node.args[0], ast.Constant)
            ):
                declared_fields.add(node.args[0].value)

    collisions = set(_FMP_KEY_RENAMES.keys()) & declared_fields
    assert not collisions, (
        f"_FMP_KEY_RENAMES LHS collides with declared schema fields: {collisions}. "
        f"These streams expect the LHS name and would be silently renamed away."
    )
