import re
import uuid
from functools import lru_cache
from pathlib import Path
from datetime import datetime, timedelta
import logging
import threading
import typing as t
import csv


# Financial acronyms FMP can smash together in field names (e.g. "EBITDATTM").
# The standard camelCase rules can't find a boundary between two all-caps runs,
# so adjacent acronyms collapse into one token and the schema field silently
# never populates. We split after a known acronym only when it's directly
# followed by another uppercase letter.
#
# Acronyms that are prefixes of longer ones (e.g. EBIT ⊂ EBITDA, ROI ⊂ ROIC)
# need a negative lookahead for the extension, otherwise regex backtracking
# would match the shorter one and split mid-word ("ROIC" -> "ROI_C").
_FINANCIAL_ACRONYMS = (
    "EBITDA",
    "NOPAT",
    "ROIC",
    "EBIT",
    "EBT",
    "TTM",
    "EPS",
    "ROE",
    "ROA",
    "ROI",
    "EV",
    "NAV",
    "DCF",
    "FCF",
    "IPO",
    "ESG",
    "ETF",
    "REIT",
    "ADR",
    "GAAP",
    "IFRS",
)


def _compile_acronym_split_re(acronyms):
    # Split after a known acronym when it's followed by either:
    #   - another uppercase letter (smashed acronym pair, e.g. EBITDATTM),
    #   - two+ lowercase letters (acronym glued to a word, e.g. EBITstart,
    #     ADRholdings). Single trailing lowercase is ignored so plurals like
    #     ETFs/NAVs/IPOs stay intact.
    parts = []
    for a in sorted(acronyms, key=len, reverse=True):
        extensions = [o[len(a) :] for o in acronyms if o != a and o.startswith(a)]
        parts.append(f"{a}(?!{'|'.join(extensions)})" if extensions else a)
    return re.compile(r"(" + "|".join(parts) + r")(?=[A-Z]|[a-z]{2})")


_ACRONYM_SPLIT_RE = _compile_acronym_split_re(_FINANCIAL_ACRONYMS)
# All listed acronyms are 2+ uppercase letters, so a string with no two
# adjacent uppercase letters can never match. Cheap pre-check to skip the
# heavier alternation regex on the common case (camelCase keys).
_HAS_UPPER_RUN_RE = re.compile(r"[A-Z]{2}")

# Canonical renames for keys that the camelCase converter can't fix because the
# original FMP key is itself malformed (typos, smashed lowercase, missing
# separators). Applied AFTER clean_strings so left-hand side is the
# converter's output and right-hand side is the documented column name.
# Each entry should have a short comment explaining why FMP emits the bad form.
_FMP_KEY_RENAMES = {
    # FMP typo: "developement" -> "development"
    "research_and_developement_to_revenue": "research_and_development_to_revenue",
    "research_and_developement_to_revenue_ttm": "research_and_development_to_revenue_ttm",  # noqa: E501
    # /stable/financial-growth emits these all-lowercase; converter can't split
    "ebitgrowth": "ebit_growth",
    "epsgrowth": "eps_growth",
    "epsdiluted_growth": "eps_diluted_growth",
    "rdexpense_growth": "rd_expense_growth",
    "sgaexpenses_growth": "sga_expenses_growth",
    # /stable/financial-growth emits "perShare" with lowercase "per"
    "book_valueper_share_growth": "book_value_per_share_growth",
    "ten_y_dividendper_share_growth_per_share": "ten_y_dividend_per_share_growth_per_share",  # noqa: E501
    "five_y_dividendper_share_growth_per_share": "five_y_dividend_per_share_growth_per_share",  # noqa: E501
    "three_y_dividendper_share_growth_per_share": "three_y_dividend_per_share_growth_per_share",  # noqa: E501
    # custom-dcf emits "costofDebt" with lowercase "of"
    "costof_debt": "cost_of_debt",
    # commitment-of-traders analysis emits "netPostion" (typo)
    "net_postion": "net_position",
    # commitment-of-traders report emits "spead" (typo) in two fields
    "change_in_noncomm_spead_all": "change_in_noncomm_spread_all",
    "traders_noncomm_spead_ol": "traders_noncomm_spread_ol",
    # balance-sheet-growth-bulk emits "othertotal" glued (lowercase)
    "growth_othertotal_stockholders_equity": "growth_other_total_stockholders_equity",
    # cash-flow-growth-bulk emits "activites" typo (missing 'i')
    "growth_net_cash_provided_by_operating_activites": "growth_net_cash_provided_by_operating_activities",
    "growth_other_investing_activites": "growth_other_investing_activities",
    "growth_net_cash_used_for_investing_activites": "growth_net_cash_used_for_investing_activities",
    "growth_other_financing_activites": "growth_other_financing_activities",
}


@lru_cache(maxsize=4096)
def _clean_one(s: str) -> str:
    """Convert a single key to snake_case + apply FMP-typo renames.
    Cached because FMP responses repeat the same ~50-200 key strings across
    millions of records during a full sync; this is the hot path."""
    cleaned = re.sub(r"[^a-zA-Z0-9_]", "_", s)
    if _HAS_UPPER_RUN_RE.search(cleaned):
        cleaned = _ACRONYM_SPLIT_RE.sub(r"\1_", cleaned)
        cleaned = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", cleaned)
    cleaned = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", cleaned)
    cleaned = re.sub(r"_+", "_", cleaned).strip("_").lower()
    return _FMP_KEY_RENAMES.get(cleaned, cleaned)


def clean_strings(lst):
    return [_clean_one(s) for s in lst]


def clean_json_keys(data: list[dict]) -> list[dict]:
    def clean_nested_dict(obj):
        if isinstance(obj, dict):
            return {_clean_one(k): clean_nested_dict(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [clean_nested_dict(item) for item in obj]
        else:
            return obj

    return [clean_nested_dict(d) for d in data]


def blank_strings_to_none(row: dict, fields: t.Iterable[str]) -> None:
    """Mutate row in place: replace "" with None for listed date-like fields.
    singer_sdk DateType/DateTimeType validation rejects empty strings, but FMP
    returns "" for unknown historical dates; normalize before validation."""
    for field in fields:
        if row.get(field) == "":
            row[field] = None


def generate_surrogate_key(data: dict, namespace=uuid.NAMESPACE_DNS) -> str:
    key_values = [str(data.get(field, "")) for field in sorted(data.keys())]
    key_string = "|".join(key_values)
    return str(uuid.uuid5(namespace, key_string))


def safe_int(value) -> int | None:
    """Safely convert value to int, handling all null/empty cases."""
    if value is None:
        return None
    if isinstance(value, str):
        if len(value.strip()) == 0:
            return None
        if value.lower() in ("null", "none", "nan"):
            return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


class ExchangeVariantsManager:
    """Manages exchange variants data with priority-based sourcing."""

    def __init__(self, config: dict, logger: logging.Logger = None, tap_instance=None):
        self.config = config
        self.logger = logger or logging.getLogger(__name__)
        self.tap_instance = tap_instance
        self._cache: t.List[t.Dict[str, t.Any]] = []
        self._cache_timestamp: t.Optional[datetime] = None
        self._cache_lock = threading.Lock()
        self._variants_by_symbol: t.Optional[t.Dict[str, t.Dict[str, t.Any]]] = None

        self.exchange_config = config.get("exchange_variants_source", {})
        self.db_config = config.get("database_config", {})

        self.use_csv = self.exchange_config.get("use_csv", False)
        self.csv_path = self.exchange_config.get(
            "csv_path", "distinct_exchange_variants.csv"
        )
        self.use_database = self.exchange_config.get("use_database", False)
        self.cache_ttl_hours = self.exchange_config.get("cache_ttl_hours", 72)

    def get_exchange_variants(self) -> t.List[t.Dict[str, t.Any]]:
        """Resolve exchange variants: DB → CSV → raise. The stream sync is
        the canonical populator; callers that reach this code on a cold
        table should run `--select exchange_variants` first."""
        with self._cache_lock:
            if self._is_cache_valid():
                self.logger.info("Using cached exchange variants data")
                return self._cache

            if self.use_database and self._has_database_config():
                self.logger.info("Loading exchange variants from database")
                data = self._load_from_database()
                if data:
                    self._update_cache(data)
                    return self._cache
                self.logger.warning("Database loading failed, trying next source")

            if self.use_csv and self._has_csv_file():
                self.logger.info("Loading exchange variants from CSV")
                data = self._load_from_csv()
                if data:
                    self._update_cache(data)
                    return self._cache
                self.logger.warning("CSV loading failed, no further sources")

            raise RuntimeError(
                "exchange_variants unavailable from any configured source "
                "(DB/CSV). Run `meltano el tap-fmp target-postgres --select "
                "exchange_variants` to populate the table, or supply "
                "distinct_exchange_variants.csv."
            )

    def _is_cache_valid(self) -> bool:
        """Check if current cache is still valid."""
        if not self._cache or not self._cache_timestamp:
            return False

        expiry_time = self._cache_timestamp + timedelta(hours=self.cache_ttl_hours)
        return datetime.now() < expiry_time

    def _update_cache(self, data: t.List[t.Dict[str, t.Any]]) -> None:
        """Update internal cache with new data."""
        self._cache = data
        self._cache_timestamp = datetime.now()
        self._variants_by_symbol = None  # invalidate derived index
        self.logger.info(f"Cache updated with {len(data)} exchange variants")

    def get_variants_by_symbol(self) -> t.Dict[str, t.Dict[str, t.Any]]:
        """Symbol → variant dict, built once per cache refresh. Used by all
        per-universe filters so they don't each rebuild an 89k-entry dict."""
        if self._variants_by_symbol is None:
            variants = self.get_exchange_variants()
            self._variants_by_symbol = {d["symbol"]: d for d in variants}
        return self._variants_by_symbol

    def _has_database_config(self) -> bool:
        required = ("host", "database", "username", "password")
        missing = [f for f in required if not self.db_config.get(f)]
        if missing and self.use_database:
            self.logger.warning(
                "database_config incomplete — missing %s; DB source skipped. "
                "Set these in meltano config or via env vars.",
                ", ".join(missing),
            )
        return not missing

    def _has_csv_file(self) -> bool:
        """Check if CSV file exists."""
        if not self.csv_path:
            return False

        csv_file_path = Path(self.csv_path)
        if not csv_file_path.is_absolute():
            csv_file_path = Path.cwd() / csv_file_path

        return csv_file_path.exists() and csv_file_path.is_file()

    def _load_from_database(self) -> t.Optional[t.List[t.Dict[str, t.Any]]]:
        """Load exchange variants from PostgreSQL database."""
        try:
            import psycopg2
            from psycopg2 import sql
            from psycopg2.extras import RealDictCursor
        except ImportError:
            self.logger.error(
                "psycopg2 not installed. Install with: uv add psycopg2-binary"
            )
            return None

        try:
            connection_params = {
                "host": self.db_config.get("host"),
                "port": self.db_config.get("port", 5432),
                "database": self.db_config.get("database"),
                "user": self.db_config.get("username"),
                "password": self.db_config.get("password"),
            }

            schema = self.db_config.get("schema", "public")
            table = self.db_config.get("table", "exchange_variants")

            # Use psycopg2.sql.Identifier rather than f-string interpolation:
            # config-sourced identifiers are still untrusted at the SQL layer
            # (typo/escape errors, future config-from-env-var). Identifier()
            # quotes them safely.
            query = sql.SQL(
                "SELECT DISTINCT symbol, currency, cik, exchange, "
                "exchange_short_name, industry, country, ipo_date, is_fund "
                "FROM {schema}.{table} ORDER BY symbol"
            ).format(
                schema=sql.Identifier(schema),
                table=sql.Identifier(table),
            )

            with psycopg2.connect(**connection_params) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(query)
                    rows = cursor.fetchall()

                    return [dict(row) for row in rows]

        except Exception as e:
            self.logger.error(f"Database loading failed: {e}")
            return None

    def _load_from_csv(self) -> t.Optional[t.List[t.Dict[str, t.Any]]]:
        """Load exchange variants from CSV file."""
        try:
            csv_file_path = Path(self.csv_path)
            if not csv_file_path.is_absolute():
                csv_file_path = Path.cwd() / csv_file_path

            data = []
            with open(csv_file_path, "r", encoding="utf-8") as file:
                reader = csv.DictReader(file)
                for row in reader:
                    clean_row = {}
                    for key, value in row.items():
                        if key and not key.startswith("Unnamed"):
                            if value in ("True", "False"):
                                value = value == "True"
                            elif value == "":
                                value = None
                            clean_row[key] = value

                    if clean_row:
                        data.append(clean_row)

            self.logger.info(f"Loaded {len(data)} exchange variants from CSV")
            return data

        except Exception as e:
            self.logger.error(f"CSV loading failed: {e}")
            return None

    def clear_cache(self) -> None:
        """Clear the internal cache."""
        with self._cache_lock:
            self._cache = []
            self._cache_timestamp = None
            self._variants_by_symbol = None
            self.logger.info("Exchange variants cache cleared")

    def get_cache_info(self) -> t.Dict[str, t.Any]:
        """Get information about current cache state."""
        with self._cache_lock:
            return {
                "cache_size": len(self._cache),
                "cache_timestamp": (
                    self._cache_timestamp.isoformat() if self._cache_timestamp else None
                ),
                "is_valid": self._is_cache_valid(),
                "expires_at": (
                    (
                        self._cache_timestamp + timedelta(hours=self.cache_ttl_hours)
                    ).isoformat()
                    if self._cache_timestamp
                    else None
                ),
                "source_priority": self._get_source_priority(),
            }

    def _get_source_priority(self) -> t.List[str]:
        """Get the current source priority list."""
        priority = []
        if self.use_database and self._has_database_config():
            priority.append("database")
        if self.use_csv and self._has_csv_file():
            priority.append("csv")
        return priority
