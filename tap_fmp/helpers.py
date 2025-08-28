import re
import uuid
from pathlib import Path
from datetime import datetime, timedelta
import logging
import threading
import typing as t
import csv


def clean_strings(lst):
    cleaned_list = []
    for s in lst:
        cleaned = re.sub(r"[^a-zA-Z0-9_]", "_", s)
        cleaned = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", cleaned)
        cleaned = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", cleaned)
        cleaned = re.sub(r"_+", "_", cleaned).strip("_").lower()
        cleaned_list.append(cleaned)
    return cleaned_list


def clean_json_keys(data: list[dict]) -> list[dict]:
    def clean_nested_dict(obj):
        if isinstance(obj, dict):
            return {
                new_key: clean_nested_dict(value)
                for new_key, value in zip(clean_strings(obj.keys()), obj.values())
            }
        elif isinstance(obj, list):
            return [clean_nested_dict(item) for item in obj]
        else:
            return obj

    return [clean_nested_dict(d) for d in data]


def generate_surrogate_key(data: dict, namespace=uuid.NAMESPACE_DNS) -> str:
    key_values = [str(data.get(field, "")) for field in data.keys()]
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

        self.exchange_config = config.get("exchange_variants_source", {})
        self.db_config = config.get("database_config", {})

        self.use_csv = self.exchange_config.get("use_csv", True)
        self.csv_path = self.exchange_config.get(
            "csv_path", "distinct_exchange_variants.csv"
        )
        self.use_database = self.exchange_config.get("use_database", False)
        self.cache_ttl_hours = self.exchange_config.get("cache_ttl_hours", 72)

    def get_exchange_variants(self) -> t.List[t.Dict[str, t.Any]]:
        """Get exchange variants using priority: DB > CSV > API."""
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
                else:
                    self.logger.warning("Database loading failed, trying next source")

            if self.use_csv and self._has_csv_file():
                self.logger.info("Loading exchange variants from CSV")
                data = self._load_from_csv()
                if data:
                    self._update_cache(data)
                    return self._cache
                else:
                    self.logger.warning("CSV loading failed, falling back to API")

            # Fallback to API (existing behavior)
            self.logger.info("Loading exchange variants from API (fallback)")
            data = self._load_from_api()
            if data:
                self._update_cache(data)
                return self._cache
            else:
                raise RuntimeError("All exchange variants sources failed")

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
        self.logger.info(f"Cache updated with {len(data)} exchange variants")

    def _has_database_config(self) -> bool:
        """Check if database configuration is complete."""
        required_fields = ["host", "database", "username", "password"]
        return all(
            self.db_config.get(field) is not None and self.db_config.get(field) != ""
            for field in required_fields
        )

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

            with psycopg2.connect(**connection_params) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    query = f"""
                        SELECT DISTINCT
                            symbol, currency, cik, exchange, exchange_short_name,
                            industry, country, ipo_date, is_fund
                        FROM {schema}.{table}
                        ORDER BY symbol
                    """
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

    def _load_from_api(self) -> t.Optional[t.List[t.Dict[str, t.Any]]]:
        """Load exchange variants from API (existing fallback behavior)."""
        try:
            if not self.tap_instance:
                self.logger.error("No tap instance available for API fallback")
                return None

            # Import here to avoid circular imports
            from tap_fmp.streams.search_streams import ExchangeVariantsStream

            self.logger.info("Loading exchange variants from FMP API...")
            exchange_variants_stream = ExchangeVariantsStream(self.tap_instance)
            data = list(exchange_variants_stream.get_records(context=None))

            self.logger.info(f"Loaded {len(data)} exchange variants from API")
            return data

        except Exception as e:
            self.logger.error(f"API loading failed: {e}")
            return None

    def clear_cache(self) -> None:
        """Clear the internal cache."""
        with self._cache_lock:
            self._cache = []
            self._cache_timestamp = None
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
        priority.append("api")
        return priority
