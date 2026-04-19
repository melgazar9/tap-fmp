"""REST client handling, including FmpRestStream base class."""

from abc import ABC
import backoff
from datetime import datetime, timedelta
import re
from singer_sdk.helpers.types import Context
from singer_sdk.streams import RESTStream
from singer_sdk import Tap
from tap_fmp.helpers import clean_json_keys, generate_surrogate_key
from tap_fmp.mixins import (
    BaseSymbolPartitionMixin,
    CompanySymbolPartitionMixin,
)
import typing as t
import logging
import requests
from singer_sdk.exceptions import ConfigValidationError
import threading
import time
import random
import csv
import io
from functools import cached_property

# Tokens matched by the Dagster sensor wired to ERROR-level log lines.
# Tests import these constants instead of re-spelling the strings.
SCHEMA_DRIFT_TOKEN = "*** SCHEMA_DRIFT ***"
DATA_TRUNCATED_TOKEN = "*** DATA_TRUNCATED ***"

# Process-global dedup for schema-drift alerts. Keyed by (stream_name,
# frozenset(missing_fields)) so each unique drift fires exactly one ERROR log
# per process. Without this, a drifted field would log millions of duplicate
# lines (one per record) and flood Dagster.
_SCHEMA_DRIFT_SEEN: set[tuple[str, frozenset[str]]] = set()
_SCHEMA_DRIFT_LOCK = threading.Lock()


class FmpRestStream(RESTStream, ABC):
    """FMP stream class with symbol partitioning support."""

    _paginate = False
    _add_surrogate_key = False
    _max_pages = 10000
    _paginate_key = "page"
    _replication_key_starting_name = "from"
    _replication_key_ending_name = "to"
    _expect_csv = False

    def __init__(self, tap: Tap) -> None:
        super().__init__(tap)
        self.parse_config_params()

        self._min_interval = float(self.config.get("min_throttle_seconds", 0.01))
        self._throttle_lock = threading.Lock()
        self._last_call_ts = 0.0

    @property
    def stream_config(self) -> dict:
        """Get configuration for this specific stream."""
        return self.config.get(self.name, {})

    @property
    def url_base(self) -> str:
        return self.config.get("base_url", "https://financialmodelingprep.com")

    def get_url(self, context):
        raise ValueError("get_url must be overridden in the stream class.")

    def parse_config_params(self) -> None:
        cfg_params = self.config.get(self.name)
        self.path_params = {}
        self.query_params = {}
        self.other_params = {}

        if not cfg_params:
            logging.warning(f"No config set for stream '{self.name}', using defaults.")
        elif isinstance(cfg_params, dict):
            self.path_params = cfg_params.get("path_params", {})
            self.query_params = cfg_params.get("query_params", {})
            self.other_params = cfg_params.get("other_params", {})
        elif isinstance(cfg_params, list):
            for params_dict in cfg_params:
                if not isinstance(params_dict, dict):
                    raise ConfigValidationError(
                        f"Expected dict in '{self.name}', but got {type(params_dict)}: {params_dict}"
                    )
                self.path_params.update(params_dict.get("path_params", {}))
                self.query_params.update(params_dict.get("query_params", {}))
                self.other_params.update(params_dict.get("other_params", {}))
        else:
            raise ConfigValidationError(
                f"Config key '{self.name}' must be a dict or list of dicts."
            )

        self.query_params["apikey"] = self.config.get("api_key")

    @cached_property
    def _schema_field_set(self) -> frozenset[str]:
        return frozenset(self.schema.get("properties", {}).keys())

    def _resolve_name_partitions(self, output_key: str = "name") -> list[dict]:
        """Build a `[{output_key: name}, ...]` partition list from
        `query_params.name` (singular) or `stream_config.other_params.names`
        (list). Raises `ConfigValidationError` if both are set or if neither
        is set, since FMP exposes no way to enumerate valid name values for
        these streams."""
        query_name = self.query_params.get("name")
        other_names = self.stream_config.get("other_params", {}).get("names")
        if query_name and other_names:
            raise ConfigValidationError(
                f"Stream {self.name}: specify either query_params.name OR "
                f"other_params.names, not both."
            )
        if query_name:
            names = [query_name] if isinstance(query_name, str) else query_name
            return [{output_key: n} for n in names]
        if other_names:
            names = other_names if isinstance(other_names, list) else [other_names]
            return [{output_key: n} for n in names]
        raise ConfigValidationError(
            f"Stream {self.name} requires query_params.name or "
            f"other_params.names in meltano.yml. FMP doesn't expose a way to "
            f"enumerate valid name values for this stream."
        )

    def _check_missing_fields(self, record: dict):
        """Log SCHEMA_DRIFT once per unique (stream, missing-field-set) when
        the record carries fields absent from the declared schema. Also emits
        a DEBUG log for schema fields absent from the record (only if DEBUG is
        enabled, since it runs on every record)."""
        schema_fields = self._schema_field_set
        record_keys = record.keys()
        missing_in_schema = record_keys - schema_fields

        if logging.getLogger().isEnabledFor(logging.DEBUG):
            missing_in_record = schema_fields - record_keys
            if missing_in_record:
                logging.debug(
                    f"Missing fields in record that are present in schema: "
                    f"{missing_in_record} for stream {self.name}"
                )

        if missing_in_schema:
            key = (self.name, frozenset(missing_in_schema))
            with _SCHEMA_DRIFT_LOCK:
                already_seen = key in _SCHEMA_DRIFT_SEEN
                if not already_seen:
                    _SCHEMA_DRIFT_SEEN.add(key)
            if not already_seen:
                self.logger.error(
                    f"{SCHEMA_DRIFT_TOKEN} stream={self.name} "
                    f"missing_fields={sorted(missing_in_schema)} "
                    f"action=add_to_schema"
                )

    def get_starting_timestamp(self, context: Context | None) -> str | None:
        """
        Determine the starting timestamp for incremental streams.

        Priority:
        1. If replication_key bookmark exists in state, use it
        2. Otherwise, use stream-specific start date from query_params or global start_date

        Works with different replication key types: timestamp, date, year, part, etc.
        """
        if self.replication_method != "INCREMENTAL":
            return None

        state = self.get_context_state(context)
        if state.get("replication_key_value"):
            return self._format_replication_key(state["replication_key_value"])
        elif state.get("starting_replication_value"):
            return self._format_replication_key(state["starting_replication_value"])

        query_params = self.stream_config.get("query_params", {})

        stream_start = None
        possible_keys = [
            self.replication_key,
            self._replication_key_starting_name,
            "start_date",  # fallback
        ]

        for key in possible_keys:
            if key in query_params:
                stream_start = query_params[key]
                break

        global_start = self.config.get("start_date")

        # Use stream config first, then global fallback
        return stream_start or global_start

    @staticmethod
    def redact_api_key(msg):
        msg_str = str(msg)
        return re.sub(r"(apikey=)[^&\s]+", r"\1<REDACTED>", msg_str)

    def _throttle(self) -> None:
        with self._throttle_lock:
            now = time.time()
            wait = self._last_call_ts + self._min_interval - now
            if wait > 0:
                time.sleep(wait + random.uniform(0, 0.1))
            self._last_call_ts = now

    def _fetch_with_retry(
        self, url: str, query_params: dict, page: int | None = None
    ) -> list[dict]:
        """Centralized API call with retry logic."""

        max_retries = self.other_params.get("max_retries", 12)

        @backoff.on_exception(
            backoff.expo,
            (requests.exceptions.RequestException,),
            base=5,
            max_value=300,
            jitter=backoff.full_jitter,
            max_tries=max_retries,
            max_time=1800,
            giveup=lambda e: (
                isinstance(e, requests.exceptions.HTTPError)
                and e.response is not None
                and e.response.status_code not in (429, 500, 502, 503, 504)
            ),
            on_backoff=lambda details: logging.warning(
                f"API request failed, retrying in {details['wait']:.1f}s "
                f"(attempt {details['tries']}): {details['exception']}"
            ),
        )
        def fetch_with_backoff():
            return self._make_http_request(url, query_params, page)

        return fetch_with_backoff()

    def _make_http_request(
        self, url: str, query_params: dict, page: int | None = None
    ) -> list[dict]:
        """Make the HTTP request and process response."""

        if page is not None:
            query_params[self._paginate_key] = page
        log_url = self.redact_api_key(url)
        log_params = {
            k: ("<REDACTED>" if k == "apikey" else v) for k, v in query_params.items()
        }
        logging.info(
            f"Stream {self.name}: Requesting: {log_url} with params: {log_params}"
        )
        query_params = {} if query_params is None else query_params
        try:
            self._throttle()

            if self._expect_csv:
                timeout = (
                    10000,
                    12000,
                )  # allow significant increase in request timeout for bulk requests
            else:
                timeout = (20, 60)

            response = self.requests_session.get(
                url, params=query_params, timeout=timeout
            )

            if (
                response.status_code == 400 and response.text == "[]"
            ):  # bulk streams may return 400 and '[]' on the last 'part'
                return []

            response.raise_for_status()

            if self._expect_csv:
                reader = csv.DictReader(io.StringIO(response.text))
                records = [row for row in reader]
            else:
                records = response.json()

            if isinstance(records, dict) and len(records):
                records = [records]

            records = clean_json_keys(records)
            logging.info(
                f"Stream {self.name}: Records returned: {len(records) if isinstance(records, list) else 'not a list'}"
            )
            return records
        except requests.exceptions.RequestException as e:
            redacted_url = self.redact_api_key(e.request.url)
            error_message = (
                f"{e.response.status_code} Client Error: {e.response.reason} for url: {redacted_url}"
                if e.response and e.request
                else self.redact_api_key(str(e))
            )
            error_message = self.redact_api_key(error_message)
            raise requests.exceptions.HTTPError(
                error_message,
                response=e.response,
                request=e.request,
            )

    def _set_configured_page(self):
        self.configured_page = None
        if self._paginate_key in self.query_params:
            self.configured_page = self.query_params[self._paginate_key]
        elif self._paginate_key in self.other_params:
            self.configured_page = self.other_params[self._paginate_key]

        if self.configured_page is not None:
            logging.info(
                f"Using configured page {self.configured_page} for stream {self.name}"
            )
        return self

    def _handle_pagination(
        self, url: str, query_params: dict, context: Context | None = None
    ) -> t.Iterable[dict]:
        self._set_configured_page()
        page = self.configured_page if self.configured_page is not None else 0
        consecutive_empty_pages = 0
        max_consecutive_empty = 2

        max_page = (
            self.configured_page
            if self.configured_page is not None
            else self._max_pages
        )

        while page <= max_page:
            records = self._fetch_with_retry(url, query_params, page)

            if not isinstance(records, list):
                self.logger.warning(
                    f"Expected list response, got {type(records)}. Stopping pagination."
                )
                break

            if not records:
                consecutive_empty_pages += 1
                if consecutive_empty_pages >= max_consecutive_empty:
                    self.logger.info(
                        f"Stopping pagination after {consecutive_empty_pages} consecutive empty pages"
                    )
                    break
            else:
                consecutive_empty_pages = 0

            for record in records:
                record = self.post_process(record, context)
                self._check_missing_fields(record)
                yield record

            page += 1

        if page > self._max_pages:
            self.logger.warning(
                f"Reached maximum page index ({self._max_pages}, inclusive). Some data may be missing."
            )

    @staticmethod
    def _format_replication_key(replication_key_value):
        return replication_key_value

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        url = self.get_url(context)

        if self._paginate:
            yield from self._handle_pagination(url, self.query_params, context)
        else:
            records = self._fetch_with_retry(url, self.query_params)
            for record in records:
                record = self.post_process(record, context)
                self._check_missing_fields(record)
                yield record

    def post_process(self, record: dict, context: Context | None = None) -> dict:
        if self._add_surrogate_key:
            record["surrogate_key"] = generate_surrogate_key(record)
        return record


class FmpSurrogateKeyStream(FmpRestStream):
    """Base class for streams that need surrogate keys."""

    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True


class BaseSymbolPartitionStream(BaseSymbolPartitionMixin, FmpRestStream):
    """Generic per-symbol stream. Subclasses MUST mix in an asset partition
    mixin (e.g. `CompanySymbolPartitionMixin`, `IndexSymbolPartitionMixin`,
    `EtfSymbolPartitionMixin`) to supply `_partition_symbols`."""

    _symbol_in_path_params = False
    _symbol_in_query_params = True

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        assert self._symbol_in_path_params or self._symbol_in_query_params

        query_params = self.query_params.copy()
        path_params = self.path_params.copy()

        if self._symbol_in_query_params:
            query_params["symbol"] = context["symbol"]
        if self._symbol_in_path_params:
            path_params["symbol"] = context["symbol"]

        url = self.get_url(context)

        if self._paginate:
            yield from self._handle_pagination(url, query_params, context)
        else:
            records = self._fetch_with_retry(url, query_params)
            for record in records:
                record = self.post_process(record, context)
                self._check_missing_fields(record)
                yield record


class CompanySymbolPartitionStream(
    CompanySymbolPartitionMixin, BaseSymbolPartitionStream
):
    """Per-symbol stream over the company stock universe. See
    `CompanySymbolPartitionMixin` for the `_raw_partition` opt-in."""


class SymbolPeriodPartitionStream(FmpSurrogateKeyStream):

    @staticmethod
    def _get_periods(periods):
        if periods is None or periods == "*":
            periods = ["Q1", "Q2", "Q3", "Q4", "FY", "annual", "quarter"]
        if isinstance(periods, str):
            periods = [periods]
        if isinstance(periods, (list, tuple, set)):
            return list(periods)
        raise ConfigValidationError(
            "period(s) must be a string, list/tuple/set, or '*'"
        )

    @property
    def partitions(self):
        periods = self.stream_config.get("query_params", {}).get(
            "period"
        ) or self.stream_config.get("other_params", {}).get("periods")

        periods = self._get_periods(periods)
        symbols = self._tap.get_cached_company_symbols()

        if not periods:
            return [{"symbol": s["symbol"]} for s in symbols]

        return [
            {"symbol": s["symbol"], "period": period}
            for s in symbols
            for period in periods
        ]

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        self.query_params.update(context)
        return super().get_records(context)


class TimeSliceStream(FmpRestStream):
    replication_key = "date"
    replication_method = "INCREMENTAL"

    def create_time_slice_chunks(self, context: Context) -> list[tuple[str, str]]:
        """Generate (from, to) date ranges for the API, as list of (start, end) ISO strings."""
        tap_cfg = self.config

        if self.replication_method == "INCREMENTAL":
            start = self.get_starting_timestamp(context)
            if start:
                start_dt = datetime.fromisoformat(start).date()
            else:
                start_dt = datetime(1970, 1, 1).date()
        else:
            start_dt = datetime(1970, 1, 1).date()

        window_days = int(self.stream_config.get("time_slice_days", 90))

        query_params = self.stream_config.get("query_params", {})
        start_date_cfg = query_params.get(
            self._replication_key_starting_name
        ) or tap_cfg.get("start_date")

        end_date = query_params.get("to") or datetime.now().strftime("%Y-%m-%d")

        if start_date_cfg:
            config_start_dt = datetime.fromisoformat(start_date_cfg).date()
            start_date = max(start_dt, config_start_dt).strftime("%Y-%m-%d")
        else:
            start_date = start_dt.strftime("%Y-%m-%d")

        if not start_date:
            raise ConfigValidationError(f"Missing start_date for {self.name}")

        start_dt = datetime.fromisoformat(start_date)
        end_dt = datetime.fromisoformat(end_date)

        if start_dt > end_dt:
            # If start_date from bookmark is after configured end_date, use start_date as both start and end
            # This handles the case where incremental processing has moved beyond the configured end window
            end_dt = start_dt
            end_date = start_date

        slices = []
        current = start_dt
        while current < end_dt:
            slice_end = min(current + timedelta(days=window_days), end_dt)
            slices.append(
                (current.strftime("%Y-%m-%d"), slice_end.strftime("%Y-%m-%d"))
            )
            current = slice_end
        return slices

    def fetch_window(
        self,
        url,
        query_params,
        from_date,
        to_date,
        max_records,
        context: Context | None = None,
    ):
        """
        Recursively fetch all records for a window, splitting if we hit the max_records limit.
        """
        query_params = query_params.copy()
        query_params[self._replication_key_starting_name] = from_date
        query_params[self._replication_key_ending_name] = to_date

        records = self._fetch_with_retry(url, query_params)
        if len(records) < max_records:
            for record in records:
                record = self.post_process(record, context)
                self._check_missing_fields(record)
                yield record
        else:
            from_dt = datetime.fromisoformat(from_date)
            to_dt = datetime.fromisoformat(to_date)
            if (to_dt - from_dt).days <= 1:
                # Window can't be split further but FMP returned at the limit,
                # implying truncation. Yield what we have so the partial window
                # isn't lost, but escalate so a Dagster sensor can catch it and
                # the user can refetch with a smaller window or higher limit.
                self.logger.error(
                    f"{DATA_TRUNCATED_TOKEN} stream={self.name} "
                    f"window={from_date}..{to_date} "
                    f"symbol={query_params.get('symbol')} "
                    f"records_returned={len(records)} "
                    f"max_records_per_request={max_records} "
                    f"action=refetch_with_smaller_window_or_higher_limit"
                )
                for record in records:
                    record = self.post_process(record, context)
                    self._check_missing_fields(record)
                    yield record
                return
            mid_dt = from_dt + (to_dt - from_dt) // 2
            mid_date = mid_dt.strftime("%Y-%m-%d")
            # Fetch left half
            yield from self.fetch_window(
                url, query_params, from_date, mid_date, max_records, context
            )
            # Fetch right half
            yield from self.fetch_window(
                url, query_params, mid_date, to_date, max_records, context
            )

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        query_params = self.query_params.copy()

        url = self.get_url(context)

        time_slices = self.create_time_slice_chunks(context)
        max_records = self.stream_config.get("other_params", {}).get(
            "max_records_per_request", 4000
        )

        # No try/except around fetch_window: a transient API failure must
        # propagate so Singer leaves state at the last fully-completed window.
        # Catching here would silently skip the failed window and advance the
        # bookmark on subsequent windows' records, leaving a permanent gap.
        for from_date, to_date in time_slices:
            yield from self.fetch_window(
                url, query_params, from_date, to_date, max_records, context
            )


class BaseSymbolPartitionTimeSliceStream(BaseSymbolPartitionMixin, TimeSliceStream):
    """Generic per-symbol time-slice stream. Subclasses MUST mix in an asset
    partition mixin to supply `_partition_symbols`."""

    _symbol_in_path_params = False
    _symbol_in_query_params = True

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        assert self._symbol_in_path_params or self._symbol_in_query_params

        query_params = self.query_params.copy()
        path_params = self.path_params.copy()

        if self._symbol_in_query_params:
            query_params["symbol"] = context["symbol"]
        if self._symbol_in_path_params:
            path_params["symbol"] = context["symbol"]

        url = self.get_url(context)
        time_slices = self.create_time_slice_chunks(context)
        max_records = self.stream_config.get("other_params", {}).get(
            "max_records_per_request", 4000
        )

        # See note above: no try/except here. Failures must propagate so Singer
        # bookmark stays at last fully-completed window.
        for from_date, to_date in time_slices:
            yield from self.fetch_window(
                url, query_params, from_date, to_date, max_records, context
            )


class CompanySymbolPartitionTimeSliceStream(
    CompanySymbolPartitionMixin, BaseSymbolPartitionTimeSliceStream
):
    """Per-symbol time-slice stream defaulting to the company stock universe."""


class IncrementalDateStream(FmpSurrogateKeyStream):
    replication_key = "date"
    replication_method = "INCREMENTAL"

    def _format_replication_key(self, replication_key_value):
        if isinstance(replication_key_value, str):
            try:
                # Try parsing as date string and return as YYYY-MM-DD
                if "T" in replication_key_value or "Z" in replication_key_value:
                    # ISO format with time
                    output_value = datetime.fromisoformat(
                        replication_key_value.replace("Z", "+00:00")
                    ).strftime("%Y-%m-%d")
                else:
                    # Already in YYYY-MM-DD format
                    output_value = replication_key_value
                return output_value
            except ValueError:
                pass
        raise ValueError(f"Could not format replication key for stream {self.name}")

    def _get_dates_dict(self):
        if "date" in self.query_params:
            return [{"date": self.query_params.get("date")}]

        query_params = self.stream_config.get("query_params", {})
        other_params = self.stream_config.get("other_params", {})
        date_range = other_params.get("date_range")

        if "date" in query_params and "date_range" in other_params:
            raise ConfigValidationError(
                "Cannot specify both 'date' and 'date_range' in query_params and other_params."
            )

        if not date_range:
            date_range = ["1990-01-01", datetime.today().date().strftime("%Y-%m-%d")]

        assert isinstance(date_range, list), "date_range must be a list"

        if len(date_range) == 1 or date_range[-1] == "today":
            date_range = date_range + [datetime.today().date().strftime("%Y-%m-%d")]

        # Generate all dates in range
        all_dates = []
        current_date = datetime.fromisoformat(date_range[0])
        end_date = datetime.fromisoformat(date_range[-1])
        while current_date <= end_date:
            all_dates.append({"date": current_date.strftime("%Y-%m-%d")})
            current_date += timedelta(days=1)

        return all_dates

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        dates_dict = self._get_dates_dict()
        starting_date = self.get_starting_timestamp(context)
        # `get_starting_timestamp` returns None on cold start (no bookmark and
        # no configured start_date). `d["date"] >= None` raises TypeError, so
        # treat None as "from the beginning" and pass every date through.
        if starting_date is None:
            filtered_dates = dates_dict
        else:
            filtered_dates = [d for d in dates_dict if d["date"] >= starting_date]

        for date_dict in filtered_dates:
            self.query_params.update(date_dict)
            yield from super().get_records(context)


class IncrementalYearStream(FmpSurrogateKeyStream):
    """Base class for streams that iterate over years incrementally."""

    replication_key = "year"
    replication_method = "INCREMENTAL"

    def _format_replication_key(self, replication_key_value):
        """Convert various year formats to integer year."""
        if (
            isinstance(replication_key_value, int)
            and 1900 <= replication_key_value <= 3000
        ):
            return replication_key_value
        elif isinstance(replication_key_value, str):
            try:
                # Try parsing as date string first
                if "-" in replication_key_value:
                    return datetime.fromisoformat(
                        replication_key_value.split("T")[0]
                    ).year
                # Try parsing as plain year
                year = int(replication_key_value)
                if 1900 <= year <= 3000:
                    return year
                else:
                    raise ValueError(f"Year {year} out of valid range")
            except (ValueError, TypeError):
                pass
        raise ValueError(
            f"Could not format replication key value '{replication_key_value}' as year for stream {self.name}"
        )

    @property
    def partitions(self):
        """Get year partitions for iteration."""
        # If year is explicitly set in query_params, use that single year
        if "year" in self.query_params:
            return [{"year": self.query_params.get("year")}]

        other_params = self.stream_config.get("other_params", {})

        current_year = datetime.today().year
        default_start_year = 1970

        global_start_date = self.config.get("start_date")
        if global_start_date:
            try:
                global_start_year = datetime.fromisoformat(global_start_date).year
                default_start_year = max(default_start_year, global_start_year)
            except (ValueError, TypeError):
                logging.warning(
                    f"Could not parse start_date '{global_start_date}', using default"
                )

        all_years = [{"year": y} for y in range(default_start_year, current_year + 1)]

        if other_params:
            years_config = other_params.get("years")
            if years_config == "*":
                return all_years
            elif isinstance(years_config, list):
                if years_config:
                    try:
                        year_ints = [int(y) for y in years_config]
                        return [{"year": y} for y in sorted(year_ints)]
                    except (ValueError, TypeError) as e:
                        raise ValueError(
                            f"Invalid year format in stream {self.name}: {years_config}. Error: {self.redact_api_key(e)}"
                        )
                else:
                    raise ValueError(f"Empty years list for stream {self.name}")
            elif years_config is None:
                return all_years
            else:
                raise ValueError(
                    f"Years must be '*', a list, or None, got {type(years_config)} for stream {self.name}"
                )
        else:
            return all_years

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Update query params with year from context and delegate to parent."""
        if context and "year" in context:
            self.query_params["year"] = context["year"]
        yield from super().get_records(context)

    def post_process(self, record: dict, context: Context | None = None) -> dict:
        """Inject year into record if not present."""
        if context and "year" in context and "year" not in record:
            record["year"] = context["year"]
        elif "year" in self.query_params and "year" not in record:
            record["year"] = self.query_params["year"]
        return super().post_process(record, context)


class BaseSymbolYearPartitionStream(CompanySymbolPartitionStream):
    """Abstract base class for streams that partition by symbol and year with configurable quarter/period logic."""

    replication_method = "INCREMENTAL"
    replication_key = "date"

    # Configurable class attributes - override in subclasses
    _partition_field_name: str = None  # "quarter" or "period"
    _partition_values: list = None  # [1,2,3,4] or ["Q1","Q2","Q3","Q4"]

    @property
    def partitions(self):
        """Get symbol + year + quarter/period partition combinations."""
        query_params = self.stream_config.get("query_params", {})
        other_params = self.stream_config.get("other_params", {})

        symbols = self._partition_symbols()

        # Get years (default: current year only)
        if "year" in query_params:
            years = [int(query_params["year"])]
        elif "years" in other_params:
            years = [int(y) for y in other_params["years"]]
        else:
            years = [int(y) for y in range(1970, datetime.today().year + 1)]

        # Get partition values (quarters/periods)
        if self._partition_field_name in query_params:
            partition_values = [query_params[self._partition_field_name]]
        elif f"{self._partition_field_name}s" in other_params:
            partition_values = other_params[f"{self._partition_field_name}s"]
        else:
            partition_values = self._partition_values

        assert years is not None, f"Years cannot be None for stream {self.name}."
        assert (
            partition_values is not None
        ), f"Must set partition values in meltano.yml or as a stream class attribute for stream {self.name}."

        partitions = []
        for symbol in symbols:
            for year in years:
                for partition_value in partition_values:
                    partitions.append(
                        {
                            "symbol": symbol["symbol"],
                            "year": year,
                            self._partition_field_name: partition_value,
                        }
                    )
        return partitions

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Update query params with year and partition value from context."""
        if context:
            if "year" in context:
                self.query_params["year"] = context["year"]
            if self._partition_field_name in context:
                self.query_params[self._partition_field_name] = context[
                    self._partition_field_name
                ]
        yield from super().get_records(context)

    def post_process(self, record: dict, context: Context | None = None) -> dict:
        """Inject year and partition field into record if not present."""
        if context:
            if "year" in context and "year" not in record:
                record["year"] = context["year"]
            if (
                self._partition_field_name in context
                and self._partition_field_name not in record
            ):
                record[self._partition_field_name] = context[self._partition_field_name]
        return super().post_process(record, context)


class SymbolYearQuarterPartitionStream(BaseSymbolYearPartitionStream):
    """Stream that partitions by symbol, year, and quarter (integer: 1, 2, 3, 4)."""

    _partition_field_name = "quarter"
    _partition_values = [1, 2, 3, 4]


class SymbolYearPeriodPartitionStream(BaseSymbolYearPartitionStream):
    """Stream that partitions by symbol, year, and period (string: Q1, Q2, Q3, Q4)."""

    _partition_field_name = "period"
    _partition_values = ["Q1", "Q2", "Q3", "Q4"]
