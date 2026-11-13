"""REST client handling, including FmpRestStream base class."""

from abc import ABC
import backoff
from datetime import datetime, timedelta
import json
import re
from singer_sdk.helpers.types import Context
from singer_sdk.streams import RESTStream
from singer_sdk import Tap
from tap_fmp.helpers import clean_json_keys, generate_surrogate_key
from tap_fmp.mixins import (
    BaseSymbolPartitionMixin,
    CompanySymbolPartitionMixin,
)
from tap_fmp.prefetch import PartitionPlan, PartitionPrefetcher
from tap_fmp.schema_drift import SchemaDriftMonitor
import typing as t
import logging
import requests
from requests.adapters import HTTPAdapter
from singer_sdk.exceptions import ConfigValidationError
import sys
import threading
import time
import random
import csv
import io
from functools import cached_property

# Tests import this constant instead of re-spelling the string.
DATA_TRUNCATED_TOKEN = "*** DATA_TRUNCATED ***"


class FmpQuotaExhaustedError(RuntimeError):
    """FMP kept refusing one request longer than the stream waits out a 429:
    the plan's 30-day bandwidth cap, its daily quota or a bulk endpoint's call
    limit is spent, and retrying only waits."""


class FmpErrorReplyError(RuntimeError):
    """FMP answered with an error message in place of data, under a success status."""


class YearWindow:
    """Years a year-partitioned stream requests: a configured list is a
    backfill; none configured means only the trailing window, since the
    history before it is already loaded."""

    # calendar years requested when none are configured; late filings and amendments land inside it
    TRAILING_YEARS: t.ClassVar[int] = 2

    @staticmethod
    def configured(configured: t.Any) -> list[int] | None:
        """The configured years, one or a list; None when unset or "*" asks for the stream's default."""
        if not configured or configured in ("*", ["*"]):
            return None
        if isinstance(configured, (str, int)):
            configured = [configured]
        return [int(year) for year in configured]

    @classmethod
    def resolve(cls, configured: t.Any, start_date: str) -> list[int]:
        """`start_date` is the floor: no year before its year is requested."""
        first_year = datetime.fromisoformat(str(start_date)[:10]).year
        years = cls.configured(configured)
        if years is None:
            this_year = datetime.today().year
            years = range(this_year - cls.TRAILING_YEARS + 1, this_year + 1)
        return [year for year in years if year >= first_year]


class FmpRestStream(RESTStream, ABC):
    """FMP stream class with symbol partitioning support."""

    # FMP caps paginated endpoints at page index 100 inclusive (docs: "Page maxed at 100").
    # Subclasses on those endpoints override `_max_pages = 100`.
    _paginate = False
    _add_surrogate_key = False
    _max_pages = 10000  # maximum page index, inclusive — tap-side fallback for uncapped endpoints
    _max_consecutive_empty_pages = (
        2  # FMP occasionally returns a single empty page mid-data
    )
    _paginate_key = "page"
    _replication_key_starting_name = "from"
    _replication_key_ending_name = "to"
    _expect_csv = False
    _retryable_status_codes = frozenset({429, 500, 502, 503, 504})
    _quota_status_code = 429
    # seconds one request may keep drawing 429s: the per-minute limit clears inside it, the 30-day bandwidth cap never does
    _quota_exhausted_after_seconds = 300
    # seconds one request may keep retrying any other failure (timeouts, 5xx)
    _transient_retry_seconds = 1800
    _error_body_max_chars = 500
    _error_reply_key = "Error Message"
    # FMP's reply when the endpoint holds nothing for the request; every other error reply is a fault
    _no_data_reply_prefix = "No Data for this symbol"

    # partition fields sent as request params; copied per request so query_params keeps only the config
    _context_query_params: t.ClassVar[tuple[str, ...]] = ()

    def __init__(self, tap: Tap) -> None:
        self._partition_states: dict[str, dict] = {}
        self._indexed_partitions: list[dict] | None = None
        self._indexed_partition_count = 0
        self._duplicate_partition_keys: set[str] = set()
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
    def _schema_drift(self) -> SchemaDriftMonitor:
        return SchemaDriftMonitor(self.name, self.schema, self.logger)

    def sync(self, context: Context | None = None) -> None:
        super().sync(context)
        self._schema_drift.report()

    def _resolve_name_partitions(self, output_key: str = "name") -> list[dict]:
        """Build a `[{output_key: name}, ...]` partition list from
        `query_params.name` (singular) or `stream_config.other_params.names`
        (list). Raises `ConfigValidationError` if both are set or if neither
        is set, since FMP exposes no way to enumerate valid name values for
        these streams."""
        query_name = self.query_params.get("name")
        other_names = self.other_params.get("names")
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

    def get_context_state(self, context: Context | None) -> dict:
        """The SDK's entry for a partition, from an index of the saved partitions: the SDK scans every
        saved partition per lookup, which outweighs all the stream's other work once a state holds
        tens of thousands.
        """
        partition_context = self._get_state_partition_context(context)
        if not partition_context:
            return super().get_context_state(context)
        key = self._partition_key(partition_context)
        state = self._partition_states.get(key)
        if state is None:
            state = self._saved_partition_state(partition_context)
        if state is None:
            state = {"context": partition_context}
            self._indexed_partitions.append(state)
            self._indexed_partition_count += 1
            self._partition_states[key] = state
        return state

    @staticmethod
    def _partition_key(partition_context: Context) -> str:
        return json.dumps(partition_context, sort_keys=True, default=str)

    def _saved_partition_state(self, partition_context: Context) -> dict | None:
        """A partition's entry in state, or None; never creates one."""
        self._index_saved_partitions()
        key = self._partition_key(partition_context)
        if key in self._duplicate_partition_keys:
            raise ValueError(
                f"State file contains duplicate entries for partition: {partition_context}."
            )
        return self._partition_states.get(key)

    def _index_saved_partitions(self) -> None:
        """One pass over the partitions in state, again only if something other than this stream added one."""
        partitions = self.stream_state.setdefault("partitions", [])
        if (
            partitions is self._indexed_partitions
            and len(partitions) == self._indexed_partition_count
        ):
            return
        self._partition_states.clear()
        self._duplicate_partition_keys.clear()
        for entry in partitions:
            key = self._partition_key(entry["context"])
            if key in self._partition_states:
                self._duplicate_partition_keys.add(key)
            self._partition_states[key] = entry
        # the SDK refuses a partition saved twice whenever it is looked up
        for key in self._duplicate_partition_keys:
            del self._partition_states[key]
        self._indexed_partitions = partitions
        self._indexed_partition_count = len(partitions)

    def get_starting_timestamp(self, context: Context | None) -> str | None:
        """
        Determine the starting timestamp for incremental streams.

        Priority:
        1. If replication_key bookmark exists in state, use it
        2. Otherwise the SDK's starting_replication_value, which is the global start_date: it is a
           floor, so a stream's own start only applies when later (callers take the later of the two)
        3. Without either, the stream-specific start date from query_params, then global start_date

        Works with different replication key types: timestamp, date, year, part, etc.
        """
        if self.replication_method != "INCREMENTAL":
            return None
        return self._starting_timestamp_from(self.get_context_state(context))

    def _starting_timestamp_from(self, state: dict) -> str | None:
        """`get_starting_timestamp` for a partition whose state entry is `state`."""
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
            # Stamped after the sleep: the pre-sleep time let the next call go early and the stream burst past the limit.
            self._last_call_ts = time.time()

    def _is_quota_refusal(self, e: BaseException | None) -> bool:
        response = getattr(e, "response", None)
        return (
            isinstance(e, requests.exceptions.HTTPError)
            and response is not None
            and response.status_code == self._quota_status_code
        )

    @staticmethod
    def _exponential_waits() -> t.Iterator[float]:
        """Exponential seconds with full jitter, for a failure that clears on its own."""
        waits = backoff.expo(base=5, max_value=300)
        next(waits)  # backoff's generators yield once before their first value
        return (backoff.full_jitter(wait) for wait in waits)

    def _quota_retry_waits(self) -> t.Iterator[float]:
        """Seconds before each retry of a 429."""
        return self._exponential_waits()

    def _retry_waits(self) -> t.Generator[float | None, BaseException | None, None]:
        """backoff sends each failure in: a 429 waits the stream's quota pace, any other failure backs off exponentially."""
        quota, transient = self._quota_retry_waits(), self._exponential_waits()
        failure = yield
        while True:
            failure = yield next(
                quota if self._is_quota_refusal(failure) else transient
            )

    def _fetch_with_retry(
        self, url: str, query_params: dict, page: int | None = None
    ) -> list[dict]:
        """Centralized API call with retry logic. A 429 that outlasts
        `_quota_exhausted_after_seconds` raises FmpQuotaExhaustedError."""

        max_retries = self.other_params.get("max_retries", 12)
        first_quota_refusal: list[float] = []

        def give_up(e: Exception) -> bool:
            if self._is_quota_refusal(e):
                if not first_quota_refusal:
                    first_quota_refusal.append(time.monotonic())
                refused_for = time.monotonic() - first_quota_refusal[0]
                return refused_for >= self._quota_exhausted_after_seconds
            response = getattr(e, "response", None)
            return (
                isinstance(e, requests.exceptions.HTTPError)
                and response is not None
                and response.status_code not in self._retryable_status_codes
            )

        @backoff.on_exception(
            self._retry_waits,
            (requests.exceptions.RequestException,),
            jitter=None,  # each wait generator applies its own
            max_tries=max_retries,
            max_time=max(
                self._transient_retry_seconds, self._quota_exhausted_after_seconds
            ),
            giveup=give_up,
            on_backoff=lambda details: logging.warning(
                f"API request failed, retrying in {details['wait']:.1f}s "
                f"(attempt {details['tries']}): {details['exception']}"
            ),
        )
        def fetch_with_backoff():
            return self._make_http_request(url, query_params, page)

        try:
            return fetch_with_backoff()
        except requests.exceptions.HTTPError as e:
            if not self._is_quota_refusal(e):
                raise
            refused_for = time.monotonic() - first_quota_refusal[0]
            raise FmpQuotaExhaustedError(
                f"Stream {self.name}: FMP answered 429 for {refused_for:.0f}s, as long as "
                f"this stream waits out ({self._quota_exhausted_after_seconds}s), so a limit "
                f"past the per-minute one is spent; stopping instead of retrying into it. {e}"
            ) from None

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

            if isinstance(records, dict) and self._error_reply_key in records:
                return self._no_data_or_raise(records[self._error_reply_key], log_url)

            if isinstance(records, dict) and len(records):
                records = [records]

            records = clean_json_keys(records)
            # the size is what the plan's 30-day bandwidth cap counts
            logging.info(
                f"Stream {self.name}: Records returned: {len(records) if isinstance(records, list) else 'not a list'}"
                f" ({len(response.content) / 1e6:.2f} MB)"
            )
            return records
        except requests.exceptions.RequestException as e:
            # `is not None`, not truthiness: a Response is falsy whenever its status is an error.
            if e.response is not None and e.request is not None:
                error_message = (
                    f"{e.response.status_code} Client Error: {e.response.reason} for url: {e.request.url}"
                    f" | response: {e.response.text[:self._error_body_max_chars]}"
                )
            else:
                error_message = str(e)
            error_message = self.redact_api_key(error_message)
            # `from None`: the original exception's message holds the raw
            # URL, and chaining would print it (and the API key) in tracebacks.
            raise requests.exceptions.HTTPError(
                error_message,
                response=e.response,
                request=e.request,
            ) from None

    def _no_data_or_raise(self, message: str, log_url: str) -> list[dict]:
        """An error reply is never a record: FMP's no-data reply is an empty result, any other one stops the stream."""
        if message.startswith(self._no_data_reply_prefix):
            logging.info(f"Stream {self.name}: no data at {log_url}")
            return []
        raise FmpErrorReplyError(
            f"Stream {self.name}: FMP answered {log_url} with an error in place of data: "
            f"{self.redact_api_key(message)[:self._error_body_max_chars]}"
        )

    def _set_configured_page(self):
        if getattr(self, "_configured_page_resolved", False):
            return self

        self.configured_page = None
        if self._paginate_key in self.query_params:
            self.configured_page = self.query_params[self._paginate_key]
        elif self._paginate_key in self.other_params:
            self.configured_page = self.other_params[self._paginate_key]

        if self.configured_page is not None:
            logging.info(
                f"Using configured page {self.configured_page} for stream {self.name}"
            )
        self._configured_page_resolved = True
        return self

    def _handle_pagination(
        self, url: str, query_params: dict, context: Context | None = None
    ) -> t.Iterable[dict]:
        self._set_configured_page()
        page = self.configured_page if self.configured_page is not None else 0
        consecutive_empty_pages = 0

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
                if consecutive_empty_pages >= self._max_consecutive_empty_pages:
                    self.logger.info(
                        f"Stopping pagination after {consecutive_empty_pages} consecutive empty pages"
                    )
                    break
            else:
                consecutive_empty_pages = 0

            yield from self._yield_processed(records, context)
            page += 1

        if self.configured_page is None and page > self._max_pages:
            self.logger.warning(
                f"Reached maximum page index ({self._max_pages}, inclusive). Some data may be missing."
            )

    @staticmethod
    def _format_replication_key(replication_key_value):
        return replication_key_value

    def _request_query_params(self, context: Context | None) -> dict:
        query_params = self.query_params.copy()
        if context:
            query_params.update(
                {field: context[field] for field in self._context_query_params}
            )
        return query_params

    def _yield_processed(
        self, records: list[dict], context: Context | None
    ) -> t.Iterable[dict]:
        for record in records:
            record = self.post_process(record, context)
            self._schema_drift.observe(record)
            yield record

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        url = self.get_url(context)

        if self._paginate:
            yield from self._handle_pagination(url, self.query_params, context)
        else:
            records = self._fetch_with_retry(url, self.query_params)
            yield from self._yield_processed(records, context)

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

        query_params = self._request_query_params(context)
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
            yield from self._yield_processed(records, context)


class CompanySymbolPartitionStream(
    CompanySymbolPartitionMixin, BaseSymbolPartitionStream
):
    """Per-symbol stream over the company stock universe. See
    `CompanySymbolPartitionMixin` for the `_raw_partition` opt-in."""


class SymbolPeriodPartitionStream(CompanySymbolPartitionStream, FmpSurrogateKeyStream):
    """Per-symbol stream asked once per period kind; a symbol mixin listed
    first chooses the symbols, the company universe otherwise."""

    # FMP answers one period kind per request and defaults to annual; annual holds the FY rows, quarter Q1-Q4
    DEFAULT_PERIODS: t.ClassVar[tuple[str, ...]] = ("annual", "quarter")
    # the period values the endpoint accepts; None accepts any
    ALLOWED_PERIODS: t.ClassVar[tuple[str, ...] | None] = None

    @classmethod
    def _get_periods(cls, periods) -> list[str]:
        if periods is None or periods == "*":
            periods = list(cls.DEFAULT_PERIODS)
        if isinstance(periods, str):
            periods = [periods]
        if not isinstance(periods, (list, tuple, set)) or not periods:
            raise ConfigValidationError(
                f"Stream {cls.name}: period(s) must be a non-empty string, list/tuple/set, or '*'"
            )
        invalid = set(periods) - set(cls.ALLOWED_PERIODS or periods)
        if invalid:
            raise ConfigValidationError(
                f"Stream {cls.name}: invalid period(s) {sorted(invalid)}; allowed {sorted(cls.ALLOWED_PERIODS)}"
            )
        return list(periods)

    @property
    def configured_periods(self) -> list[str]:
        """Period kinds requested per symbol: the stream's `period`/`periods` config, else DEFAULT_PERIODS."""
        return self._get_periods(
            self.query_params.get("period") or self.other_params.get("periods")
        )

    @property
    def partitions(self):
        periods = self.configured_periods
        return [
            {**partition, "period": period}
            for partition in super().partitions
            for period in periods
        ]

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        self.query_params.update(context)
        return super().get_records(context)


class TruncationReason:
    """Reason codes for `*** DATA_TRUNCATED ***` log lines. The Dagster
    sensor matches on these substrings — keep stable across releases."""

    SILENT_CAP = "fmp_silent_cap_below_max_records"
    PAGE_CEILING = "hit_page_ceiling_with_data"
    SPLIT_FLOOR = "window_at_min_granularity_still_at_cap"


class TimeSliceStream(FmpRestStream):
    replication_key = "date"
    replication_method = "INCREMENTAL"

    # Last-line defense against silent truncation: `fetch_window`'s halving
    # fires on `max_records_per_request` and page-ceiling hits, not on silent caps.
    _default_time_slice_days: int = 90

    # True where a partition's first row can sit years past its window's start, so a late first row says nothing.
    _sparse_history: bool = False

    # True where the endpoint omits records dated on `from`; the request then starts a day early.
    _exclusive_from: bool = False

    # True where a paged window asks its ceiling page as soon as it looks likely to overflow, so an
    # overflowing window skips the pages in between, whose rows its halves fetch again anyway
    _probe_page_ceiling: bool = False

    _LOG_FIXED_KEYS = frozenset(
        {
            "stream",
            "window",
            "symbol",
            "records_returned",
            "max_records_per_request",
            "reason",
            "action",
        }
    )

    def create_time_slice_chunks(self, context: Context) -> list[tuple[str, str]]:
        """Generate (from, to) date ranges for the API, as list of (start, end) ISO strings."""
        start = (
            self.get_starting_timestamp(context)
            if self.replication_method == "INCREMENTAL"
            else None
        )
        return self._time_slices_from(start)

    def _time_slices_from(self, start: str | None) -> list[tuple[str, str]]:
        """The (from, to) windows of a partition whose starting timestamp is `start`."""
        tap_cfg = self.config
        start_dt = (
            datetime.fromisoformat(start).date()
            if start
            else datetime(1970, 1, 1).date()
        )

        window_days = int(
            self.other_params.get("time_slice_days", self._default_time_slice_days)
        )

        query_params = self.stream_config.get("query_params", {})
        start_date_cfg = query_params.get(
            self._replication_key_starting_name
        ) or tap_cfg.get("start_date")

        # the global end_date is a ceiling over the stream's own `to`, as start_date is a floor under its `from`
        ceilings = [
            str(ceiling)[:10]
            for ceiling in (query_params.get("to"), tap_cfg.get("end_date"))
            if ceiling
        ]
        end_date = min(ceilings) if ceilings else datetime.now().strftime("%Y-%m-%d")

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

    def _log_data_truncated(
        self,
        *,
        from_date: str,
        to_date: str,
        symbol: str | None,
        records_returned: int,
        max_records: int,
        reason: str,
        action: str,
        **extra: object,
    ) -> None:
        collisions = self._LOG_FIXED_KEYS & extra.keys()
        assert not collisions, f"extra keys collide with fixed log fields: {collisions}"
        parts = [
            DATA_TRUNCATED_TOKEN,
            f"stream={self.name}",
            f"window={from_date}..{to_date}",
            f"symbol={symbol}",
            f"records_returned={records_returned}",
            f"max_records_per_request={max_records}",
            f"reason={reason}",
            f"action={action}",
        ]
        for k, v in extra.items():
            parts.append(f"{k}={v}")
        self.logger.error(" ".join(parts))

    def _fetch_paginated_window(
        self,
        url: str,
        query_params: dict,
        *,
        probe_ceiling: bool = False,
        ceiling_first: bool = False,
    ) -> tuple[list[dict], bool]:
        """Aggregate raw records across all pages within a (from, to) window.
        Returns `(records, hit_page_ceiling)`. The ceiling flag is True only
        when the loop exited because we exhausted `_max_pages` with data
        still flowing — caller treats that as truncation. A configured
        single-page fetch never reports ceiling-hit.

        With `probe_ceiling` the ceiling page is asked as soon as an overflow
        is likely: before page 0 when `ceiling_first`, else once a page comes
        back full. Rows there mean the window overflows, so it returns no
        records at once; the caller must split it, as its halves fetch every
        row again. The probed page is never asked twice."""
        self._set_configured_page()
        page = self.configured_page if self.configured_page is not None else 0
        max_page = (
            self.configured_page
            if self.configured_page is not None
            else self._max_pages
        )
        probe = probe_ceiling and self.configured_page is None and max_page > 0
        ceiling_records: list[dict] | None = None
        if probe and ceiling_first:
            ceiling_records = self._fetch_with_retry(url, query_params, max_page)
            if ceiling_records:
                return [], True
        consecutive_empty = 0
        records: list[dict] = []

        while page <= max_page:
            if page == max_page and ceiling_records is not None:
                page_records = ceiling_records
            else:
                page_records = self._fetch_with_retry(url, query_params, page)
            if not isinstance(page_records, list):
                self.logger.warning(
                    f"Expected list response on page {page}, got "
                    f"{type(page_records).__name__}; stopping pagination."
                )
                return records, False
            if (
                probe
                and ceiling_records is None
                and page < max_page
                and self._is_full_page(page_records, query_params)
            ):
                ceiling_records = self._fetch_with_retry(url, query_params, max_page)
                if ceiling_records:
                    return [], True
            if not page_records:
                consecutive_empty += 1
                if consecutive_empty >= self._max_consecutive_empty_pages:
                    return records, False
            else:
                consecutive_empty = 0
                records.extend(page_records)
            page += 1

        return records, self.configured_page is None and consecutive_empty == 0

    @staticmethod
    def _is_full_page(page_records: list[dict], query_params: dict) -> bool:
        """Whether a page holds the `limit` asked for; without one, fullness is unknown."""
        limit = query_params.get("limit")
        return limit is not None and len(page_records) >= int(limit)

    def _check_silent_truncation(
        self,
        records: list[dict],
        from_date: str,
        to_date: str,
        max_records: int,
        symbol: str | None,
    ) -> None:
        if not records or not isinstance(records[0], dict):
            return
        try:
            # FMP returns chart records DESC, but be order-agnostic.
            candidates = [
                datetime.fromisoformat(d).date()
                for d in (records[0].get("date"), records[-1].get("date"))
                if d
            ]
            if not candidates:
                return
            earliest = min(candidates)
            from_dt = datetime.fromisoformat(from_date).date()
            # 7-day tolerance covers 9/11-class closures, Christmas–NY
            # half-days + weekend, 3-day weekends + symbol halt.
            if (earliest - from_dt).days > 7:
                self._log_data_truncated(
                    from_date=from_date,
                    to_date=to_date,
                    symbol=symbol,
                    records_returned=len(records),
                    max_records=max_records,
                    reason=TruncationReason.SILENT_CAP,
                    action="set_smaller_time_slice_days_for_this_stream",
                    earliest_returned=earliest.isoformat(),
                )
        except (ValueError, TypeError) as e:
            self.logger.debug(
                f"silent-truncation check skipped for stream={self.name} "
                f"window={from_date}..{to_date}: {type(e).__name__}: {e}"
            )

    def fetch_window(
        self,
        url,
        query_params,
        from_date,
        to_date,
        max_records,
        context: Context | None = None,
    ):
        """Fetch all records for a window. Three modes:

        1. `_paginate=True`: iterate all pages within the window. If the
           page ceiling is hit, split the window in half and recurse. If
           already at 1-day granularity, log DATA_TRUNCATED.
        2. Non-paginated, `len(records) >= max_records`: split window in
           half and recurse. If already at 1-day granularity, log
           DATA_TRUNCATED.
        3. Non-paginated, `len(records) < max_records`: yield. Also runs
           the silent-truncation safety net for FMP's hidden per-request
           caps that sit below max_records.
        """
        for records in self._window_batches(
            url, query_params, from_date, to_date, max_records
        ):
            yield from self._yield_processed(records, context)

    def _window_batches(
        self,
        url: str,
        query_params: dict,
        from_date: str,
        to_date: str,
        max_records: int,
        *,
        overflow_likely: bool = False,
    ) -> t.Iterator[list[dict]]:
        """`fetch_window`'s raw records, one list per window it settles on. Only requests and logs,
        never state or record processing, so a worker thread can run it."""
        query_params = query_params.copy()
        query_params[self._replication_key_starting_name] = (
            (datetime.fromisoformat(from_date[:10]) - timedelta(days=1)).strftime(
                "%Y-%m-%d"
            )
            if self._exclusive_from
            else from_date
        )
        query_params[self._replication_key_ending_name] = to_date
        symbol = query_params.get("symbol")
        splittable = self._is_splittable(from_date, to_date)

        if self._paginate:
            # an unsplittable window needs every page whatever the ceiling holds
            records, hit_page_ceiling = self._fetch_paginated_window(
                url,
                query_params,
                probe_ceiling=self._probe_page_ceiling and splittable,
                ceiling_first=overflow_likely,
            )
            if hit_page_ceiling and splittable:
                yield from self._both_halves(
                    url, query_params, from_date, to_date, max_records
                )
                return
            if hit_page_ceiling:
                self._log_data_truncated(
                    from_date=from_date,
                    to_date=to_date,
                    symbol=symbol,
                    records_returned=len(records),
                    max_records=max_records,
                    reason=TruncationReason.PAGE_CEILING,
                    action="set_smaller_time_slice_days_for_this_stream",
                    max_pages=self._max_pages,
                )
            elif not self._sparse_history:
                self._check_silent_truncation(
                    records, from_date, to_date, max_records, symbol
                )
            yield records
            return

        records = self._fetch_with_retry(url, query_params)

        if len(records) < max_records:
            self._check_silent_truncation(
                records, from_date, to_date, max_records, symbol
            )
            yield records
            return

        if not splittable:
            self._log_data_truncated(
                from_date=from_date,
                to_date=to_date,
                symbol=symbol,
                records_returned=len(records),
                max_records=max_records,
                reason=TruncationReason.SPLIT_FLOOR,
                action="refetch_with_smaller_window_or_higher_limit",
            )
            yield records
            return

        yield from self._both_halves(url, query_params, from_date, to_date, max_records)

    @staticmethod
    def _is_splittable(from_date: str, to_date: str) -> bool:
        return (
            datetime.fromisoformat(to_date) - datetime.fromisoformat(from_date)
        ).days > 1

    def _both_halves(
        self, url, query_params, from_date, to_date, max_records
    ) -> t.Iterator[list[dict]]:
        """A half of a window that overflowed is likely to overflow too."""
        from_dt = datetime.fromisoformat(from_date)
        to_dt = datetime.fromisoformat(to_date)
        mid_dt = from_dt + (to_dt - from_dt) // 2
        mid_date = mid_dt.strftime("%Y-%m-%d")
        yield from self._window_batches(
            url, query_params, from_date, mid_date, max_records, overflow_likely=True
        )
        yield from self._window_batches(
            url, query_params, mid_date, to_date, max_records, overflow_likely=True
        )

    @property
    def _max_records_per_request(self) -> int:
        return self.other_params.get("max_records_per_request", 4000)

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        query_params = self._request_query_params(context)

        url = self.get_url(context)

        time_slices = self.create_time_slice_chunks(context)
        max_records = self._max_records_per_request

        # No try/except around fetch_window: a transient API failure must
        # propagate so Singer leaves state at the last fully-completed window.
        # Catching here would silently skip the failed window and advance the
        # bookmark on subsequent windows' records, leaving a permanent gap.
        for from_date, to_date in time_slices:
            yield from self.fetch_window(
                url, query_params, from_date, to_date, max_records, context
            )


class ConcurrentPartitionTimeSliceStream(TimeSliceStream):
    """A time-sliced stream over many partitions, fetched ahead on worker threads.

    Workers fetch the partitions after the one the SDK is writing, so requests overlap, while
    the SDK still writes every record, bookmark and state message on the main thread in
    partition order. Every request still takes its turn in `_throttle`, which the workers
    share, so the stream's request rate is the one `min_throttle_seconds` sets.
    """

    # threads fetching partitions; 1 fetches each partition only when the SDK reaches it
    PARTITION_WORKERS: t.ClassVar[int] = 8
    # rows held for partitions the SDK has not reached; caps memory, the partition being written may pass it
    MAX_BUFFERED_RECORDS: t.ClassVar[int] = 200_000
    # partitions planned ahead of the one being written, so workers keep busy behind a large partition
    MAX_QUEUED_PARTITIONS: t.ClassVar[int] = 10_000
    # seconds between state messages, sent at a partition's start: each one serializes every bookmark
    STATE_MESSAGE_SECONDS: t.ClassVar[float] = 300.0
    # the SDK's record-count trigger for state messages, off: STATE_MESSAGE_SECONDS paces them instead
    STATE_MSG_FREQUENCY = sys.maxsize

    def __init__(self, tap: Tap) -> None:
        super().__init__(tap)
        self._prefetcher: PartitionPrefetcher | None = None
        self._last_state_message = time.monotonic()

    @property
    def partition_workers(self) -> int:
        return int(self.other_params.get("partition_workers", self.PARTITION_WORKERS))

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        self._send_state_between_partitions()
        prefetcher = self._prefetcher_for(context)
        if prefetcher is None:
            yield from super().get_records(context)
            return
        try:
            for records in prefetcher.batches(
                context, self.get_starting_timestamp(context)
            ):
                yield from self._yield_processed(records, context)
        except BaseException:
            prefetcher.close()
            raise

    def _send_state_between_partitions(self) -> None:
        now = time.monotonic()
        if now - self._last_state_message >= self.STATE_MESSAGE_SECONDS:
            self._write_state_message()
            self._last_state_message = now

    def _prefetcher_for(self, context: Context | None) -> PartitionPrefetcher | None:
        """The prefetcher that reads `context` next, started there if none does; None fetches it in place."""
        if context is None or self.partition_workers <= 1:
            return None
        if self._prefetcher is not None and self._prefetcher.expects(context):
            return self._prefetcher
        if self._prefetcher is not None:
            self._prefetcher.close()
        contexts = self.partitions or []
        first = next((i for i, c in enumerate(contexts) if c == context), None)
        if first is None:
            return None
        self._set_configured_page()
        self.requests_session.mount(
            self.url_base, HTTPAdapter(pool_maxsize=self.partition_workers)
        )
        self._prefetcher = PartitionPrefetcher(
            contexts[first:],
            plan=self._plan_partition,
            fetch=self._fetch_partition,
            workers=self.partition_workers,
            max_buffered_records=self.MAX_BUFFERED_RECORDS,
            max_queued_partitions=self.MAX_QUEUED_PARTITIONS,
            name=self.name,
            logger=self.logger,
        )
        return self._prefetcher

    def _plan_partition(self, context: Context) -> PartitionPlan:
        start = self._queued_partition_start(context)
        return PartitionPlan(
            context=context,
            start=start,
            query_params=self._request_query_params(context),
            windows=self._time_slices_from(start),
        )

    def _queued_partition_start(self, context: Context) -> str | None:
        """What `get_starting_timestamp` returns once the SDK, right before reading the partition,
        writes its starting value (start_date when there is no bookmark); worked out without writing state.
        """
        if self.replication_method != "INCREMENTAL":
            return None
        saved = (
            self._saved_partition_state(self._get_state_partition_context(context))
            or {}
        )
        return self._starting_timestamp_from(
            {
                "replication_key_value": saved.get("replication_key_value"),
                "starting_replication_value": self.config.get("start_date"),
            }
        )

    def _fetch_partition(self, plan: PartitionPlan) -> t.Iterator[list[dict]]:
        url = self.get_url(plan.context)
        for from_date, to_date in plan.windows:
            yield from self._window_batches(
                url,
                plan.query_params,
                from_date,
                to_date,
                self._max_records_per_request,
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
        max_records = self._max_records_per_request

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

        query_params = self.query_params
        other_params = self.other_params
        date_range = other_params.get("date_range")

        if "date" in query_params and "date_range" in other_params:
            raise ConfigValidationError(
                "Cannot specify both 'date' and 'date_range' in query_params and other_params."
            )

        if not date_range:
            date_range = [
                str(self.config["start_date"])[:10],
                datetime.today().date().strftime("%Y-%m-%d"),
            ]

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

        other_params = self.other_params

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


class SymbolYearQuarterPartitionStream(CompanySymbolPartitionStream):
    """Stream asked per symbol, fiscal year and quarter, bookmarked per symbol."""

    replication_method = "INCREMENTAL"
    replication_key = "date"
    # symbol x year x quarter state would outgrow elt.buffer_size; the target upserts on surrogate_key
    state_partitioning_keys = ["symbol"]
    _context_query_params = ("year", "quarter")

    # years a fiscal year can trail, and lead, the calendar year of its call
    FISCAL_YEAR_LAG: t.ClassVar[int] = 2
    FISCAL_YEAR_LEAD: t.ClassVar[int] = 1
    QUARTERS: t.ClassVar[tuple[int, ...]] = (1, 2, 3, 4)

    def _fiscal_years(self, symbol: str, last_year: int) -> list[int]:
        """The fiscal years a call on or after the symbol's bookmark, else start_date, can carry."""
        start = self.get_starting_timestamp({"symbol": symbol})
        if not start:
            raise ConfigValidationError(f"Missing start_date for {self.name}")
        return list(range(int(str(start)[:4]) - self.FISCAL_YEAR_LAG, last_year + 1))

    @cached_property
    def partitions(self) -> list[dict]:
        """Configured years are a backfill for every symbol. Cached: the SDK reads it again to finalize state."""
        backfill = YearWindow.configured(
            self.query_params.get("year", self.other_params.get("years"))
        )
        if "quarter" in self.query_params:
            quarters = [self.query_params["quarter"]]
        else:
            quarters = self.other_params.get("quarters") or self.QUARTERS
        last_year = datetime.today().year + self.FISCAL_YEAR_LEAD
        return [
            {"symbol": symbol["symbol"], "year": year, "quarter": quarter}
            for symbol in self._partition_symbols()
            for year in backfill or self._fiscal_years(symbol["symbol"], last_year)
            for quarter in quarters
        ]
