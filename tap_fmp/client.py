"""REST client handling, including FmpRestStream base class."""

from abc import ABC
import backoff
from datetime import datetime, timedelta
import re
from singer_sdk.helpers.types import Context
from singer_sdk.streams import RESTStream
from singer_sdk import Tap
from tap_fmp.helpers import clean_json_keys, generate_surrogate_key
from tap_fmp.mixins import SymbolPartitionMixin
import typing as t
import logging
import requests
from singer_sdk.exceptions import ConfigValidationError
import threading
import time
import random
import csv
import io


class FmpRestStream(RESTStream, ABC):
    """FMP stream class with symbol partitioning support."""

    _use_cached_symbols_default = False
    _paginate = False
    _add_surrogate_key = False
    _max_pages = 10000  # prevent infinite loops
    _paginate_key = "page"
    _replication_key_starting_name = "from"
    _replication_key_ending_name = "to"
    _expect_csv = False

    def __init__(self, tap: Tap) -> None:
        super().__init__(tap)
        self._all_symbols = None
        self.parse_config_params()

        self._min_interval = float(self.config.get("min_throttle_seconds", 0.01))
        self._throttle_lock = threading.Lock()
        self._last_call_ts = 0.0

    def _get_stream_config(self) -> dict:
        """Get configuration for this specific stream."""
        return self.config.get(self.name, {})

    @property
    def use_cached_symbols(self) -> bool:
        """Whether to use cached symbols for this stream."""
        stream_config = self._get_stream_config()

        if "use_cached_symbols" in stream_config:
            use_cached_symbols = stream_config["use_cached_symbols"]
            if not isinstance(use_cached_symbols, bool):
                raise ConfigValidationError(
                    f"Config for {self.name}.use_cached_symbols must be bool, "
                    f"got {type(use_cached_symbols)}"
                )
            return use_cached_symbols

        if hasattr(type(self), "_use_cached_symbols_default"):
            return getattr(type(self), "_use_cached_symbols_default")

        raise AttributeError(
            f"use_cached_symbols is not defined for stream {self.name}"
        )

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

    def _check_missing_fields(self, schema: dict, record: dict):
        """Validate record against schema and handle missing fields."""
        schema_fields = set(schema.get("properties", {}).keys())
        record_keys = set(record.keys())
        missing_in_record = schema_fields - record_keys
        missing_in_schema = record_keys - schema_fields

        if missing_in_record:
            logging.debug(
                f"Missing fields in record that are present in schema: {missing_in_record} for stream {self.name}"
            )

        if missing_in_schema:
            logging.warning(
                f"*** URGENT: Missing fields in schema that are present record for {self.name}: {missing_in_schema} ***"
            )

    def get_starting_timestamp(self, context: Context | None) -> str | None:
        if self.replication_method == "INCREMENTAL":
            state = self.get_context_state(context)
            if state.get("replication_key_value"):
                return self._format_replication_key(state["replication_key_value"])
            elif state.get("starting_replication_value"):
                return self._format_replication_key(state["starting_replication_value"])
            else:
                stream_config = self.config.get(self.name)
                if stream_config:
                    starting_timestamp = stream_config.get(
                        self._replication_key_starting_name
                    )
                else:
                    starting_timestamp = self.config.get("start_date")
                return starting_timestamp
        return None

    @staticmethod
    def redact_api_key(msg):
        return re.sub(r"(apikey=)[^&\s]+", r"\1<REDACTED>", msg)

    def _throttle(self) -> None:
        with self._throttle_lock:
            now = time.time()
            wait = self._last_call_ts + self._min_interval - now
            if wait > 0:
                time.sleep(wait + random.uniform(0, 0.1))
            self._last_call_ts = now

    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.RequestException,),
        base=5,
        max_value=300,
        jitter=backoff.full_jitter,
        max_tries=12,
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
    def _fetch_with_retry(
        self, url: str, query_params: dict, page: int | None = None
    ) -> list[dict]:
        """Centralized API call with retry logic."""

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
            self.configured_page + 1
            if self.configured_page is not None
            else self._max_pages
        )

        while page < max_page:
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
                self._check_missing_fields(self.schema, record)
                yield record

            page += 1

        if page >= self._max_pages:
            self.logger.warning(
                f"Reached maximum pages ({self._max_pages}). Some data may be missing."
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
                self._check_missing_fields(self.schema, record)
                yield record

    def post_process(self, record: dict, context: Context | None = None) -> dict:
        if self._add_surrogate_key:
            record["surrogate_key"] = generate_surrogate_key(record)
        return record


class FmpSurrogateKeyStream(FmpRestStream):
    """Base class for streams that need surrogate keys."""

    primary_keys = ["surrogate_key"]
    _add_surrogate_key = True


class SymbolPartitionStream(SymbolPartitionMixin, FmpRestStream):
    _use_cached_symbols_default = True
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
                self._check_missing_fields(self.schema, record)
                yield record


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
        config = self.config.get(self.name, {})
        periods = config.get("query_params", {}).get("period") or config.get(
            "other_params", {}
        ).get("periods")

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
        stream_cfg = self.config.get(self.name, {})
        tap_cfg = self.config

        if self.replication_method == "INCREMENTAL":
            start = self.get_starting_timestamp(context)
            start_dt = datetime.fromisoformat(start).date()
        else:
            start_dt = datetime(1970, 1, 1).date()

        window_days = int(stream_cfg.get("time_slice_days", 90))

        query_params = stream_cfg.get("query_params", {})
        start_date_cfg = query_params.get(
            self._replication_key_starting_name
        ) or tap_cfg.get("start_date")

        end_date = query_params.get("to") or (
            datetime.now() + timedelta(days=90)
        ).strftime("%Y-%m-%d")

        start_date = (
            (max(start_dt, datetime.fromisoformat(start_date_cfg).date()))
            - timedelta(days=1)
        ).strftime("%Y-%m-%d")

        if not start_date:
            raise ConfigValidationError(f"Missing start_date for {self.name}")

        start_dt = datetime.fromisoformat(start_date)
        end_dt = datetime.fromisoformat(end_date)

        if start_dt > end_dt:
            raise ConfigValidationError(
                f"start_date {start_date} is after end_date {end_date} for {self.name}"
            )

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
                self._check_missing_fields(self.schema, record)
                yield record
        else:
            from_dt = datetime.fromisoformat(from_date)
            to_dt = datetime.fromisoformat(to_date)
            if (to_dt - from_dt).days <= 1:
                # Can't split further, yield what we have but log a warning
                logging.warning(
                    f"Max records hit for {from_date} to {to_date} (symbol={query_params.get('symbol')})."
                    f"Some data may be missing."
                )
                for record in records:
                    record = self.post_process(record)
                    self._check_missing_fields(self.schema, record)
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
        max_records = (
            self.config.get(self.name, {})
            .get("other_params", {})
            .get("max_records_per_request", 4000)
        )

        for from_date, to_date in time_slices:
            try:
                yield from self.fetch_window(
                    url, query_params, from_date, to_date, max_records, context
                )
            except Exception as e:
                logging.error(
                    f"Failed to fetch records for stream {self.name}"
                    f"{self._replication_key_starting_name}={from_date} {self._replication_key_ending_name}={to_date}: {e}"
                )
                continue


class SymbolPartitionTimeSliceStream(SymbolPartitionMixin, TimeSliceStream):
    _use_cached_symbols_default = True
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
        max_records = (
            self.config.get(self.name, {})
            .get("other_params", {})
            .get("max_records_per_request", 4000)
        )

        for from_date, to_date in time_slices:
            try:
                yield from self.fetch_window(
                    url, query_params, from_date, to_date, max_records, context
                )
            except Exception as e:
                logging.error(
                    f"Failed to fetch records for symbol={context['symbol']}"
                    f"{self._replication_key_starting_name}={from_date} {self._replication_key_ending_name}={to_date}: {e}"
                )
                continue


class SymbolFetcher(FmpRestStream):
    """
    Fetch and caches FMP symbols in memory for the duration of a Meltano tap run.
    """

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.config.get('base_url')}/stable/stock-list"

    def fetch_all_symbols(self, context: Context | None = None) -> list[dict]:
        url = self.get_url(context)
        return self._fetch_with_retry(url, self.query_params)

    @staticmethod
    def fetch_specific_symbols(symbol_list: list[str]) -> list[dict]:
        """
        Create symbol records for a specific list of symbols.
        """
        if isinstance(symbol_list, str):
            return [{"symbol": symbol_list.upper(), "company_name": None}]
        return [
            {
                "symbol": symbol.upper(),
                "company_name": None,
            }
            for symbol in symbol_list
        ]


class CikFetcher(FmpRestStream):
    """
    Fetch and cache FMP CIKs in memory for the duration of a Meltano tap run.
    """

    _paginate = True

    def get_url(self, context) -> str:
        return f"{self.config.get('base_url')}/stable/cik-list"

    def fetch_all_ciks(self, context) -> list[dict]:
        url = self.get_url(context)
        return list(self._handle_pagination(url, self.query_params, context))

    @staticmethod
    def fetch_specific_ciks(cik_list: list[str]) -> list[dict]:
        """
        Create CIK records for a specific list of CIKs.
        """
        if isinstance(cik_list, str):
            return [{"cik": cik_list, "company_name": None}]
        return [
            {
                "cik": cik,
                "company_name": None,
            }
            for cik in cik_list
        ]


class ExchangeFetcher(FmpRestStream):
    """
    Fetch and cache FMP exchanges in memory for the duration of a Meltano tap run.
    """

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}/stable/all-exchange-market-hours"

    def fetch_all_exchanges(self, context: Context | None = None) -> list[dict]:
        url = self.get_url(context)
        return self._fetch_with_retry(url, self.query_params)

    @staticmethod
    def fetch_specific_exchanges(exchange_list: list[str]) -> list[dict]:
        """
        Create exchange records for a specific list of exchanges.
        """
        if isinstance(exchange_list, str):
            return [{"exchange": exchange_list, "name": None}]
        return [
            {
                "exchange": exchange,
                "name": None,
            }
            for exchange in exchange_list
        ]


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

        query_params = self.config.get(self.name, {}).get("query_params", {})
        other_params = self.config.get(self.name, {}).get("other_params", {})
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
        filtered_dates = [d for d in dates_dict if d["date"] >= starting_date]

        for date_dict in filtered_dates:
            self.query_params.update(date_dict)
            yield from super().get_records(context)
