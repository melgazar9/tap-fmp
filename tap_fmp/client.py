"""REST client handling, including FmpRestStream base class."""

from __future__ import annotations
from abc import ABC
import backoff
from datetime import datetime, timedelta
import re
from singer_sdk.helpers.types import Context
from singer_sdk.streams import Stream
from singer_sdk import Tap
from tap_fmp.helpers import clean_json_keys
import typing as t
import logging
import requests
from singer_sdk.exceptions import ConfigValidationError
from tap_fmp.helpers import generate_surrogate_key

class FmpRestStream(Stream, ABC):
    """FMP stream class with symbol partitioning support."""

    _use_cached_symbols_default = False
    _paginate = False
    _add_surrogate_key = False

    def __init__(self, tap: Tap) -> None:
        super().__init__(tap)
        self._all_symbols = None
        self.parse_config_params()

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
        raise ValueError("Must be overridden in the stream class.")

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
                return state["starting_replication_value"]
            elif state.get("starting_replication_value"):
                return state["starting_replication_value"]
            else:
                stream_config = self.config.get(self.name)
                if stream_config:
                    starting_timestamp = stream_config.get("from")
                else:
                    starting_timestamp = self.config.get("start_date")
                return starting_timestamp
        return None

    @staticmethod
    def redact_api_key(msg):
        return re.sub(r"(apikey=)[^&\s]+", r"\1<REDACTED>", msg)

    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.RequestException,),
        max_tries=8,
        max_time=300,
        jitter=backoff.full_jitter,
    )
    def _fetch_with_retry(
        self, url: str, query_params: dict, page: int | None = None
    ) -> list[dict]:
        """Centralized API call with retry logic."""
        if page is not None:
            query_params["page"] = page
        log_url = self.redact_api_key(url)
        log_params = {
            k: ("<REDACTED>" if k == "apikey" else v) for k, v in query_params.items()
        }
        logging.info(f"Requesting: {log_url} with params: {log_params}")
        query_params = {} if query_params is None else query_params
        try:
            response = requests.get(url, params=query_params)
            response.raise_for_status()
            records = response.json()
            if isinstance(records, dict) and len(records):
                records = [records]
            records = clean_json_keys(records)
            logging.info(
                f"Records returned: {len(records) if isinstance(records, list) else 'not a list'}"
            )
            return records
        except requests.exceptions.RequestException as e:
            redacted_url = self.redact_api_key(e.request.url)
            error_message = (
                f"{e.response.status_code} Client Error: {e.response.reason} for url: {redacted_url}"
                if e.response and e.request
                else str(e)
            )

            error_message = self.redact_api_key(
                error_message
            )  # This redacts any key in the message string
            raise requests.exceptions.HTTPError(
                error_message,
                response=e.response,
                request=e.request,
            )

    def _handle_pagination(self, url: str, query_params: dict) -> t.Iterable[dict]:
        page = 0
        max_pages = 10000  # prevent infinite loops
        consecutive_empty_pages = 0
        max_consecutive_empty = 2

        while page < max_pages:
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
                record = self.post_process(record)
                self._check_missing_fields(self.schema, record)
                yield record

            page += 1

        if page >= max_pages:
            self.logger.warning(
                f"Reached maximum pages ({max_pages}). Some data may be missing."
            )

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        url = self.get_url(context)
        query_params = self.query_params.copy()

        if self._paginate:
            yield from self._handle_pagination(url, query_params)
        else:
            records = self._fetch_with_retry(url, query_params)
            for record in records:
                record = self.post_process(record)
                self._check_missing_fields(self.schema, record)
                yield record
    
    def post_process(self, record: dict, context: Context | None = None) -> dict:
        if self._add_surrogate_key:
            record["surrogate_key"] = generate_surrogate_key(record)
        return record


class SymbolPartitionStream(FmpRestStream):
    _use_cached_symbols_default = True
    _symbol_in_path_params = False
    _symbol_in_query_params = True

    @property
    def partitions(self):
        return [{"symbol": s["symbol"]} for s in self._tap.get_cached_symbols()]

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
            yield from self._handle_pagination(url, query_params)
        else:
            records = self._fetch_with_retry(url, query_params)
            for record in records:
                record = self.post_process(record)
                self._check_missing_fields(self.schema, record)
                yield record


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
        start_date_cfg = query_params.get("from") or tap_cfg.get("start_date")

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

    def fetch_window(self, url, query_params, from_date, to_date, max_records):
        """
        Recursively fetch all records for a window, splitting if we hit the max_records limit.
        """
        query_params = query_params.copy()
        query_params["from"] = from_date
        query_params["to"] = to_date

        records = self._fetch_with_retry(url, query_params)
        if len(records) < max_records:
            for record in records:
                record = self.post_process(record)
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
                url, query_params, from_date, mid_date, max_records
            )
            # Fetch right half
            yield from self.fetch_window(
                url, query_params, mid_date, to_date, max_records
            )

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        query_params = self.query_params.copy()

        url = self.get_url(context)

        time_slices = self.create_time_slice_chunks(context)
        max_records = self.config.get(self.name, {}).get("other_params", {}).get("max_records_per_request", 4000)

        for from_date, to_date in time_slices:
            try:
                yield from self.fetch_window(
                    url, query_params, from_date, to_date, max_records
                )
            except Exception as e:
                logging.error(
                    f"Failed to fetch records for symbol={context['symbol']} from={from_date} to={to_date}: {e}"
                )
                continue


class SymbolPartitionTimeSliceStream(TimeSliceStream):
    _use_cached_symbols_default = True
    _symbol_in_path_params = False
    _symbol_in_query_params = True

    @property
    def partitions(self):
        return [{"symbol": s["symbol"]} for s in self._tap.get_cached_symbols()]

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
        max_records = self.config.get(self.name, {}).get("other_params", {}).get("max_records_per_request", 4000)

        for from_date, to_date in time_slices:
            try:
                yield from self.fetch_window(
                    url, query_params, from_date, to_date, max_records
                )
            except Exception as e:
                logging.error(
                    f"Failed to fetch records for symbol={context['symbol']} from={from_date} to={to_date}: {e}"
                )
                continue