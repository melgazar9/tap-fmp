"""REST client handling, including FmpRestStream base class."""

from __future__ import annotations
from abc import ABC
import backoff
from singer_sdk.helpers.types import Context
from singer_sdk.streams import Stream
from singer_sdk import Tap
from tap_fmp.helpers import clean_json_keys
import typing as t
import logging
import requests
from singer_sdk.exceptions import ConfigValidationError

class FmpRestStream(Stream, ABC):
    """FMP stream class with symbol partitioning support."""

    _use_cached_symbols_default = True
    _paginate = False

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
                f"*** URGENT: Missing fields in schema that are present record: {missing_in_schema} ***"
            )

    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.RequestException,),
        max_tries=8,
        max_time=300,
        jitter=backoff.full_jitter,
    )
    def _fetch_with_retry(self, url: str, query_params: dict, page: int | None = None) -> list[dict]:
        """Centralized API call with retry logic."""
        if page is not None:
            query_params["page"] = page
        log_url = url.replace(self.config.get("api_key", ""), "<REDACTED>")
        log_params = {k: ("<REDACTED>" if k == "apikey" else v) for k, v in query_params.items()}
        logging.info(f"Requesting: {log_url} with params: {log_params}")
        query_params = {} if query_params is None else query_params
        response = requests.get(url, params=query_params)
        response.raise_for_status()
        records = response.json()
        records = clean_json_keys(records)
        logging.info(f"Records returned: {len(records) if isinstance(records, list) else 'not a list'}")
        return records

    def _handle_pagination(self, url: str, query_params: dict) -> t.Iterable[dict]:
        page = 0
        max_pages = 1000  # prevent infinite loops
        consecutive_empty_pages = 0
        max_consecutive_empty = 2
        
        while page < max_pages:
            records = self._fetch_with_retry(url, query_params, page)
            
            if not isinstance(records, list):
                self.logger.warning(f"Expected list response, got {type(records)}. Stopping pagination.")
                break
            
            if not records:
                consecutive_empty_pages += 1
                if consecutive_empty_pages >= max_consecutive_empty:
                    self.logger.info(f"Stopping pagination after {consecutive_empty_pages} consecutive empty pages")
                    break
            else:
                consecutive_empty_pages = 0
                
            for record in records:
                self._check_missing_fields(self.schema, record)
                yield record
                
            page += 1
            
        if page >= max_pages:
            self.logger.warning(f"Reached maximum pages ({max_pages}). Some data may be missing.")

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        url = self.get_url(context)
        query_params = self.query_params.copy()

        if self._paginate:
            yield from self._handle_pagination(url, query_params)
        else:
            records = self._fetch_with_retry(url, query_params)
            for record in records:
                self._check_missing_fields(self.schema, record)
                yield record


class SymbolPartitionedStream(FmpRestStream):
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
                self._check_missing_fields(self.schema, record)
                yield record