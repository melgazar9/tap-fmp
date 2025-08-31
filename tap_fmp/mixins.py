"""Shared mixins for common stream functionality."""

from __future__ import annotations

import typing as t
from abc import ABC, abstractmethod
from singer_sdk.helpers.types import Context
from singer_sdk import typing as th


class SelectableStreamMixin(ABC):
    """Mixin for streams that support configurable selection of items.

    Classes using this mixin must have:
    - self.config: dict
    - self.post_process(record: dict) -> dict method
    - self._check_missing_fields(schema: dict, record: dict) method
    - self.schema: dict property
    """

    config: dict
    schema: dict

    @property
    @abstractmethod
    def selection_config_key(self) -> str:
        """The key in config for this stream's selection settings."""
        raise ValueError("selection_config_key must be overridden by subclass.")

    @property
    @abstractmethod
    def selection_field_key(self) -> str:
        """The key within selection config for the select field."""
        raise ValueError("selection_field_key must be overridden by subclass.")

    @property
    @abstractmethod
    def item_name_singular(self) -> str:
        """Singular name for logging (e.g., 'sector', 'industry')."""
        raise ValueError("item_name_singular must be overridden by subclass.")

    @property
    @abstractmethod
    def item_name_plural(self) -> str:
        """Plural name for logging (e.g., 'sectors', 'industries')."""
        raise ValueError("item_name_plural must be overridden by subclass.")

    def get_selected_items_list(self) -> list[str] | None:
        """Get a list of selected items from config."""
        config = self.config.get(self.selection_config_key, {})
        selected_items = config.get(self.selection_field_key)

        if not selected_items or selected_items in ("*", ["*"]):
            return None

        if isinstance(selected_items, str):
            return selected_items.split(",")

        if isinstance(selected_items, list):
            if selected_items == ["*"]:
                return None
            return selected_items
        return None

    @abstractmethod
    def create_record_from_item(self, item: str) -> dict:
        """Create a record dict from a single item string. Must be overridden by subclass."""
        raise NotImplementedError(
            "create_record_from_item must be implemented by subclass"
        )

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Get records with selection logic applied."""
        selected_items = self.get_selected_items_list()

        if not selected_items:
            self.logger.info(
                f"No specific {self.item_name_plural} selected, fetching all {self.item_name_plural} from API..."
            )
            # Fetch directly from API to avoid circular reference
            yield from super().get_records(context)
        else:
            self.logger.info(
                f"Processing selected {self.item_name_plural}: {selected_items}"
            )
            for item in selected_items:
                record = self.create_record_from_item(item)
                record = self.post_process(record, context)
                self._check_missing_fields(self.schema, record)
                yield record


class BaseSymbolPartitionMixin(ABC):
    """Generic mixin for streams that partition by symbols with specific caching methods."""

    query_params: dict
    config: dict
    _tap: object
    name: str

    @abstractmethod
    def get_cached_company_symbols(self) -> list[dict]:
        """Get symbols from the specific cache method. Must be implemented by subclasses."""
        raise NotImplementedError(
            "get_cached_company_symbols must be implemented by subclass."
        )

    @property
    def selection_config_section(self) -> str | None:
        """Optional selection config section name (e.g., 'commodities' for select_commodities)."""
        return None

    @property
    def selection_field_name(self) -> str | None:
        """Optional selection field name (e.g., 'select_commodities')."""
        return None

    @property
    def partitions(self):
        """Get symbol partitions with validation and fallbacks."""
        if self.selection_config_section and self.selection_field_name:
            selected_symbols = self.config.get(self.selection_config_section, {}).get(
                self.selection_field_name
            )
            if selected_symbols and selected_symbols not in ("*", ["*"]):
                if isinstance(selected_symbols, str):
                    selected_symbols = [selected_symbols]
                return [{"symbol": symbol} for symbol in selected_symbols]

        query_params_symbol = self.query_params.get("symbol")
        other_params_symbols = self.config.get("other_params", {}).get("symbols")

        if query_params_symbol and other_params_symbols:
            raise ValueError(
                f"Cannot specify symbol configurations in both query_params and "
                f"other_params for stream {self.name}."
            )

        if query_params_symbol:
            return (
                [{"symbol": query_params_symbol}]
                if isinstance(query_params_symbol, str)
                else [{"symbol": symbol} for symbol in query_params_symbol]
            )
        elif other_params_symbols:
            return (
                [{"symbol": symbol} for symbol in other_params_symbols]
                if isinstance(other_params_symbols, list)
                else [{"symbol": other_params_symbols}]
            )
        else:
            cached_company_symbols = self.get_cached_company_symbols()
            return [{"symbol": c.get("symbol")} for c in cached_company_symbols]


class SymbolPartitionMixin(BaseSymbolPartitionMixin):
    """Mixin for streams that partition data by symbols, using company symbols as default."""

    def get_cached_company_symbols(self) -> list[dict]:
        """Get symbols from cached company symbols."""
        return self._tap.get_cached_company_symbols()


class FinancialStatementSymbolPartitionMixin(BaseSymbolPartitionMixin):
    """Mixin for financial statement streams that use only symbols with available financial data."""

    def get_cached_company_symbols(self) -> list[dict]:
        """Get symbols from cached financial statement symbols (optimized for financial data)."""
        return self._tap.get_cached_financial_statement_symbols()


class TranscriptSymbolPartitionMixin(BaseSymbolPartitionMixin):
    """Mixin for earnings transcript streams that use only symbols with available transcript data."""

    def get_cached_company_symbols(self) -> list[dict]:
        """Get symbols from cached transcript symbols (optimized for transcript data)."""
        return self._tap.get_cached_transcript_symbols()


class BaseConfigMixin(SelectableStreamMixin):
    """Base class for config mixins with common selection logic."""

    # These should be overridden by subclasses
    _selection_config_key: str = None
    _selection_field_key: str = None
    _item_name_singular: str = None
    _item_name_plural: str = None

    @property
    def selection_config_key(self) -> str:
        if self._selection_config_key is None:
            raise NotImplementedError("_selection_config_key must be set in subclass")
        return self._selection_config_key

    @property
    def selection_field_key(self) -> str:
        if self._selection_field_key is None:
            raise NotImplementedError("_selection_field_key must be set in subclass")
        return self._selection_field_key

    @property
    def selection_config_section(self) -> str:
        return self.selection_config_key

    @property
    def selection_field_name(self) -> str:
        return self.selection_field_key

    @property
    def item_name_singular(self) -> str:
        if self._item_name_singular is None:
            raise NotImplementedError("_item_name_singular must be set in subclass")
        return self._item_name_singular

    @property
    def item_name_plural(self) -> str:
        if self._item_name_plural is None:
            raise NotImplementedError("_item_name_plural must be set in subclass")
        return self._item_name_plural

    def create_record_from_item(self, item: str) -> dict:
        """Create a record dict from a symbol string. Default implementation for symbol-based records."""
        return {"symbol": item}

class CommodityConfigMixin(BaseConfigMixin):
    """Mixin providing commodity configuration properties."""

    _selection_config_key = "commodities"
    _selection_field_key = "select_commodities"
    _item_name_singular = "commodity"
    _item_name_plural = "commodities"


class CryptoConfigMixin(BaseConfigMixin):
    """Mixin providing crypto configuration properties."""

    _selection_config_key = "crypto_symbols"
    _selection_field_key = "select_crypto_symbols"
    _item_name_singular = "crypto symbol"
    _item_name_plural = "crypto symbols"

    def get_symbols_for_batch_stream(self) -> list[str]:
        """Get default crypto symbols from cached list."""
        cached_symbols = self._tap.get_cached_crypto_symbols()
        return [
            symbol.get("symbol") for symbol in cached_symbols if symbol.get("symbol")
        ]


class EtfConfigMixin(BaseConfigMixin):
    """Mixin providing ETF configuration properties."""

    _selection_config_key = "etf_symbols"
    _selection_field_key = "select_etf_symbols"
    _item_name_singular = "ETF symbol"
    _item_name_plural = "ETF symbols"


class IndexConfigMixin(BaseConfigMixin):
    """Mixin providing Index configuration properties."""

    _selection_config_key = "index_symbols"
    _selection_field_key = "select_index_symbols"
    _item_name_singular = "Index symbol"
    _item_name_plural = "Index symbols"


class CompanyConfigMixin(BaseConfigMixin):
    """Mixin providing company configuration properties."""

    _selection_config_key = "company_symbols"
    _selection_field_key = "select_symbols"
    _item_name_singular = "company symbol"
    _item_name_plural = "company symbols"

    def get_symbols_for_batch_stream(self) -> list[str]:
        """Get default company symbols from cached list."""
        cached_company_symbols = self._tap.get_cached_company_symbols()
        return [
            symbol.get("symbol")
            for symbol in cached_company_symbols
            if symbol.get("symbol")
        ]


class FinancialStatementConfigMixin(BaseConfigMixin):
    """Mixin providing financial statement symbol configuration properties."""

    _selection_config_key = "financial_statement_symbols"
    _selection_field_key = "select_symbols"
    _item_name_singular = "financial statement symbol"
    _item_name_plural = "financial statement symbols"


    def get_symbols_for_batch_stream(self) -> list[str]:
        """Get default financial statement symbols from cached list."""
        cached_financial_statement_symbols = (
            self._tap.get_cached_financial_statement_symbols()
        )
        return [
            symbol.get("symbol")
            for symbol in cached_financial_statement_symbols
            if symbol.get("symbol")
        ]


class CompanyBatchStreamMixin:
    """Mixin for batch streams that use company symbols without selection logic."""

    def get_symbols_for_batch_stream(self) -> list[str]:
        """Get company symbols from cached list."""
        cached_company_symbols = self._tap.get_cached_company_symbols()
        return [
            symbol.get("symbol")
            for symbol in cached_company_symbols
            if symbol.get("symbol")
        ]


class ForexConfigMixin(BaseConfigMixin):
    """Mixin providing Forex configuration properties."""

    _selection_config_key = "forex_pairs"
    _selection_field_key = "select_forex_pairs"
    _item_name_singular = "forex pair"
    _item_name_plural = "forex pairs"

    def get_symbols_for_batch_stream(self) -> list[str]:
        """Get default forex pairs from cached list."""
        cached_forex_pairs = self._tap.get_cached_forex_pairs()
        return [pair.get("symbol") for pair in cached_forex_pairs if pair.get("symbol")]


class BasePriceSchemaMixin:
    """Base schema for price data with symbol/date/price/volume."""

    primary_keys = ["symbol", "date"]
    replication_key = "date"
    replication_method = "INCREMENTAL"


class BaseIntervalPriceSchemaMixin(BasePriceSchemaMixin):
    """Base schema for interval price data (OHLC + datetime)."""

    schema = th.PropertiesList(
        th.Property("symbol", th.StringType, required=True),
        th.Property("date", th.DateTimeType),
        th.Property("open", th.NumberType),
        th.Property("high", th.NumberType),
        th.Property("low", th.NumberType),
        th.Property("close", th.NumberType),
        th.Property("volume", th.NumberType),
    ).to_dict()


class BaseAdjustedPriceSchemaMixin(BasePriceSchemaMixin):
    """Base schema for adjusted price data (OHLC adjusted + volume)."""

    schema = th.PropertiesList(
        th.Property("symbol", th.StringType, required=True),
        th.Property("date", th.DateType),
        th.Property("adj_open", th.NumberType),
        th.Property("adj_high", th.NumberType),
        th.Property("adj_low", th.NumberType),
        th.Property("adj_close", th.NumberType),
        th.Property("volume", th.NumberType),
    ).to_dict()


class ChartLightMixin(BasePriceSchemaMixin):
    """Light chart (price + volume)."""

    schema = th.PropertiesList(
        th.Property("symbol", th.StringType, required=True),
        th.Property("date", th.DateType),
        th.Property("price", th.NumberType),
        th.Property("volume", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/historical-price-eod/light"


class ChartFullMixin(BasePriceSchemaMixin):
    """Full chart (price + volume)."""

    schema = th.PropertiesList(
        th.Property("symbol", th.StringType, required=True),
        th.Property("date", th.DateType),
        th.Property("open", th.NumberType),
        th.Property("high", th.NumberType),
        th.Property("low", th.NumberType),
        th.Property("close", th.NumberType),
        th.Property("volume", th.IntegerType),
        th.Property("change", th.NumberType),
        th.Property("change_percent", th.NumberType),
        th.Property("vwap", th.NumberType),
    ).to_dict()

    def get_url(self, context: Context):
        return f"{self.url_base}/stable/historical-price-eod/full"


class Prices1minMixin(BaseIntervalPriceSchemaMixin):
    def get_url(self, context: Context):
        return f"{self.url_base}/stable/historical-chart/1min"


class Prices5minMixin(BaseIntervalPriceSchemaMixin):
    def get_url(self, context: Context):
        return f"{self.url_base}/stable/historical-chart/5min"


class Prices15minMixin(BaseIntervalPriceSchemaMixin):
    def get_url(self, context: Context):
        return f"{self.url_base}/stable/historical-chart/15min"


class Prices30minMixin(BaseIntervalPriceSchemaMixin):
    def get_url(self, context: Context):
        return f"{self.url_base}/stable/historical-chart/30min"


class Prices1HrMixin(BaseIntervalPriceSchemaMixin):
    def get_url(self, context: Context):
        return f"{self.url_base}/stable/historical-chart/1hour"


class Prices4HrMixin(BaseIntervalPriceSchemaMixin):
    def get_url(self, context: Context):
        return f"{self.url_base}/stable/historical-chart/4hour"


class BatchSymbolPartitionMixin(ABC):
    """Mixin for streams that need to partition symbols into chunks for API limits."""

    _max_symbols_per_request: int = 100
    query_params: dict
    _tap: object

    @property
    def partitions(self) -> list[dict]:
        """Return symbol partitions, splitting large symbol lists into chunks."""
        symbols_config = self.query_params.get("symbols")
        if symbols_config:
            if isinstance(symbols_config, list):
                symbols = symbols_config
            elif isinstance(symbols_config, str):
                symbols = symbols_config.split(",")
            else:
                symbols = [str(symbols_config)]
        else:
            symbols = self.get_symbols_for_batch_stream()

        # Sort symbols for consistent incremental replication tracking
        symbols = sorted(symbols)

        # Split symbols into chunks of _max_symbols_per_request
        partitions = []
        for i in range(0, len(symbols), self._max_symbols_per_request):
            chunk_symbols = symbols[i : i + self._max_symbols_per_request]
            symbols_str = ",".join(chunk_symbols)
            partitions.append({"symbols": symbols_str})

        return partitions

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Handle batch symbol context and delegate to parent."""
        if context and "symbols" in context:
            self.query_params["symbols"] = context["symbols"]
        yield from super().get_records(context)


class CikConfigMixin(BaseConfigMixin):
    """Mixin providing CIK configuration properties."""

    _selection_config_key = "ciks"
    _selection_field_key = "select_ciks"
    _item_name_singular = "cik"
    _item_name_plural = "ciks"

    def create_record_from_item(self, item: str) -> dict:
        """Create a CIK record dict from a single CIK string."""
        return {"cik": item, "company_name": None}


class ExchangeConfigMixin(BaseConfigMixin):
    """Mixin providing Exchange configuration properties."""

    _selection_config_key = "exchanges"
    _selection_field_key = "select_exchanges"
    _item_name_singular = "exchange"
    _item_name_plural = "exchanges"

    def create_record_from_item(self, item: str) -> dict:
        """Create an exchange record dict from a single exchange string."""
        return {"exchange": item, "name": None}
