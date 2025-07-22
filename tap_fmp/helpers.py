import re
import requests
from types import MappingProxyType

import uuid


class SymbolFetcher:
    """
    Fetch and caches FMP symbols in memory for the duration of a Meltano tap run.
    """

    def __init__(self, config: MappingProxyType):
        self.config = config

    def fetch_all_symbols(self) -> list[dict]:
        endpoint = f"{self.config.get('base_url')}/stable/stock-list?apikey={self.config.get('api_key')}"
        response = requests.get(endpoint)
        response.raise_for_status()
        symbols = response.json()
        symbols = clean_json_keys(symbols)
        return symbols

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


class CikFetcher:
    """
    Fetch and cache FMP CIKs in memory for the duration of a Meltano tap run.
    """

    def __init__(self, config: MappingProxyType):
        self.config = config

    def fetch_all_ciks(self) -> list[dict]:
        endpoint = f"{self.config.get('base_url')}/stable/cik-list?apikey={self.config.get('api_key')}"
        response = requests.get(endpoint)
        response.raise_for_status()
        ciks = response.json()
        ciks = clean_json_keys(ciks)
        return ciks

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


class ExchangeFetcher:
    """
    Fetch and cache FMP exchanges in memory for the duration of a Meltano tap run.
    """

    def __init__(self, config: MappingProxyType):
        self.config = config

    def fetch_all_exchanges(self) -> list[dict]:
        endpoint = f"{self.config.get('base_url')}/stable/all-exchange-market-hours?apikey={self.config.get('api_key')}"
        response = requests.get(endpoint)
        response.raise_for_status()
        exchanges = response.json()
        exchanges = clean_json_keys(exchanges)
        return exchanges

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

def clean_strings(lst):
    cleaned_list = []
    for s in lst:
        # Handle all caps words specially - if entire string is caps, just lowercase it
        if s.isupper() and s.isalpha():
            cleaned = s.lower()
        else:
            # Remove special characters first
            cleaned = re.sub(r"[^a-zA-Z0-9_]", "_", s)
            # Convert camelCase to snake_case (but not all caps)
            cleaned = re.sub(r"(?<!^)(?=[A-Z][a-z])", "_", cleaned)
            # Clean up multiple underscores and strip leading/trailing ones
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
