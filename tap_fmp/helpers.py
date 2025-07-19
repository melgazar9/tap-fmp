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


def clean_strings(lst):
    cleaned_list = [
        re.sub(r"[^a-zA-Z0-9_]", "_", s) for s in lst
    ]  # remove special characters
    cleaned_list = [
        re.sub(r"(?<!^)(?=[A-Z])", "_", s).lower() for s in cleaned_list
    ]  # camel case -> snake case
    cleaned_list = [
        re.sub(r"_+", "_", s).strip("_").lower() for s in cleaned_list
    ]  # clean leading and trailing underscores
    return cleaned_list


def clean_json_keys(data: list[dict]) -> list[dict]:
    return [
        {new_key: value for new_key, value in zip(clean_strings(d.keys()), d.values())}
        for d in data
    ]


def generate_surrogate_key(data: dict, namespace=uuid.NAMESPACE_DNS) -> str:
    key_values = [str(data.get(field, "")) for field in data.keys()]
    key_string = "|".join(key_values)
    return str(uuid.uuid5(namespace, key_string))
