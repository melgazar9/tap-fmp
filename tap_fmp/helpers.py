import re

import uuid


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
