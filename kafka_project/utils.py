import json
from typing import List, Any


def load_messages(path: str) -> List[Any]:
    """Load a JSON array of messages from `path` and return as Python objects."""
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError("messages file must contain a JSON array")
    return data
