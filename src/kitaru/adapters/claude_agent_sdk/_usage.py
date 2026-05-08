"""Usage normalization helpers for Claude Agent SDK payloads."""

from typing import Any

from ._serialization import to_json_safe


def normalize_usage(value: Any) -> dict[str, Any] | None:
    """Return a JSON-safe usage dictionary, or ``None`` when absent."""
    if value is None:
        return None
    safe = to_json_safe(value)
    if isinstance(safe, dict):
        return safe
    return {"raw": safe}
