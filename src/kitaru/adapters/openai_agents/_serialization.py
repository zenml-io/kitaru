"""JSON-safe serialization helpers for OpenAI Agents SDK adapter capture."""

from typing import Any

from pydantic_core import to_jsonable_python


def to_cache_identity(value: Any) -> Any:
    """Best-effort stable identity for synthetic checkpoint cache keys.

    Cache keys need a different fallback from observability capture: a repr can
    contain memory addresses or sensitive object contents. If the SDK object is
    not JSON-serializable, fall back to its Python type identity only.
    """
    try:
        return to_jsonable_python(value, serialize_unknown=True)
    except ValueError:
        value_type = type(value)
        return {
            "python_type": f"{value_type.__module__}.{value_type.__qualname__}",
            "serialization_error": "cache_identity_serialization_failed",
        }
