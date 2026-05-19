"""JSON-safe serialization helpers for OpenAI Agents SDK adapter capture."""

from collections.abc import Mapping
from dataclasses import fields, is_dataclass
from typing import Any
from uuid import uuid4

from pydantic_core import to_jsonable_python


def to_json_safe(value: Any) -> Any:
    """Best-effort conversion for observability payloads.

    This helper is deliberately forgiving so telemetry capture does not break
    user code. It may include ``repr(...)`` fallback text, so do not use it for
    cache keys.
    """
    try:
        return to_jsonable_python(value, serialize_unknown=True)
    except ValueError as exc:
        return {
            "repr": repr(value),
            "python_type": type(value).__name__,
            "serialization_error": str(exc),
        }


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


def stable_cache_identity(
    value: Any,
    *,
    opaque_objects_unique: bool = False,
    max_depth: int = 12,
    max_items: int = 512,
) -> Any:
    """Return a deterministic, bounded cache identity for public API values.

    The happy path is plain JSON-like data, dataclasses, and Pydantic models.
    Cycles or very large object graphs fall back to a miss-safe opaque token
    instead of recursing forever or collapsing distinct contexts together.
    """
    remaining_items = [max_items]
    seen: set[int] = set()
    return _stable_cache_identity(
        value,
        opaque_objects_unique=opaque_objects_unique,
        max_depth=max_depth,
        remaining_items=remaining_items,
        seen=seen,
    )


def _stable_cache_identity(
    value: Any,
    *,
    opaque_objects_unique: bool,
    max_depth: int,
    remaining_items: list[int],
    seen: set[int],
) -> Any:
    if value is None or isinstance(value, str | int | float | bool):
        return value
    if max_depth <= 0:
        return _opaque_cache_identity(value, reason="max_depth_exceeded")
    if remaining_items[0] <= 0:
        return _opaque_cache_identity(value, reason="max_items_exceeded")

    value_id = id(value)
    if value_id in seen:
        return _opaque_cache_identity(value, reason="cycle_detected")

    if isinstance(value, list | tuple):
        if len(value) > remaining_items[0]:
            return _opaque_cache_identity(value, reason="max_items_exceeded")
        seen.add(value_id)
        remaining_items[0] -= len(value)
        try:
            return {
                "collection_type": type(value).__name__,
                "items": [
                    _stable_cache_identity(
                        item,
                        opaque_objects_unique=opaque_objects_unique,
                        max_depth=max_depth - 1,
                        remaining_items=remaining_items,
                        seen=seen,
                    )
                    for item in value
                ],
            }
        finally:
            seen.remove(value_id)

    if isinstance(value, set | frozenset):
        if len(value) > remaining_items[0]:
            return _opaque_cache_identity(value, reason="max_items_exceeded")
        seen.add(value_id)
        remaining_items[0] -= len(value)
        try:
            item_identities = [
                _stable_cache_identity(
                    item,
                    opaque_objects_unique=opaque_objects_unique,
                    max_depth=max_depth - 1,
                    remaining_items=remaining_items,
                    seen=seen,
                )
                for item in value
            ]
            return {
                "collection_type": type(value).__name__,
                "items": sorted(item_identities, key=_identity_sort_key),
            }
        finally:
            seen.remove(value_id)

    if isinstance(value, Mapping):
        if len(value) > remaining_items[0]:
            return _opaque_cache_identity(value, reason="max_items_exceeded")
        seen.add(value_id)
        remaining_items[0] -= len(value)
        try:
            if all(isinstance(key, str) for key in value):
                return {
                    key: _stable_cache_identity(
                        nested,
                        opaque_objects_unique=opaque_objects_unique,
                        max_depth=max_depth - 1,
                        remaining_items=remaining_items,
                        seen=seen,
                    )
                    for key, nested in sorted(value.items())
                }
            pairs = [
                {
                    "key": _stable_cache_identity(
                        key,
                        opaque_objects_unique=opaque_objects_unique,
                        max_depth=max_depth - 1,
                        remaining_items=remaining_items,
                        seen=seen,
                    ),
                    "value": _stable_cache_identity(
                        nested,
                        opaque_objects_unique=opaque_objects_unique,
                        max_depth=max_depth - 1,
                        remaining_items=remaining_items,
                        seen=seen,
                    ),
                }
                for key, nested in value.items()
            ]
            return {
                "collection_type": "dict",
                "items": sorted(
                    pairs, key=lambda item: _identity_sort_key(item["key"])
                ),
            }
        finally:
            seen.remove(value_id)

    if is_dataclass(value) and not isinstance(value, type):
        seen.add(value_id)
        value_type = type(value)
        public_fields = [
            field for field in fields(value) if not field.name.startswith("_")
        ]
        if len(public_fields) > remaining_items[0]:
            return _opaque_cache_identity(value, reason="max_items_exceeded")
        remaining_items[0] -= len(public_fields)
        try:
            return {
                "python_type": f"{value_type.__module__}.{value_type.__qualname__}",
                "fields": {
                    field.name: _stable_cache_identity(
                        getattr(value, field.name),
                        opaque_objects_unique=opaque_objects_unique,
                        max_depth=max_depth - 1,
                        remaining_items=remaining_items,
                        seen=seen,
                    )
                    for field in public_fields
                },
            }
        finally:
            seen.remove(value_id)

    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        try:
            return stable_cache_identity(
                model_dump(mode="json"),
                opaque_objects_unique=opaque_objects_unique,
                max_depth=max_depth - 1,
                max_items=remaining_items[0],
            )
        except Exception as exc:
            return _opaque_cache_identity(value, reason=type(exc).__name__)

    if opaque_objects_unique:
        return _opaque_cache_identity(value, reason="opaque_object")

    value_type = type(value)
    return {
        "python_type": f"{value_type.__module__}.{value_type.__qualname__}",
        "name": getattr(value, "name", None),
        "model_name": getattr(value, "model_name", None),
    }


def _opaque_cache_identity(value: Any, *, reason: str) -> dict[str, Any]:
    value_type = type(value)
    return {
        "python_type": f"{value_type.__module__}.{value_type.__qualname__}",
        "opaque_cache_token": uuid4().hex,
        "serialization_error": reason,
    }


def _identity_sort_key(value: Any) -> str:
    return repr(to_cache_identity(value))
