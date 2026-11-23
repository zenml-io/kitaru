#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Canonical payload evidence and bounded deterministic evaluator results."""

import hashlib
import json
import math
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import Any

from kitaru.api_models.v1.evaluation import EvaluationResult

_RESULT_VALUE_MAX_BYTES = 64_000
_UNAVAILABLE = {"$kitaru": "unavailable"}


def format_decimal(value: Decimal) -> str:
    """Format a finite decimal without exponent or insignificant zeroes."""
    if not value.is_finite():
        raise ValueError("number must be finite")
    formatted = format(value, "f")
    if "." in formatted:
        formatted = formatted.rstrip("0").rstrip(".")
    if value == 0:
        return "0"
    return formatted


def _format_datetime(value: datetime) -> str | dict[str, str]:
    """Format an aware datetime in UTC, or mark a naive value unavailable."""
    if value.tzinfo is None or value.utcoffset() is None:
        return _UNAVAILABLE
    return (
        value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
    )


def normalize_observed(value: Any) -> tuple[Any, bool]:
    """Normalize observed evidence without failing on provider-specific values."""
    if value is None or isinstance(value, (str, bool, int)):
        return value, True
    if isinstance(value, float):
        if math.isfinite(value):
            return value, True
        return _UNAVAILABLE, False
    if isinstance(value, Decimal):
        if value.is_finite():
            return format_decimal(value), True
        return _UNAVAILABLE, False
    if isinstance(value, datetime):
        normalized = _format_datetime(value)
        return normalized, normalized is not _UNAVAILABLE
    if isinstance(value, uuid.UUID):
        return str(value), True
    if isinstance(value, Enum):
        return normalize_observed(value.value)
    if isinstance(value, (list, tuple)):
        normalized_items: list[Any] = []
        complete = True
        for item in value:
            normalized, available = normalize_observed(item)
            normalized_items.append(normalized)
            complete = complete and available
        return normalized_items, complete
    if isinstance(value, dict):
        if not all(isinstance(key, str) for key in value):
            return _UNAVAILABLE, False
        normalized_dict: dict[str, Any] = {}
        complete = True
        for key in sorted(value):
            normalized, available = normalize_observed(value[key])
            normalized_dict[key] = normalized
            complete = complete and available
        return normalized_dict, complete
    return _UNAVAILABLE, False


def serialize_json(value: Any) -> str:
    """Serialize normalized JSON with the revision-1 encoding."""
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def create_json_result(
    name: str,
    value: Any,
    explanation: str,
    *,
    passed: bool | None = None,
) -> EvaluationResult:
    """Create a deterministic structured-string result."""
    normalized, _ = normalize_observed(value)
    encoded = serialize_json(normalized)
    encoded_bytes = encoded.encode("utf-8")
    if len(encoded_bytes) > _RESULT_VALUE_MAX_BYTES:
        encoded = serialize_json(
            {
                "$kitaru": "result_truncated",
                "sha256": hashlib.sha256(encoded_bytes).hexdigest(),
                "utf8_bytes": len(encoded_bytes),
            }
        )
    return EvaluationResult(
        name=name,
        value=encoded,
        explanation=explanation,
        passed=passed,
    )


def _decode_pointer_part(raw_part: str) -> str:
    """Decode one RFC 6901 reference token and reject invalid escapes."""
    decoded: list[str] = []
    position = 0
    while position < len(raw_part):
        character = raw_part[position]
        if character != "~":
            decoded.append(character)
            position += 1
            continue
        if position + 1 >= len(raw_part) or raw_part[position + 1] not in {"0", "1"}:
            raise ValueError("JSON Pointer '~' escapes must be '~0' or '~1'")
        decoded.append("~" if raw_part[position + 1] == "0" else "/")
        position += 2
    return "".join(decoded)


def decode_pointer_parts(pointer: str) -> list[str]:
    """Validate and decode every token in an RFC 6901 JSON Pointer."""
    if pointer == "":
        return []
    if not pointer.startswith("/"):
        raise ValueError("JSON Pointer paths must be empty or start with '/'")
    return [_decode_pointer_part(part) for part in pointer[1:].split("/")]


def resolve_pointer(document: Any, pointer: str) -> tuple[bool, Any]:
    """Resolve an RFC 6901 JSON Pointer against normalized JSON evidence."""
    current = document
    for part in decode_pointer_parts(pointer):
        if isinstance(current, dict):
            if part not in current:
                return False, None
            current = current[part]
        elif isinstance(current, list):
            if (
                not part.isascii()
                or not part.isdigit()
                or (len(part) > 1 and part.startswith("0"))
                or len(part) > len(str(len(current)))
            ):
                return False, None
            index = int(part)
            if index >= len(current):
                return False, None
            current = current[index]
        else:
            return False, None
    return True, current
