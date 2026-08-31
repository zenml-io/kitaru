#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
"""Bounded and redacted capture for values sent to Kitaru."""

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, fields, is_dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from itertools import islice
from typing import Any
from uuid import UUID

from pydantic import BaseModel

_REDACTED = "[REDACTED]"
_SENSITIVE_KEYS = frozenset(
    {
        "authorization",
        "proxy-authorization",
        "api-key",
        "x-api-key",
        "password",
        "passwd",
        "secret",
        "client-secret",
        "access-token",
        "refresh-token",
        "cookie",
        "set-cookie",
    }
)


@dataclass(frozen=True)
class CapturePolicy:
    """Limits and optional final redactor for one invocation's stored copies."""

    max_child_nodes: int = 10_000
    max_field_bytes: int = 256 * 1024
    max_buffer_bytes: int = 16 * 1024 * 1024
    max_depth: int = 20
    max_collection_items: int = 1_000
    redactor: Callable[[Any], Any] | None = None

    def __post_init__(self) -> None:
        """Validate that every capture limit is positive."""
        for name in (
            "max_child_nodes",
            "max_field_bytes",
            "max_buffer_bytes",
            "max_depth",
            "max_collection_items",
        ):
            if getattr(self, name) < 1:
                raise ValueError(f"{name} must be at least 1")


@dataclass(frozen=True)
class CaptureResult:
    """Captured JSON value plus explicit loss information."""

    value: Any
    lossy: bool
    truncated: bool
    reasons: tuple[str, ...]
    encoded_bytes: int

    @property
    def replayable(self) -> bool:
        """Return whether capture retained exact replay-key semantics."""
        return not self.lossy


@dataclass
class CaptureBudget:
    """Mutable invocation-wide node and byte counters."""

    policy: CapturePolicy
    child_nodes: int = 0
    buffered_bytes: int = 0
    dropped_nodes: int = 0
    dropped_bytes: int = 0

    def reserve_node(self) -> bool:
        """Reserve one child-node slot."""
        if self.child_nodes >= self.policy.max_child_nodes:
            self.dropped_nodes += 1
            return False
        self.child_nodes += 1
        return True

    def reserve_bytes(self, size: int) -> bool:
        """Reserve buffered bytes without exceeding the invocation bound."""
        if self.buffered_bytes + size > self.policy.max_buffer_bytes:
            self.dropped_bytes += size
            return False
        self.buffered_bytes += size
        return True

    def release_bytes(self, size: int) -> None:
        """Release bytes after a successful flush."""
        self.buffered_bytes = max(0, self.buffered_bytes - size)


def _key_is_sensitive(key: object) -> bool:
    normalized = str(key).strip().lower().replace("_", "-")
    return normalized in _SENSITIVE_KEYS


def _serialization_marker(value: Any) -> dict[str, str]:
    return {
        "__kitaru_capture__": "serialization_failed",
        "type": type(value).__name__,
    }


class _FieldTooLarge(Exception):
    """Stop measuring a field as soon as it exceeds its byte limit."""


def _json_encoded_size(value: Any, *, limit: int | None) -> int:
    """Measure compact JSON UTF-8 bytes without materializing the document."""
    total = 0

    def add(size: int) -> None:
        nonlocal total
        total += size
        if limit is not None and total > limit:
            raise _FieldTooLarge

    def add_string(string: str) -> None:
        add(1)
        for character in string:
            codepoint = ord(character)
            if character in ('"', "\\") or character in "\b\f\n\r\t":
                add(2)
            elif codepoint < 0x20:
                add(6)
            else:
                add(len(character.encode()))
        add(1)

    def measure(item: Any) -> None:
        if item is None or item is True:
            add(4)
        elif item is False:
            add(5)
        elif isinstance(item, int):
            add(len(str(item)))
        elif isinstance(item, float):
            add(len(repr(item)))
        elif isinstance(item, str):
            add_string(item)
        elif isinstance(item, Mapping):
            add(1)
            for index, key in enumerate(sorted(item)):
                if index:
                    add(1)
                if not isinstance(key, str):
                    raise TypeError("JSON object keys must be strings")
                add_string(key)
                add(1)
                measure(item[key])
            add(1)
        elif isinstance(item, list):
            add(1)
            for index, child in enumerate(item):
                if index:
                    add(1)
                measure(child)
            add(1)
        else:
            raise TypeError(f"{type(item).__name__} is not JSON serializable")

    measure(value)
    return total


def _convert(
    value: Any,
    *,
    policy: CapturePolicy,
    depth: int,
    reasons: list[str],
    active_ids: set[int],
    work_remaining: list[int],
) -> Any:
    if work_remaining[0] <= 0:
        reasons.append("max_field_bytes")
        return {"__kitaru_capture__": "max_field_bytes"}
    work_remaining[0] -= 1
    if depth > policy.max_depth:
        reasons.append("max_depth")
        return {"__kitaru_capture__": "max_depth"}
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):
            reasons.append("non_finite_float")
            return {"__kitaru_capture__": "non_finite_float"}
        return value
    if isinstance(value, (UUID, datetime, date, Decimal)):
        return str(value)
    if isinstance(value, Enum):
        return _convert(
            value.value,
            policy=policy,
            depth=depth,
            reasons=reasons,
            active_ids=active_ids,
            work_remaining=work_remaining,
        )
    if isinstance(value, BaseModel):
        value = value.model_dump(mode="python", warnings=False)
    elif is_dataclass(value) and not isinstance(value, type):
        value = {field.name: getattr(value, field.name) for field in fields(value)}
    if isinstance(value, Mapping):
        identity = id(value)
        if identity in active_ids:
            reasons.append("cycle")
            return {"__kitaru_capture__": "cycle"}
        active_ids.add(identity)
        result: dict[str, Any] = {}
        try:
            item_limit = min(policy.max_collection_items, work_remaining[0])
            items = list(islice(value.items(), item_limit + 1))
            if len(items) > item_limit:
                reasons.append(
                    "max_collection_items"
                    if item_limit == policy.max_collection_items
                    else "max_field_bytes"
                )
                items.pop()
            for key, item in items:
                if work_remaining[0] <= 0:
                    reasons.append("max_field_bytes")
                    break
                output_key = str(key)
                if not isinstance(key, str):
                    reasons.append("non_string_key")
                if output_key in result:
                    reasons.append("key_collision")
                if _key_is_sensitive(key):
                    work_remaining[0] -= 1
                    reasons.append("sensitive_key_redacted")
                    result[output_key] = _REDACTED
                else:
                    result[output_key] = _convert(
                        item,
                        policy=policy,
                        depth=depth + 1,
                        reasons=reasons,
                        active_ids=active_ids,
                        work_remaining=work_remaining,
                    )
        finally:
            active_ids.remove(identity)
        return result
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        identity = id(value)
        if identity in active_ids:
            reasons.append("cycle")
            return {"__kitaru_capture__": "cycle"}
        active_ids.add(identity)
        try:
            item_limit = min(policy.max_collection_items, work_remaining[0])
            items = list(islice(value, item_limit + 1))
            if len(items) > item_limit:
                reasons.append(
                    "max_collection_items"
                    if item_limit == policy.max_collection_items
                    else "max_field_bytes"
                )
                items.pop()
            converted_items: list[Any] = []
            for item in items:
                if work_remaining[0] <= 0:
                    reasons.append("max_field_bytes")
                    break
                converted_items.append(
                    _convert(
                        item,
                        policy=policy,
                        depth=depth + 1,
                        reasons=reasons,
                        active_ids=active_ids,
                        work_remaining=work_remaining,
                    )
                )
            return converted_items
        finally:
            active_ids.remove(identity)
    reasons.append("serialization_failed")
    return _serialization_marker(value)


def capture_value(value: Any, policy: CapturePolicy) -> CaptureResult:
    """Create one bounded JSON-compatible copy without changing the input."""
    reasons: list[str] = []
    converted = _convert(
        value,
        policy=policy,
        depth=0,
        reasons=reasons,
        active_ids=set(),
        work_remaining=[policy.max_field_bytes],
    )
    if policy.redactor is not None:
        try:
            converted = policy.redactor(converted)
            reasons.append("custom_redactor")
            converted = _convert(
                converted,
                policy=policy,
                depth=0,
                reasons=reasons,
                active_ids=set(),
                work_remaining=[policy.max_field_bytes],
            )
        except Exception:
            reasons.append("redactor_failed")
            converted = {"__kitaru_capture__": "redactor_failed"}
    try:
        encoded_bytes = _json_encoded_size(converted, limit=policy.max_field_bytes)
    except _FieldTooLarge:
        reasons.append("max_field_bytes")
        converted = {"__kitaru_capture__": "max_field_bytes"}
        encoded_bytes = _json_encoded_size(converted, limit=None)
    except (TypeError, UnicodeEncodeError, ValueError):
        reasons.append("serialization_failed")
        converted = _serialization_marker(converted)
        encoded_bytes = _json_encoded_size(converted, limit=None)
    unique_reasons = tuple(dict.fromkeys(reasons))
    return CaptureResult(
        value=converted,
        lossy=bool(unique_reasons),
        truncated=any(reason.startswith("max_") for reason in unique_reasons),
        reasons=unique_reasons,
        encoded_bytes=encoded_bytes,
    )


def capture_execution_view(
    config: Mapping[str, Any] | None, policy: CapturePolicy
) -> CaptureResult:
    """Capture only explicitly allowed runnable configuration fields."""
    if config is None:
        return capture_value({}, policy)
    configurable = config.get("configurable")
    view: dict[str, Any] = {}
    if isinstance(config.get("tags"), Sequence) and not isinstance(
        config.get("tags"), (str, bytes)
    ):
        view["tags"] = list(config["tags"])
    if isinstance(config.get("metadata"), Mapping):
        view["metadata"] = dict(config["metadata"])
    if isinstance(configurable, Mapping) and "thread_id" in configurable:
        view["thread_id"] = configurable["thread_id"]
    if "run_id" in config:
        view["run_id"] = config["run_id"]
    return capture_value(view, policy)
