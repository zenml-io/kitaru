#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
"""Versioned JSON codec for replayable LangGraph tool outcomes."""

import json
from collections.abc import Mapping, Sequence
from itertools import islice
from typing import Any, cast

from langchain_core.messages import ToolMessage
from langgraph.types import Command

from .capability import ToolPolicyError
from .capture import CapturePolicy, capture_value

TOOL_OUTCOME_SCHEMA = "kitaru.langgraph.tool_result.v1"
_NESTED_TYPE = "__kitaru_langgraph_type__"
_NESTED_VALUE = "value"


def _capture_marker(reason: str) -> dict[str, str]:
    return {"__kitaru_capture__": reason}


def _encode_nested(
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
        return _capture_marker("max_field_bytes")
    work_remaining[0] -= 1
    if depth > policy.max_depth:
        reasons.append("max_depth")
        return _capture_marker("max_depth")
    if isinstance(value, ToolMessage):
        identity = id(value)
        if identity in active_ids:
            reasons.append("cycle")
            return _capture_marker("cycle")
        active_ids.add(identity)
        try:
            fields = {
                "content": value.content,
                "additional_kwargs": value.additional_kwargs,
                "response_metadata": value.response_metadata,
                "name": value.name,
                "id": value.id,
                "tool_call_id": value.tool_call_id,
                "artifact": value.artifact,
                "status": value.status,
            }
            return {
                _NESTED_TYPE: "tool_message",
                **{
                    key: _encode_nested(
                        item,
                        policy=policy,
                        depth=depth + 1,
                        reasons=reasons,
                        active_ids=active_ids,
                        work_remaining=work_remaining,
                    )
                    for key, item in fields.items()
                },
            }
        finally:
            active_ids.remove(identity)
    if isinstance(value, Mapping):
        identity = id(value)
        if identity in active_ids:
            reasons.append("cycle")
            return _capture_marker("cycle")
        active_ids.add(identity)
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
            encoded: dict[str, Any] = {}
            for key, item in items:
                if work_remaining[0] <= 0:
                    reasons.append("max_field_bytes")
                    break
                output_key = str(key)
                if not isinstance(key, str):
                    reasons.append("non_string_key")
                if output_key in encoded:
                    reasons.append("key_collision")
                encoded[output_key] = _encode_nested(
                    item,
                    policy=policy,
                    depth=depth + 1,
                    reasons=reasons,
                    active_ids=active_ids,
                    work_remaining=work_remaining,
                )
        finally:
            active_ids.remove(identity)
        if _NESTED_TYPE in encoded:
            return {_NESTED_TYPE: "mapping", _NESTED_VALUE: encoded}
        return encoded
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        identity = id(value)
        if identity in active_ids:
            reasons.append("cycle")
            return _capture_marker("cycle")
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
            encoded_items: list[Any] = []
            for item in items:
                if work_remaining[0] <= 0:
                    reasons.append("max_field_bytes")
                    break
                encoded_items.append(
                    _encode_nested(
                        item,
                        policy=policy,
                        depth=depth + 1,
                        reasons=reasons,
                        active_ids=active_ids,
                        work_remaining=work_remaining,
                    )
                )
            if isinstance(value, tuple):
                return {_NESTED_TYPE: "tuple", _NESTED_VALUE: encoded_items}
            return encoded_items
        finally:
            active_ids.remove(identity)
    return value


def _decode_mapping(
    value: Mapping[Any, Any], *, tool_call_id: str, tool_name: str
) -> dict[str, Any]:
    return {
        str(key): _decode_nested(
            item,
            tool_call_id=tool_call_id,
            tool_name=tool_name,
        )
        for key, item in value.items()
    }


def _decode_nested(value: Any, *, tool_call_id: str, tool_name: str) -> Any:
    if isinstance(value, Mapping):
        nested_type = value.get(_NESTED_TYPE)
        if nested_type == "mapping":
            nested_value = value.get(_NESTED_VALUE)
            if not isinstance(nested_value, Mapping):
                raise ToolPolicyError("Stored nested mapping payload is malformed")
            return _decode_mapping(
                nested_value,
                tool_call_id=tool_call_id,
                tool_name=tool_name,
            )
        if nested_type == "tuple":
            nested_value = value.get(_NESTED_VALUE)
            if not isinstance(nested_value, list):
                raise ToolPolicyError("Stored nested tuple payload is malformed")
            return tuple(
                _decode_nested(item, tool_call_id=tool_call_id, tool_name=tool_name)
                for item in nested_value
            )
        if nested_type == "tool_message":
            fields = _decode_mapping(
                {key: item for key, item in value.items() if key != _NESTED_TYPE},
                tool_call_id=tool_call_id,
                tool_name=tool_name,
            )
            status = fields.get("status")
            if status not in ("success", "error"):
                raise ToolPolicyError("Stored ToolMessage status is malformed")
            return ToolMessage(
                content=fields.get("content"),
                additional_kwargs=dict(fields.get("additional_kwargs") or {}),
                response_metadata=dict(fields.get("response_metadata") or {}),
                name=tool_name,
                id=fields.get("id"),
                tool_call_id=tool_call_id,
                artifact=fields.get("artifact"),
                status=status,
            )
        if nested_type is not None:
            raise ToolPolicyError("Stored nested value type is unsupported")
        return _decode_mapping(
            value,
            tool_call_id=tool_call_id,
            tool_name=tool_name,
        )
    if isinstance(value, list):
        return [
            _decode_nested(item, tool_call_id=tool_call_id, tool_name=tool_name)
            for item in value
        ]
    return value


def encode_tool_outcome(
    result: ToolMessage | Command[Any],
    *,
    policy: CapturePolicy | None = None,
) -> dict[str, Any]:
    """Encode a supported public tool result into a tagged JSON envelope."""
    capture_policy = policy or CapturePolicy()
    preencode_reasons: list[str] = []
    active_ids: set[int] = set()
    work_remaining = [capture_policy.max_field_bytes]
    if isinstance(result, ToolMessage):
        kind = "tool_message"
        payload = _encode_nested(
            result,
            policy=capture_policy,
            depth=0,
            reasons=preencode_reasons,
            active_ids=active_ids,
            work_remaining=work_remaining,
        )
    elif isinstance(result, Command):
        kind = "command"
        payload = {
            "graph": result.graph,
            "update": _encode_nested(
                result.update,
                policy=capture_policy,
                depth=0,
                reasons=preencode_reasons,
                active_ids=active_ids,
                work_remaining=work_remaining,
            ),
            "resume": _encode_nested(
                result.resume,
                policy=capture_policy,
                depth=0,
                reasons=preencode_reasons,
                active_ids=active_ids,
                work_remaining=work_remaining,
            ),
            "goto": _encode_nested(
                result.goto,
                policy=capture_policy,
                depth=0,
                reasons=preencode_reasons,
                active_ids=active_ids,
                work_remaining=work_remaining,
            ),
        }
    else:
        raise TypeError("tool outcome must be a ToolMessage or Command")
    captured = capture_value(payload, capture_policy)
    loss_reasons = list(dict.fromkeys([*preencode_reasons, *captured.reasons]))
    return {
        "schema": TOOL_OUTCOME_SCHEMA,
        "kind": kind,
        "replayable": not loss_reasons,
        "loss_reasons": loss_reasons,
        "payload": captured.value,
    }


def decode_tool_outcome(
    value: Any,
    *,
    tool_call_id: str,
    tool_name: str,
) -> ToolMessage | Command[Any]:
    """Decode one exact tagged outcome and remap current call identity."""
    try:
        if not isinstance(value, Mapping) or value.get("schema") != TOOL_OUTCOME_SCHEMA:
            raise ToolPolicyError("Stored tool result has an unknown envelope")
        if value.get("replayable") is not True:
            raise ToolPolicyError("Stored tool result is not replayable")
        payload = value.get("payload")
        if not isinstance(payload, Mapping):
            raise ToolPolicyError("Stored tool result payload is malformed")
        kind = value.get("kind")
        if kind == "tool_message":
            decoded = _decode_nested(
                payload, tool_call_id=tool_call_id, tool_name=tool_name
            )
            if not isinstance(decoded, ToolMessage):
                raise ToolPolicyError("Stored ToolMessage payload is malformed")
            return decoded
        if kind == "command":
            required_fields = ("graph", "update", "resume", "goto")
            if not all(field in payload for field in required_fields):
                raise ToolPolicyError("Stored Command payload is malformed")
            return Command(
                graph=cast(str | None, payload["graph"]),
                update=_decode_nested(
                    payload["update"],
                    tool_call_id=tool_call_id,
                    tool_name=tool_name,
                ),
                resume=_decode_nested(
                    payload["resume"],
                    tool_call_id=tool_call_id,
                    tool_name=tool_name,
                ),
                goto=_decode_nested(
                    payload["goto"],
                    tool_call_id=tool_call_id,
                    tool_name=tool_name,
                ),
            )
        raise ToolPolicyError("Stored tool result kind is unsupported")
    except ToolPolicyError:
        raise
    except Exception:
        # Validation and custom mappings can include stored secrets in their errors.
        raise ToolPolicyError("Stored tool result payload is malformed") from None


def coerce_static_tool_result(
    value: Any,
    *,
    tool_call_id: str,
    tool_name: str,
) -> ToolMessage | Command[Any]:
    """Turn a static result or tagged outcome into a framework-valid result."""
    if isinstance(value, Mapping) and value.get("schema") == TOOL_OUTCOME_SCHEMA:
        return decode_tool_outcome(
            value, tool_call_id=tool_call_id, tool_name=tool_name
        )
    if isinstance(value, str):
        content = value
    else:
        content = json.dumps(value, sort_keys=True, separators=(",", ":"))
    return ToolMessage(
        content=content,
        artifact=value,
        name=tool_name,
        tool_call_id=tool_call_id,
    )
