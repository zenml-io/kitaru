#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""Versioned JSON codec for replayable Claude SDK MCP tool results."""

import json
from collections.abc import Mapping
from typing import Any

from .capability import ToolPolicyError

TOOL_RESULT_SCHEMA = "kitaru.claude_agent_sdk.tool_result.v1"
MAX_TOOL_RESULT_BYTES = 64 * 1024
MAX_TEXT_BLOCKS = 100


def normalize_tool_result(value: Any) -> dict[str, Any]:
    """Return the exact replayable text-only MCP result subset."""
    if not isinstance(value, Mapping):
        raise ToolPolicyError("Claude SDK MCP tool result must be a mapping")
    if set(value) - {"content", "is_error"}:
        raise ToolPolicyError("Claude SDK MCP tool result has unsupported fields")
    content = value.get("content")
    if not isinstance(content, list) or len(content) > MAX_TEXT_BLOCKS:
        raise ToolPolicyError("Claude SDK MCP tool result content is malformed")
    blocks: list[dict[str, str]] = []
    for block in content:
        if (
            not isinstance(block, Mapping)
            or set(block) != {"type", "text"}
            or block.get("type") != "text"
            or not isinstance(block.get("text"), str)
        ):
            raise ToolPolicyError(
                "Claude SDK MCP replay supports text content blocks only"
            )
        blocks.append({"type": "text", "text": block["text"]})
    is_error = value.get("is_error", False)
    if not isinstance(is_error, bool):
        raise ToolPolicyError("Claude SDK MCP tool result is_error must be boolean")
    payload = {"content": blocks, "is_error": is_error}
    try:
        encoded = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError, RecursionError) as error:
        raise ToolPolicyError(
            "Claude SDK MCP tool result is not JSON-safe UTF-8"
        ) from error
    if len(encoded) > MAX_TOOL_RESULT_BYTES:
        raise ToolPolicyError("Claude SDK MCP tool result exceeds the replay limit")
    return payload


def encode_tool_result(value: Any) -> dict[str, Any]:
    """Encode a result, marking unsupported values as non-replayable."""
    try:
        payload = normalize_tool_result(value)
    except ToolPolicyError as error:
        return {
            "schema": TOOL_RESULT_SCHEMA,
            "replayable": False,
            "reason": str(error),
            "payload": None,
        }
    return {
        "schema": TOOL_RESULT_SCHEMA,
        "replayable": True,
        "payload": payload,
    }


def decode_tool_result(value: Any) -> dict[str, Any]:
    """Decode one exact, replayable Claude SDK MCP result envelope."""
    if not isinstance(value, Mapping) or value.get("schema") != TOOL_RESULT_SCHEMA:
        raise ToolPolicyError("Stored Claude SDK MCP result has an unknown envelope")
    if value.get("replayable") is not True:
        raise ToolPolicyError("Stored Claude SDK MCP result is not replayable")
    if set(value) != {"schema", "replayable", "payload"}:
        raise ToolPolicyError("Stored Claude SDK MCP result envelope is malformed")
    return normalize_tool_result(value.get("payload"))


__all__ = [
    "MAX_TEXT_BLOCKS",
    "MAX_TOOL_RESULT_BYTES",
    "TOOL_RESULT_SCHEMA",
    "decode_tool_result",
    "encode_tool_result",
    "normalize_tool_result",
]
