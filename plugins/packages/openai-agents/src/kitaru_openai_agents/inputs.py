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
"""OpenAI input normalization shared by task and replay paths."""

import json
from typing import Any, cast

from agents import TResponseInputItem
from pydantic import TypeAdapter, ValidationError

_INPUT_ITEMS_ADAPTER = TypeAdapter(list[TResponseInputItem])
_TRUNCATION_REASONS = {
    "max_bytes",
    "max_characters",
    "max_depth",
    "max_items",
    "max_items_or_non_string_keys",
}


def parse_tool_arguments(value: str) -> Any:
    """Parse function tool arguments as strict JSON."""

    def reject_constant(constant: str) -> Any:
        raise ValueError(f"Invalid JSON constant: {constant}")

    return json.loads(value, parse_constant=reject_constant)


def contains_capture_marker(value: Any) -> bool:
    """Check whether captured data contains a lossy serialization marker."""
    if isinstance(value, dict):
        truncation = value.get("_kitaru_truncated")
        if (
            isinstance(truncation, dict)
            and truncation.get("reason") in _TRUNCATION_REASONS
        ):
            return True
        if set(value) == {"_kitaru_unsupported_type"} and isinstance(
            value["_kitaru_unsupported_type"], str
        ):
            return True
        return any(contains_capture_marker(item) for item in value.values())
    if isinstance(value, list):
        return any(contains_capture_marker(item) for item in value)
    return False


def normalize_openai_input(value: Any) -> str | list[TResponseInputItem]:
    """Keep valid OpenAI input lists and serialize other JSON deterministically."""
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        try:
            _INPUT_ITEMS_ADAPTER.validate_python(value)
        except ValidationError:
            pass
        else:
            return cast(list[TResponseInputItem], value)
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
