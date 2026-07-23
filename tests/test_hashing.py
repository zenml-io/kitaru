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
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Tests for canonical JSON hashing."""

import hashlib

from kitaru.hashing import canonical_json, tool_call_cache_key


def test_canonical_json_sorts_keys() -> None:
    """Serialize objects with sorted keys regardless of insertion order."""
    assert canonical_json({"b": 1, "a": 2}) == canonical_json({"a": 2, "b": 1})
    assert canonical_json({"b": 1, "a": 2}) == b'{"a":2,"b":1}'


def test_canonical_json_compact_separators() -> None:
    """Serialize without whitespace."""
    assert (
        canonical_json({"a": [1, 2], "b": {"c": None}}) == b'{"a":[1,2],"b":{"c":null}}'
    )


def test_canonical_json_utf8() -> None:
    """Encode non-ASCII characters as UTF-8 instead of escapes."""
    assert canonical_json({"city": "Zürich"}) == '{"city":"Zürich"}'.encode()


def test_tool_call_cache_key() -> None:
    """Hash the canonical payload of tool name and inputs."""
    key = tool_call_cache_key("get_weather", {"city": "Berlin", "unit": "c"})
    expected = hashlib.sha256(
        b'{"inputs":{"city":"Berlin","unit":"c"},"tool":"get_weather"}'
    ).hexdigest()
    assert key == expected
    assert len(key) == 64


def test_tool_call_cache_key_input_order_stable() -> None:
    """Produce the same key for reordered input keys."""
    assert tool_call_cache_key("t", {"a": 1, "b": 2}) == tool_call_cache_key(
        "t", {"b": 2, "a": 1}
    )


def test_tool_call_cache_key_distinguishes_tool() -> None:
    """Produce different keys for different tools with equal inputs."""
    assert tool_call_cache_key("a", {"x": 1}) != tool_call_cache_key("b", {"x": 1})
