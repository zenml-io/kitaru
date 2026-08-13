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
"""Tests for tool call cache key derivation."""

from kitaru.cache_keys import compute_tool_cache_key


def test_key_is_stable_across_input_ordering() -> None:
    """Derive the same key for inputs differing only in key order."""
    first = compute_tool_cache_key("search", {"a": 1, "b": 2})
    second = compute_tool_cache_key("search", {"b": 2, "a": 1})

    assert first is not None
    assert len(first) == 64
    assert first == second


def test_key_differs_by_tool_name() -> None:
    """Derive different keys for the same inputs under different tools."""
    assert compute_tool_cache_key("search", {"q": "hi"}) != compute_tool_cache_key(
        "fetch", {"q": "hi"}
    )


def test_no_key_without_inputs() -> None:
    """Return no key when the inputs are absent."""
    assert compute_tool_cache_key("search", None) is None


def test_no_key_for_unserializable_inputs() -> None:
    """Return no key when the inputs are not JSON-serializable."""
    assert compute_tool_cache_key("search", {"q": object()}) is None


def test_no_key_for_non_finite_inputs() -> None:
    """Return no key when the inputs carry a non-finite float."""
    assert compute_tool_cache_key("search", {"q": float("nan")}) is None


def test_key_for_empty_inputs() -> None:
    """Derive a key for a call with recorded but empty inputs."""
    assert compute_tool_cache_key("search", {}) is not None
