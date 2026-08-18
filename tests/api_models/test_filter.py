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
"""Tests for filter API models."""

import pytest
from pydantic import TypeAdapter, ValidationError

from kitaru.api_models.v1.filter import FilterCondition, FilterParam

_adapter: TypeAdapter[FilterParam] = TypeAdapter(FilterParam)


def _nested_filter_json(depth: int) -> str:
    """Build a JSON filter string nested ``depth`` levels of ``and``.

    Args:
        depth: Number of nested ``and`` operators.

    Returns:
        JSON-encoded filter string.
    """
    leaf = '{"field":"name","op":"eq","value":"x"}'
    return '{"and":[' * depth + leaf + "]}" * depth


def test_condition_from_json_string() -> None:
    """Parse a single condition from its JSON string encoding."""
    parsed = _adapter.validate_python('{"field":"name","op":"eq","value":"x"}')
    assert parsed == FilterCondition(field="name", op="eq", value="x")


def test_deeply_nested_filter_rejected_cleanly() -> None:
    """Reject an over-deep filter with a validation error, not a RecursionError."""
    payload = _nested_filter_json(2000)
    with pytest.raises(ValidationError):
        _adapter.validate_python(payload)


def test_filter_within_depth_cap_accepted() -> None:
    """Accept a filter nested within the depth cap."""
    parsed = _adapter.validate_python(_nested_filter_json(2))
    assert parsed is not None
