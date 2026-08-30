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
"""Property tests for LangGraph value capture."""

from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st

from kitaru_langgraph.capture import CapturePolicy, capture_value

_KNOWN = "#906"

# The string spellings are drawn alongside the non-string keys so that a draw can hold
# two distinct keys that `str()` maps onto the same name. Purely random text keys
# practically never collide with an int, bool, or tuple key, which left this property
# passing vacuously even at 2000 examples.
_colliding_names = st.sampled_from(["1", "0", "-1", "True", "False", "(0,)", "(1,)"])
_keys = st.one_of(
    st.text(max_size=8),
    _colliding_names,
    st.integers(-3, 3),
    st.booleans(),
    st.tuples(st.integers(0, 2)),
)
_values = st.recursive(
    st.none()
    | st.booleans()
    | st.integers()
    | st.floats(allow_nan=False)
    | st.text(max_size=20),
    lambda c: (
        st.lists(c, max_size=4) | st.dictionaries(st.text(max_size=8), c, max_size=4)
    ),
    max_leaves=10,
)


@given(value=st.dictionaries(st.text(max_size=8), _values, max_size=6))
def test_string_keyed_mapping_is_captured_exactly_or_flagged(
    value: dict[str, Any],
) -> None:
    result = capture_value(value, CapturePolicy())
    assert result.lossy or result.value == value


@pytest.mark.xfail(strict=True, reason=_KNOWN)
@given(value=st.dictionaries(_keys, _values, max_size=6))
def test_key_collapse_is_reported_as_lossy(value: dict[Any, Any]) -> None:
    result = capture_value(value, CapturePolicy())
    if len({str(k) for k in value}) < len(value):
        assert result.lossy and result.reasons and not result.replayable
    else:
        assert len(result.value) == len(value)


@pytest.mark.xfail(strict=True, reason=_KNOWN)
def test_colliding_keys_example() -> None:
    result = capture_value({1: "a", "1": "b"}, CapturePolicy())
    assert len(result.value) == 2 or result.lossy
