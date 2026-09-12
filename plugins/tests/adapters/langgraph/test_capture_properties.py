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

import copy
import json
from typing import Any

from hypothesis import given
from hypothesis import strategies as st

from kitaru_langgraph.capture import CapturePolicy, capture_value

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

_json_text = st.text(alphabet=st.characters(blacklist_categories=("Cs",)), max_size=23)
_json_strings = st.one_of(
    _json_text,
    st.tuples(
        st.sampled_from(['"', "\\", "\b", "\f", "\n", "\r", "\t", "\x00", "é", "😀"]),
        _json_text,
    ).map(lambda parts: "".join(parts)),
)
_json_keys = _json_strings.map(lambda value: f"field:{value}")
_json_values = st.recursive(
    st.none()
    | st.booleans()
    | st.integers(min_value=-(10**30), max_value=10**30)
    | st.floats(allow_nan=False, allow_infinity=False)
    | _json_strings,
    lambda children: (
        st.lists(children, max_size=4)
        | st.dictionaries(_json_keys, children, max_size=4)
    ),
    max_leaves=12,
)


def _get_json_size(value: Any) -> int:
    """Return the independent compact JSON UTF-8 byte count."""
    return len(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    )


@given(value=st.dictionaries(st.text(max_size=8), _values, max_size=6))
def test_string_keyed_mapping_is_captured_exactly_or_flagged(
    value: dict[str, Any],
) -> None:
    result = capture_value(value, CapturePolicy())
    assert result.lossy or result.value == value


@given(value=st.dictionaries(_keys, _values, max_size=6))
def test_key_collapse_is_reported_as_lossy(value: dict[Any, Any]) -> None:
    result = capture_value(value, CapturePolicy())
    if any(not isinstance(key, str) for key in value):
        assert result.lossy and result.reasons and not result.replayable
    if len({str(k) for k in value}) == len(value):
        assert len(result.value) == len(value)


def test_colliding_keys_example() -> None:
    result = capture_value({1: "a", "1": "b"}, CapturePolicy())
    assert len(result.value) == 2 or result.lossy


@given(value=_json_values)
def test_json_native_values_have_exact_byte_receipts(value: Any) -> None:
    original = copy.deepcopy(value)

    result = capture_value(value, CapturePolicy())

    assert result.value == value
    assert result.encoded_bytes == _get_json_size(value)
    assert result.replayable
    assert not result.lossy
    assert not result.truncated
    assert not result.reasons
    assert value == original


@given(value=_json_values)
def test_field_byte_limit_is_inclusive(value: Any) -> None:
    encoded_bytes = _get_json_size(value)

    at = capture_value(value, CapturePolicy(max_field_bytes=encoded_bytes))
    above = capture_value(value, CapturePolicy(max_field_bytes=encoded_bytes + 1))

    if encoded_bytes > 1:
        below = capture_value(value, CapturePolicy(max_field_bytes=encoded_bytes - 1))
        assert below.value == {"__kitaru_capture__": "max_field_bytes"}
        assert below.encoded_bytes == _get_json_size(below.value)
        assert below.reasons == ("max_field_bytes",)
        assert below.lossy and below.truncated and not below.replayable

    for result in (at, above):
        assert result.value == value
        assert result.encoded_bytes == encoded_bytes
        assert result.replayable
        assert not result.reasons


@given(max_field_bytes=st.integers(min_value=1, max_value=16))
def test_tiny_budget_reports_final_marker_size(max_field_bytes: int) -> None:
    result = capture_value(
        "value-too-large-for-the-generated-budget",
        CapturePolicy(max_field_bytes=max_field_bytes),
    )

    assert result.value == {"__kitaru_capture__": "max_field_bytes"}
    assert result.encoded_bytes == _get_json_size(result.value)
    assert result.encoded_bytes > max_field_bytes
    assert result.reasons == ("max_field_bytes",)
    assert result.lossy and result.truncated and not result.replayable


@given(
    prefix=_json_strings,
    surrogate=st.integers(min_value=0xD800, max_value=0xDFFF).map(chr),
    suffix=_json_strings,
)
def test_unencodable_strings_use_serialization_marker(
    prefix: str, surrogate: str, suffix: str
) -> None:
    value = f"{prefix}{surrogate}{suffix}"

    result = capture_value(value, CapturePolicy())

    assert result.value == {
        "__kitaru_capture__": "serialization_failed",
        "type": "str",
    }
    assert result.encoded_bytes == _get_json_size(result.value)
    assert result.reasons == ("serialization_failed",)
    assert result.lossy and not result.truncated and not result.replayable


@given(value=_json_values, max_field_bytes=st.integers(min_value=1, max_value=128))
def test_loss_flags_and_receipts_remain_consistent(
    value: Any, max_field_bytes: int
) -> None:
    original = copy.deepcopy(value)

    result = capture_value(value, CapturePolicy(max_field_bytes=max_field_bytes))

    assert result.encoded_bytes == _get_json_size(result.value)
    assert result.lossy == bool(result.reasons)
    assert result.replayable == (not result.lossy)
    assert result.truncated == any(
        reason.startswith("max_") for reason in result.reasons
    )
    assert value == original
