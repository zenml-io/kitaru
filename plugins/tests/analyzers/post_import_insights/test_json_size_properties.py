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
"""Property tests for the bounded JSON string size used by payload budgets."""

import pytest
from hypothesis import given
from hypothesis import strategies as st

from kitaru_post_import_insights import profiling

_CHARACTERS = st.one_of(
    st.characters(),
    st.sampled_from(['"', "\\", "\b", "\t", "\n", "\f", "\r", "\x00", "\x1b", "\x7f"]),
    st.sampled_from([chr(0x2028), chr(0x2029), "é", "中", "\U0001f600"]),
    st.integers(min_value=0xD800, max_value=0xDFFF).map(chr),
)
_TEXT = st.text(_CHARACTERS, max_size=40)


def _get_reference_size(value: str, limit: int) -> int | None:
    """Count UTF-8 bytes of the JSON string one character at a time."""
    size = 2
    for character in value:
        codepoint = ord(character)
        if character in {"\b", "\t", "\n", "\f", "\r", '"', "\\"}:
            size += 2
        elif codepoint < 0x20:
            size += 6
        elif 0xD800 <= codepoint <= 0xDFFF:
            return None
        else:
            size += len(character.encode("utf-8"))
    return size if size <= limit else None


@given(_TEXT, st.integers(min_value=0, max_value=200))
def test_size_matches_the_per_character_count(value: str, limit: int) -> None:
    assert profiling._calculate_json_string_size(value, limit) == (
        _get_reference_size(value, limit)
    )


@given(_TEXT)
def test_size_is_exact_at_the_limit_boundary(value: str) -> None:
    size = _get_reference_size(value, 10**9)
    if size is None:
        assert profiling._calculate_json_string_size(value, 10**9) is None
        return
    assert profiling._calculate_json_string_size(value, size) == size
    assert profiling._calculate_json_string_size(value, size - 1) is None


@pytest.mark.parametrize(
    ("value", "size"),
    [
        ("", 2),
        ('a"b', 6),
        ("\x01", 8),
        ("\x7f", 3),
        (chr(0x2028), 5),
        ("\U0001f600", 6),
        ("ok\ud800", None),
    ],
)
def test_size_of_escaped_and_multibyte_characters(value: str, size: int | None) -> None:
    assert profiling._calculate_json_string_size(value, 100) == size
