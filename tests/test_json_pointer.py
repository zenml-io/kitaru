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
"""Tests for RFC 6901 JSON Pointer resolution."""

from kitaru.json_pointer import resolve_json_pointer

DOCUMENT = {
    "messages": [{"content": "hi"}, {"content": "there"}],
    "a/b": 1,
    "m~n": 2,
    "": 3,
}


def test_empty_pointer_selects_the_document() -> None:
    """Select the whole document for the empty pointer."""
    assert resolve_json_pointer(DOCUMENT, "") == (True, DOCUMENT)


def test_pointer_without_leading_slash_misses() -> None:
    """Miss a pointer that does not start with a slash."""
    assert resolve_json_pointer(DOCUMENT, "messages") == (False, None)


def test_object_member_selected_by_key() -> None:
    """Select an object member by its key."""
    assert resolve_json_pointer(DOCUMENT, "/messages/0/content") == (True, "hi")


def test_empty_key_selected() -> None:
    """Select the member stored under the empty key."""
    assert resolve_json_pointer(DOCUMENT, "/") == (True, 3)


def test_escaped_tokens_decoded() -> None:
    """Decode the ~1 and ~0 escapes before looking a key up."""
    assert resolve_json_pointer(DOCUMENT, "/a~1b") == (True, 1)
    assert resolve_json_pointer(DOCUMENT, "/m~0n") == (True, 2)


def test_invalid_escape_misses() -> None:
    """Miss a token carrying an escape other than ~0 or ~1."""
    assert resolve_json_pointer(DOCUMENT, "/a~2b") == (False, None)
    assert resolve_json_pointer(DOCUMENT, "/messages~") == (False, None)


def test_missing_key_misses() -> None:
    """Miss a key the object does not carry."""
    assert resolve_json_pointer(DOCUMENT, "/absent") == (False, None)


def test_index_out_of_range_misses() -> None:
    """Miss an array index beyond the last element."""
    assert resolve_json_pointer(DOCUMENT, "/messages/2") == (False, None)


def test_leading_zero_index_misses() -> None:
    """Miss an array index written with a leading zero."""
    assert resolve_json_pointer(DOCUMENT, "/messages/00") == (False, None)


def test_negative_index_misses() -> None:
    """Miss a negative array index."""
    assert resolve_json_pointer(DOCUMENT, "/messages/-1") == (False, None)


def test_non_decimal_index_misses() -> None:
    """Miss an array index written in non-ASCII digits."""
    assert resolve_json_pointer(DOCUMENT, "/messages/\N{FULLWIDTH DIGIT ZERO}") == (
        False,
        None,
    )


def test_descent_into_scalar_misses() -> None:
    """Miss a pointer that descends past a scalar."""
    assert resolve_json_pointer(DOCUMENT, "/messages/0/content/0") == (False, None)


def test_null_member_selected() -> None:
    """Select a member whose value is null, distinguishing it from a miss."""
    assert resolve_json_pointer({"reasoning": None}, "/reasoning") == (True, None)
