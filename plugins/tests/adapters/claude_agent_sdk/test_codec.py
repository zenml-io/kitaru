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
"""Claude SDK MCP tool-result codec properties."""

import json
import traceback
from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st

from kitaru_claude_agent_sdk import ToolPolicyError
from kitaru_claude_agent_sdk.codec import (
    MAX_TEXT_BLOCKS,
    MAX_TOOL_RESULT_BYTES,
    TOOL_RESULT_SCHEMA,
    decode_tool_result,
    encode_tool_result,
)

_json_text = st.text(alphabet=st.characters(blacklist_categories=("Cs",)), max_size=256)
_text_blocks = st.lists(
    _json_text.map(lambda text: {"type": "text", "text": text}),
    max_size=12,
)


def _compact_utf8_size(value: Any) -> int:
    return len(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    )


@given(blocks=_text_blocks, is_error=st.booleans(), include_flag=st.booleans())
def test_supported_results_round_trip_through_json(
    blocks: list[dict[str, str]], is_error: bool, include_flag: bool
) -> None:
    result: dict[str, Any] = {"content": blocks}
    if include_flag:
        result["is_error"] = is_error

    envelope = encode_tool_result(result)

    assert envelope["schema"] == TOOL_RESULT_SCHEMA
    assert envelope["replayable"] is True
    assert decode_tool_result(json.loads(json.dumps(envelope))) == {
        "content": blocks,
        "is_error": is_error if include_flag else False,
    }


@given(
    secret=st.text(min_size=8, max_size=20, alphabet="abcdef0123456789"),
    unsupported=st.sampled_from(["extra", "image", "non_boolean"]),
)
def test_unsupported_results_are_marked_without_exposing_payloads(
    secret: str, unsupported: str
) -> None:
    if unsupported == "extra":
        result: Any = {"content": [], "private": secret}
    elif unsupported == "image":
        result = {"content": [{"type": "image", "data": secret}]}
    else:
        result = {"content": [], "is_error": secret}

    envelope = encode_tool_result(result)

    assert envelope["replayable"] is False
    assert envelope["payload"] is None
    assert secret not in json.dumps(envelope)
    with pytest.raises(ToolPolicyError) as caught:
        decode_tool_result(json.loads(json.dumps(envelope)))
    assert secret not in "".join(traceback.format_exception(caught.value))


@pytest.mark.parametrize(
    "stored",
    [
        {},
        {"schema": "future", "replayable": True, "payload": {}},
        {
            "schema": TOOL_RESULT_SCHEMA,
            "replayable": True,
            "payload": {"content": [{"type": "image", "data": "x"}]},
        },
    ],
)
def test_codec_fails_closed_for_malformed_or_non_replayable_values(stored: Any) -> None:
    with pytest.raises(ToolPolicyError):
        decode_tool_result(stored)


def test_tool_result_byte_limit_boundary() -> None:
    empty_payload = {
        "content": [{"type": "text", "text": ""}],
        "is_error": False,
    }
    compact_json_overhead = _compact_utf8_size(empty_payload)
    text_at_limit = "x" * (MAX_TOOL_RESULT_BYTES - compact_json_overhead)
    result_at_limit = {"content": [{"type": "text", "text": text_at_limit}]}
    result_above_limit = {"content": [{"type": "text", "text": f"{text_at_limit}x"}]}
    payload_at_limit = {**result_at_limit, "is_error": False}
    payload_above_limit = {**result_above_limit, "is_error": False}

    assert _compact_utf8_size(payload_at_limit) == MAX_TOOL_RESULT_BYTES
    assert _compact_utf8_size(payload_above_limit) == MAX_TOOL_RESULT_BYTES + 1

    envelope_at_limit = encode_tool_result(result_at_limit)
    envelope_above_limit = encode_tool_result(result_above_limit)

    assert envelope_at_limit["schema"] == TOOL_RESULT_SCHEMA
    assert envelope_at_limit["replayable"] is True
    assert envelope_above_limit["schema"] == TOOL_RESULT_SCHEMA
    assert envelope_above_limit["replayable"] is False
    assert envelope_above_limit["payload"] is None
    with pytest.raises(ToolPolicyError):
        decode_tool_result(envelope_above_limit)


@pytest.mark.parametrize("count", [MAX_TEXT_BLOCKS, MAX_TEXT_BLOCKS + 1])
def test_text_block_count_boundary(count: int) -> None:
    envelope = encode_tool_result({"content": [{"type": "text", "text": ""}] * count})

    assert envelope["replayable"] is (count == MAX_TEXT_BLOCKS)
