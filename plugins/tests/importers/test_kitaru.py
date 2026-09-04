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
"""Tests for the Kitaru JSONL importer plugin."""

import json
from typing import Any

import pytest

from kitaru.api_models.v1.imports import ImportFailure
from kitaru.api_models.v1.session import SessionStatus
from kitaru.task.importer import ImportedSession
from kitaru_jsonl_importer.importer import InvalidImport, parse


def _session() -> dict[str, Any]:
    """Return one complete portable Kitaru session."""
    return {
        "status": "completed",
        "name": "Weather request",
        "inputs": {"question": "How is the weather?"},
        "outputs": {"answer": "Sunny."},
        "error": None,
        "started_at": "2026-07-22T10:00:00Z",
        "ended_at": "2026-07-22T10:00:01Z",
        "external_id": "session-1",
        "metadata": {"environment": "test"},
        "framework": "pydantic-ai",
        "nodes": [
            {
                "index": 0,
                "parent_index": None,
                "secondary_parent_indexes": [],
                "external_id": "node-1",
                "trace_id": "trace-1",
                "node_type": "llm_call",
                "name": "model request",
                "status": "completed",
                "input_text_selector": "/1/content",
                "output_text_selector": "/0/content",
                "system_prompt_selector": "/0/content",
                "reasoning": "The source reports clear skies.",
                "inputs": [
                    {"role": "system", "content": "Answer briefly."},
                    {"role": "user", "content": "How is the weather?"},
                ],
                "outputs": [{"role": "assistant", "content": "Sunny."}],
                "attributes": {},
                "metadata": {},
            }
        ],
    }


def test_parses_one_complete_session_per_line() -> None:
    """Validate the public portable session contract without transformation."""
    content = (json.dumps(_session()) + "\n").encode()

    parsed = list(parse(content, {}))

    assert len(parsed) == 1
    session = parsed[0]
    assert isinstance(session, ImportedSession)
    assert session.status is SessionStatus.COMPLETED
    assert session.framework == "pydantic-ai"
    assert session.nodes[0].input_text_selector == "/1/content"
    assert session.nodes[0].output_text_selector == "/0/content"
    assert session.nodes[0].system_prompt_selector == "/0/content"
    assert session.nodes[0].reasoning == "The source reports clear skies."


def test_isolates_invalid_lines_and_forbids_unknown_fields() -> None:
    """Keep valid lines while reporting malformed or out-of-contract records."""
    invalid = _session() | {"unknown": True}
    content = "\n".join((json.dumps(_session()), "not json", json.dumps(invalid)))

    parsed = list(parse(content.encode(), {}))

    assert isinstance(parsed[0], ImportedSession)
    assert isinstance(parsed[1], ImportFailure)
    assert parsed[1].line == 2
    assert isinstance(parsed[2], ImportFailure)
    assert parsed[2].line == 3
    assert "extra_forbidden" in parsed[2].error


def test_rejects_nested_importer_nodes() -> None:
    """Require the public flat node representation."""
    nested = _session()
    nested["nodes"] = [
        {
            "node_type": "span",
            "name": "run",
            "status": "completed",
            "inputs": {},
            "outputs": {},
            "attributes": {},
            "children": [],
        }
    ]

    parsed = list(parse(json.dumps(nested).encode(), {}))

    assert isinstance(parsed[0], ImportFailure)
    assert "flat indexed representation" in parsed[0].error


@pytest.mark.parametrize("content", [b"", b" \n ", b"\xff"])
def test_rejects_empty_or_non_utf8_uploads(content: bytes) -> None:
    """Reject uploads that cannot contain JSONL records."""
    with pytest.raises(InvalidImport):
        list(parse(content, {}))


@pytest.mark.parametrize("value", ["NaN", "sNaN", "Infinity", "-Infinity", -1])
def test_invalid_node_cost_isolates_line(value: object) -> None:
    """Enforce finite nonnegative costs without changing the shared model."""
    bad = _session()
    bad["nodes"][0]["cost"] = value
    content = "\n".join(json.dumps(record) for record in (_session(), bad, _session()))
    results = list(parse(content.encode(), {}))
    assert [type(item) for item in results] == [
        ImportedSession,
        ImportFailure,
        ImportedSession,
    ]
    for item in results:
        item.model_dump_json()


@pytest.mark.parametrize(
    "field",
    ["input_tokens", "output_tokens", "cached_input_tokens", "reasoning_tokens"],
)
def test_negative_tokens_isolate_line(field: str) -> None:
    """Reject each negative mapped count locally."""
    bad = _session()
    bad["nodes"][0]["tokens"] = {field: -1}
    results = list(
        parse((json.dumps(bad) + "\n" + json.dumps(_session())).encode(), {})
    )
    assert [type(item) for item in results] == [ImportFailure, ImportedSession]


def test_surrogates_and_decoder_recursion_isolate_lines() -> None:
    """Both decoding and final serialization failures preserve valid lines."""
    bad = _session() | {"external_id": "bad\ud800", "metadata": {"nested": "\udfff"}}
    deep = "[" * 2000 + "0" + "]" * 2000
    content = "\n".join(
        (json.dumps(_session()), json.dumps(bad), deep, json.dumps(_session()))
    )
    results = list(parse(content.encode(), {}))
    assert [type(item) for item in results] == [
        ImportedSession,
        ImportFailure,
        ImportFailure,
        ImportedSession,
    ]
    for item in results:
        item.model_dump_json()


def test_flat_chain_is_not_subject_to_nested_depth_limit() -> None:
    """Accept a 128-node indexed chain without building a nested tree."""
    value = _session()
    node = value["nodes"][0]
    value["nodes"] = [
        node | {"index": index, "parent_index": index - 1 if index else None}
        for index in range(128)
    ]
    [session] = list(parse(json.dumps(value).encode(), {}))
    assert isinstance(session, ImportedSession)
    assert len(session.nodes) == 128
    session.model_dump_json()
