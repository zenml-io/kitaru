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

import pytest

from importers.kitaru import InvalidImport, parse
from kitaru.api_models.v1.imports import ImportFailure
from kitaru.api_models.v1.session import SessionStatus
from kitaru.task.importer import ParsedSession


def _session() -> dict[str, object]:
    """Return one complete portable Kitaru session."""
    return {
        "status": "completed",
        "name": "Weather request",
        "system_prompt": "Answer briefly.",
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
                "input_text": "How is the weather?",
                "output_text": "Sunny.",
                "system_prompt": "Answer briefly.",
                "reasoning": "The source reports clear skies.",
                "inputs": [{"role": "user", "content": "How is the weather?"}],
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
    assert isinstance(session, ParsedSession)
    assert session.status is SessionStatus.COMPLETED
    assert session.framework == "pydantic-ai"
    assert session.system_prompt == "Answer briefly."
    assert session.nodes[0].input_text == "How is the weather?"
    assert session.nodes[0].output_text == "Sunny."
    assert session.nodes[0].reasoning == "The source reports clear skies."


def test_isolates_invalid_lines_and_forbids_unknown_fields() -> None:
    """Keep valid lines while reporting malformed or out-of-contract records."""
    invalid = _session() | {"unknown": True}
    content = "\n".join((json.dumps(_session()), "not json", json.dumps(invalid)))

    parsed = list(parse(content.encode(), {}))

    assert isinstance(parsed[0], ParsedSession)
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
