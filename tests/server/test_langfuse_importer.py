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
"""Langfuse JSONL importer tests."""

import json
import uuid
from typing import Any

import pytest

from kitaru.server.adapters.importers.langfuse import LangfuseJSONLImporter
from kitaru.server.application.models.import_jobs import ImportContext
from kitaru.server.domain.import_job import InvalidImport
from kitaru.server.domain.session import SessionStatus
from kitaru.server.domain.session_node import NodeStatus, NodeType


def jsonl(*records: dict[str, Any]) -> bytes:
    """Encode records as JSONL."""
    return b"\n".join(json.dumps(record).encode() for record in records)


def context(source_instance: str | None = None) -> ImportContext:
    """Build an import context."""
    return ImportContext(
        agent_version_id=uuid.uuid4(),
        source_instance=source_instance,
    )


def observation(
    observation_id: str,
    trace_id: str,
    *,
    session_id: str | None = "conversation-1",
    project_id: str | None = "project-1",
    parent_id: str | None = None,
    observation_type: str = "SPAN",
    input_: Any = None,
    output: Any = None,
    start_time: str = "2026-07-24T10:00:00Z",
    **extra: Any,
) -> dict[str, Any]:
    """Build one enriched Langfuse observation."""
    record: dict[str, Any] = {
        "id": observation_id,
        "traceId": trace_id,
        "type": observation_type,
        "name": observation_id,
        "input": input_,
        "output": output,
        "startTime": start_time,
        "endTime": "2026-07-24T10:00:01Z",
        **extra,
    }
    if session_id is not None:
        record["sessionId"] = session_id
    if project_id is not None:
        record["projectId"] = project_id
    if parent_id is not None:
        record["parentObservationId"] = parent_id
    return record


def test_imports_multiturn_observations() -> None:
    """Group source traces into a multi-turn Kitaru session."""
    batch = LangfuseJSONLImporter().parse(
        jsonl(
            observation(
                "root-1",
                "trace-1",
                input_={"message": "hello"},
                output={"answer": "hi"},
            ),
            observation(
                "generation-1",
                "trace-1",
                parent_id="root-1",
                observation_type="GENERATION",
                input_={"messages": ["hello"]},
                output={"text": "hi"},
                modelId="gpt-5",
                usageDetails={"input": 3, "output": 2},
            ),
            observation(
                "root-2",
                "trace-2",
                input_={"message": "again"},
                output={"answer": "done"},
                start_time="2026-07-24T10:01:00Z",
            ),
            observation(
                "tool-1",
                "trace-2",
                parent_id="root-2",
                observation_type="TOOL",
                input_={"city": "Berlin"},
                output={"temperature": 21},
                toolName="weather",
                start_time="2026-07-24T10:01:00.100000Z",
            ),
        ),
        context(),
    )

    assert batch.errors == []
    assert len(batch.sessions) == 1
    session = batch.sessions[0]
    assert session.source_id == "conversation-1"
    assert session.source_instance == "project-1"
    assert session.status is SessionStatus.COMPLETED
    assert session.inputs == {
        "schema_version": 1,
        "turns": [
            {"source_trace_id": "trace-1", "inputs": {"message": "hello"}},
            {"source_trace_id": "trace-2", "inputs": {"message": "again"}},
        ],
    }
    assert [turn.trace_id for turn in session.turns] == ["trace-1", "trace-2"]
    nodes = {node.source_id: node for node in session.nodes}
    assert nodes["trace-1:root-1"].node_type is NodeType.SPAN
    assert nodes["trace-1:generation-1"].node_type is NodeType.LLM_CALL
    assert nodes["trace-1:generation-1"].parent_source_id == "trace-1:root-1"
    assert nodes["trace-2:root-2"].node_type is NodeType.SPAN
    assert nodes["trace-2:tool-1"].node_type is NodeType.TOOL_CALL
    assert nodes["trace-2:tool-1"].tool_name == "weather"
    assert session.readiness.level == "ready"
    assert session.readiness.tool_call_count == 1
    assert session.readiness.replayable_tool_call_count == 1


def test_trace_without_session_id_becomes_one_turn_session() -> None:
    """Use a trace id when Langfuse has no session id."""
    batch = LangfuseJSONLImporter().parse(
        jsonl(observation("root", "trace-1", session_id=None, input_="hello")),
        context(),
    )

    assert batch.sessions[0].source_id == "trace-1"
    assert len(batch.sessions[0].turns) == 1


def test_imports_nested_trace_rows() -> None:
    """Detect trace-per-line JSONL and carry trace context to observations."""
    trace = {
        "id": "trace-1",
        "sessionId": "conversation-1",
        "projectId": "project-1",
        "name": "support turn",
        "input": {"message": "hello"},
        "output": {"answer": "hi"},
        "observations": [
            {
                "id": "root",
                "type": "SPAN",
                "name": "agent",
                "startTime": "2026-07-24T10:00:00Z",
                "endTime": "2026-07-24T10:00:01Z",
            }
        ],
    }

    batch = LangfuseJSONLImporter().parse(jsonl(trace), context())

    assert batch.sessions[0].name == "support turn"
    assert batch.sessions[0].turns[0].inputs == {"message": "hello"}
    assert batch.sessions[0].outputs == {"answer": "hi"}


def test_imports_legacy_ingestion_events() -> None:
    """Detect ingestion-event JSONL and merge create and update events."""
    batch = LangfuseJSONLImporter().parse(
        jsonl(
            {
                "type": "trace-create",
                "body": {
                    "id": "trace-1",
                    "sessionId": "conversation-1",
                    "projectId": "project-1",
                    "input": {"message": "hello"},
                },
            },
            {
                "type": "span-create",
                "body": {
                    "id": "root",
                    "traceId": "trace-1",
                    "name": "agent",
                    "startTime": "2026-07-24T10:00:00Z",
                },
            },
            {
                "type": "span-update",
                "body": {
                    "id": "root",
                    "endTime": "2026-07-24T10:00:01Z",
                    "output": {"answer": "hi"},
                },
            },
        ),
        context(),
    )

    assert batch.errors == []
    assert batch.sessions[0].nodes[0].outputs == {"answer": "hi"}


def test_reports_one_invalid_group_without_losing_valid_sessions() -> None:
    """Isolate semantic errors by grouped source session."""
    batch = LangfuseJSONLImporter().parse(
        jsonl(
            observation(
                "valid-root",
                "valid-trace",
                session_id="valid-session",
                input_="hello",
            ),
            observation(
                "invalid-root",
                "invalid-trace",
                session_id="invalid-session",
                project_id=None,
                input_="hello",
            ),
        ),
        context(),
    )

    assert [session.source_id for session in batch.sessions] == ["valid-session"]
    assert len(batch.errors) == 1
    assert batch.errors[0].source_id == "invalid-session"
    assert "provide source_instance" in batch.errors[0].message


def test_source_instance_selection_handles_exports_without_project_ids() -> None:
    """Apply the explicit source instance when the export omits a project id."""
    batch = LangfuseJSONLImporter().parse(
        jsonl(observation("root", "trace-1", project_id=None, input_="hello")),
        context(source_instance="selected-project"),
    )

    assert batch.errors == []
    assert batch.sessions[0].source_instance == "selected-project"


def test_status_message_does_not_imply_failure() -> None:
    """Treat a status message as diagnostic unless its level failed."""
    batch = LangfuseJSONLImporter().parse(
        jsonl(
            observation(
                "root",
                "trace-1",
                input_="hello",
                level="WARNING",
                statusMessage="model retried once",
            )
        ),
        context(),
    )

    assert batch.sessions[0].status is SessionStatus.COMPLETED
    assert batch.sessions[0].nodes[0].status is NodeStatus.COMPLETED


def test_exact_normalized_content_has_stable_digest() -> None:
    """Ignore JSONL row order when hashing normalized evidence."""
    first = observation("root", "trace-1", input_="hello")
    child = observation(
        "child",
        "trace-1",
        parent_id="root",
        observation_type="GENERATION",
    )
    importer = LangfuseJSONLImporter()

    forward = importer.parse(jsonl(first, child), context()).sessions[0]
    reversed_ = importer.parse(jsonl(child, first), context()).sessions[0]

    assert forward.content_digest == reversed_.content_digest


def test_rejects_mixed_row_shapes() -> None:
    """Reject files that mix incompatible export representations."""
    nested_trace = {"id": "trace-1", "observations": []}
    enriched = observation("root", "trace-2")

    with pytest.raises(InvalidImport, match="mixes multiple"):
        LangfuseJSONLImporter().parse(jsonl(nested_trace, enriched), context())
