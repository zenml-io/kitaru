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
from decimal import Decimal
from typing import Any

import pytest
from kitaru_importer_langfuse import LangfuseJSONLImporter, parse

from kitaru.importers import (
    ImportContext,
    InvalidImport,
    NodeStatus,
    NodeType,
    SessionStatus,
)
from kitaru.task.importer import ParsedSession


def jsonl(*records: dict[str, Any]) -> bytes:
    """Encode records as JSONL."""
    return b"\n".join(json.dumps(record).encode() for record in records)


def context(source_instance: str | None = None) -> ImportContext:
    """Build an import context."""
    return ImportContext(source_instance=source_instance)


def test_unified_parse_returns_prefixed_external_id() -> None:
    """Expose normalized sessions through the unified plugin entrypoint."""
    parsed = list(
        parse(
            jsonl(observation("root", "trace-1", input_="hello", output="world")),
            {},
        )
    )

    assert len(parsed) == 1
    assert isinstance(parsed[0], ParsedSession)
    assert parsed[0].external_id == "project-1:conversation-1"


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
            {
                "source_trace_id": "trace-1",
                "inputs": {"message": "hello"},
                "outputs": {"answer": "hi"},
            },
            {
                "source_trace_id": "trace-2",
                "inputs": {"message": "again"},
                "outputs": {"answer": "done"},
            },
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
        "environment": "production",
        "name": "support turn",
        "release": "release-1",
        "tags": ["support"],
        "version": "version-1",
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
    assert batch.sessions[0].source_metadata["langfuse.environments"] == ["production"]
    assert batch.sessions[0].source_metadata["langfuse.releases"] == ["release-1"]
    assert batch.sessions[0].source_metadata["langfuse.tags"] == ["support"]
    assert batch.sessions[0].source_metadata["langfuse.versions"] == ["version-1"]


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


def test_maps_openai_agents_function_span_as_tool() -> None:
    """Recognize the function span shape emitted by OpenAI Agents."""
    batch = LangfuseJSONLImporter().parse(
        jsonl(
            observation(
                "root",
                "trace-1",
                input_={"message": "weather"},
                output={"answer": "sunny"},
            ),
            observation(
                "function",
                "trace-1",
                parent_id="root",
                observation_type="SPAN",
                input_={"city": "Delft"},
                output="18 C",
                name="Function: get_weather",
                metadata={
                    "attributes": {
                        "name": "get_weather",
                        "gen_ai.system": "openai",
                    },
                    "response": {"encrypted_content": "do-not-retain"},
                    "resourceAttributes": {
                        "host.name": "private-host",
                        "service.name": "weather-agent",
                    },
                },
            ),
        ),
        context(),
    )

    nodes = {node.source_id: node for node in batch.sessions[0].nodes}
    tool = nodes["trace-1:function"]
    assert tool.node_type is NodeType.TOOL_CALL
    assert tool.tool_name == "get_weather"
    assert tool.provider == "openai"
    assert tool.source_metadata == {
        "langfuse.gen_ai.system": "openai",
        "langfuse.name": "get_weather",
        "langfuse.service.name": "weather-agent",
    }


def test_maps_model_provider_cost_and_bounded_metadata() -> None:
    """Map model evidence without retaining raw provider payloads."""
    batch = LangfuseJSONLImporter().parse(
        jsonl(
            observation(
                "generation",
                "trace-1",
                observation_type="GENERATION",
                input_={"messages": ["hello"]},
                output={"text": "hi"},
                model="requested-model",
                modelId="resolved-model",
                costDetails={"total": 0.0125},
                metadata={
                    "attributes": {
                        "gen_ai.provider.name": "openai",
                        "gen_ai.request.model": "requested-model",
                        "gen_ai.response.model": "resolved-model",
                    },
                    "response": {"encrypted_content": "do-not-retain"},
                },
            )
        ),
        context(),
    )

    node = batch.sessions[0].nodes[0]
    assert node.requested_model == "requested-model"
    assert node.model == "resolved-model"
    assert node.provider == "openai"
    assert node.cost == Decimal("0.0125")
    assert "response" not in node.source_metadata
    assert node.source_metadata["langfuse.gen_ai.provider.name"] == "openai"


def test_uses_model_when_model_id_is_null() -> None:
    """Use the exported model name when modelId is present but null."""
    batch = LangfuseJSONLImporter().parse(
        jsonl(
            observation(
                "generation",
                "trace-1",
                observation_type="GENERATION",
                input_={"messages": ["hello"]},
                output={"text": "hi"},
                model="fixture-model",
                modelId=None,
            )
        ),
        context(),
    )

    node = batch.sessions[0].nodes[0]
    assert node.requested_model == "fixture-model"
    assert node.model == "fixture-model"


def test_recovered_tool_failure_does_not_fail_session() -> None:
    """Use the root observation outcome after a retry succeeds."""
    session = (
        LangfuseJSONLImporter()
        .parse(
            jsonl(
                observation(
                    "root",
                    "trace-1",
                    input_={"message": "invoice"},
                    output={"answer": "recovered"},
                ),
                observation(
                    "tool",
                    "trace-1",
                    parent_id="root",
                    observation_type="TOOL",
                    input_={"invoice": "one"},
                    output=None,
                    level="ERROR",
                    statusMessage="temporary failure",
                ),
            ),
            context(),
        )
        .sessions[0]
    )

    assert session.status is SessionStatus.COMPLETED
    assert session.error is None
    assert session.readiness.level == "partial"
