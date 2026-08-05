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

import importers.langfuse as langfuse_module
from importers.langfuse import InvalidImport, LangfuseJSONLImporter, parse
from kitaru.api_models.v1.session import SessionStatus
from kitaru.api_models.v1.session_node import NodeStatus, NodeType
from kitaru.task.importer import ImportFailure, ParsedNode, ParsedSession


def jsonl(*records: dict[str, Any]) -> bytes:
    """Encode records as JSONL."""
    return b"\n".join(json.dumps(record).encode() for record in records)


def params(source_instance: str | None = None) -> dict[str, Any]:
    """Build importer parameters."""
    return {"source_instance": source_instance} if source_instance else {}


def sessions(
    content: bytes, importer_params: dict[str, Any] | None = None
) -> list[ParsedSession]:
    """Return successfully parsed sessions."""
    return [
        item
        for item in LangfuseJSONLImporter().parse(content, importer_params or {})
        if isinstance(item, ParsedSession)
    ]


def failures(
    content: bytes, importer_params: dict[str, Any] | None = None
) -> list[ImportFailure]:
    """Return isolated import failures."""
    return [
        item
        for item in LangfuseJSONLImporter().parse(content, importer_params or {})
        if isinstance(item, ImportFailure)
    ]


def flatten(nodes: list[ParsedNode]) -> list[ParsedNode]:
    """Flatten parsed nodes depth-first for assertions."""
    return [node for root in nodes for node in (root, *flatten(root.children))]


def test_rejects_oversized_upload(monkeypatch: pytest.MonkeyPatch) -> None:
    """Reject content before decoding when it exceeds the importer limit."""
    monkeypatch.setattr(langfuse_module, "MAX_UPLOAD_BYTES", 3)

    with pytest.raises(InvalidImport, match="50 MiB upload limit"):
        LangfuseJSONLImporter().parse(b"1234", {})


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


def test_unified_parse_preserves_node_trace_id() -> None:
    """Keep provider trace identity through the public parser contract."""
    parsed = list(
        parse(
            jsonl(observation("root", "trace-1", input_="hello", output="world")),
            {},
        )
    )

    assert isinstance(parsed[0], ParsedSession)
    assert parsed[0].nodes[0].trace_id == "trace-1"


def test_imports_pretty_printed_json_array() -> None:
    """Accept the JSON array format advertised by the importer."""
    content = json.dumps(
        [observation("root", "trace-1", input_="hello", output="world")],
        indent=2,
    ).encode()

    parsed = sessions(content)

    assert len(parsed) == 1
    assert parsed[0].metadata["langfuse.session_id"] == "conversation-1"


def test_unified_parse_rejects_parent_cycle() -> None:
    """Report a cyclic provider graph instead of importing an empty node tree."""
    parsed = list(
        parse(
            jsonl(
                observation("first", "trace-1", parent_id="second", input_="hello"),
                observation("second", "trace-1", parent_id="first"),
            ),
            {},
        )
    )

    assert len(parsed) == 1
    assert isinstance(parsed[0], ImportFailure)
    assert "parent cycle" in parsed[0].error


def test_imports_multiturn_observations() -> None:
    """Group source traces into a multi-turn Kitaru session."""
    parsed = LangfuseJSONLImporter().parse(
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
        {},
    )

    assert len(parsed) == 1
    session = parsed[0]
    assert isinstance(session, ParsedSession)
    assert session.external_id == "project-1:conversation-1"
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
    assert session.metadata["langfuse.trace_ids"] == ["trace-1", "trace-2"]
    nodes = {node.external_id: node for node in flatten(session.nodes)}
    assert nodes["trace-1:root-1"].node_type is NodeType.SPAN
    assert nodes["trace-1:generation-1"].node_type is NodeType.LLM_CALL
    assert nodes["trace-1:generation-1"] in nodes["trace-1:root-1"].children
    assert nodes["trace-2:root-2"].node_type is NodeType.SPAN
    assert nodes["trace-2:tool-1"].node_type is NodeType.TOOL_CALL
    assert nodes["trace-2:tool-1"].tool_name == "weather"
    readiness = session.metadata["replay_readiness"]
    assert isinstance(readiness, dict)
    assert readiness["level"] == "ready"
    assert readiness["tool_call_count"] == 1
    assert readiness["replayable_tool_call_count"] == 1


def test_trace_without_session_id_becomes_one_turn_session() -> None:
    """Use a trace id when Langfuse has no session id."""
    parsed = sessions(
        jsonl(observation("root", "trace-1", session_id=None, input_="hello")),
    )

    assert parsed[0].metadata["langfuse.session_id"] == "trace-1"
    assert parsed[0].metadata["source_trace_count"] == 1


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

    session = sessions(jsonl(trace))[0]

    assert session.name == "support turn"
    assert session.inputs["turns"][0]["inputs"] == {"message": "hello"}
    assert session.outputs == {"answer": "hi"}
    assert session.metadata["langfuse.environments"] == ["production"]
    assert session.metadata["langfuse.releases"] == ["release-1"]
    assert session.metadata["langfuse.versions"] == ["version-1"]


def test_imports_legacy_ingestion_events() -> None:
    """Detect ingestion-event JSONL and merge create and update events."""
    parsed = sessions(
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
        )
    )

    assert parsed[0].nodes[0].outputs == {"answer": "hi"}


def test_reports_one_invalid_group_without_losing_valid_sessions() -> None:
    """Isolate semantic errors by grouped source session."""
    content = jsonl(
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
    )
    parsed_sessions = sessions(content)
    parsed_failures = failures(content)

    assert [session.metadata["langfuse.session_id"] for session in parsed_sessions] == [
        "valid-session"
    ]
    assert len(parsed_failures) == 1
    assert parsed_failures[0].external_id == "invalid-session"
    assert "provide source_instance" in parsed_failures[0].error


def test_source_instance_selection_handles_exports_without_project_ids() -> None:
    """Apply the explicit source instance when the export omits a project id."""
    parsed = sessions(
        jsonl(observation("root", "trace-1", project_id=None, input_="hello")),
        params(source_instance="selected-project"),
    )

    assert parsed[0].external_id == "selected-project:conversation-1"


def test_status_message_does_not_imply_failure() -> None:
    """Treat a status message as diagnostic unless its level failed."""
    parsed = sessions(
        jsonl(
            observation(
                "root",
                "trace-1",
                input_="hello",
                level="WARNING",
                statusMessage="model retried once",
            )
        )
    )

    assert parsed[0].status is SessionStatus.COMPLETED
    assert parsed[0].nodes[0].status is NodeStatus.COMPLETED


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

    forward = importer.parse(jsonl(first, child), {})[0]
    reversed_ = importer.parse(jsonl(child, first), {})[0]

    assert isinstance(forward, ParsedSession)
    assert isinstance(reversed_, ParsedSession)
    assert (
        forward.metadata["source_content_digest"]
        == reversed_.metadata["source_content_digest"]
    )


def test_rejects_mixed_row_shapes() -> None:
    """Reject files that mix incompatible export representations."""
    nested_trace = {"id": "trace-1", "observations": []}
    enriched = observation("root", "trace-2")

    with pytest.raises(InvalidImport, match="mixes multiple"):
        LangfuseJSONLImporter().parse(jsonl(nested_trace, enriched), {})


def test_maps_openai_agents_function_span_as_tool() -> None:
    """Recognize the function span shape emitted by OpenAI Agents."""
    session = sessions(
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
        )
    )[0]

    nodes = {node.external_id: node for node in flatten(session.nodes)}
    tool = nodes["trace-1:function"]
    assert tool.node_type is NodeType.TOOL_CALL
    assert tool.tool_name == "get_weather"
    assert tool.provider == "openai"
    assert tool.metadata == {
        "langfuse.gen_ai.system": "openai",
        "langfuse.name": "get_weather",
        "langfuse.service.name": "weather-agent",
    }


def test_maps_model_provider_cost_and_bounded_metadata() -> None:
    """Map model evidence without retaining raw provider payloads."""
    session = sessions(
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
        )
    )[0]

    node = session.nodes[0]
    assert node.requested_model == "requested-model"
    assert node.model == "resolved-model"
    assert node.provider == "openai"
    assert node.cost == Decimal("0.0125")
    assert "response" not in node.metadata
    assert node.metadata["langfuse.gen_ai.provider.name"] == "openai"


def test_uses_model_when_model_id_is_null() -> None:
    """Use the exported model name when modelId is present but null."""
    session = sessions(
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
        )
    )[0]

    node = session.nodes[0]
    assert node.requested_model == "fixture-model"
    assert node.model == "fixture-model"


def test_recovered_tool_failure_does_not_fail_session() -> None:
    """Use the root observation outcome after a retry succeeds."""
    session = sessions(
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
        )
    )[0]

    assert session.status is SessionStatus.COMPLETED
    assert session.error is None
    readiness = session.metadata["replay_readiness"]
    assert isinstance(readiness, dict)
    assert readiness["level"] == "partial"
