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
"""Langfuse JSONL importer plugin tests."""

import json
from decimal import Decimal
from typing import Any

import pytest

import kitaru_langfuse_importer.importer as langfuse_module
from kitaru.api_models.v1.session import SessionStatus
from kitaru.api_models.v1.session_node import NodeStatus, NodeType
from kitaru.task.importer import (
    ImportedNode,
    ImportedSession,
    ImportFailure,
)
from kitaru.task.importer import (
    flatten_nodes as flatten_imported_nodes,
)
from kitaru_langfuse_importer.importer import (
    InvalidImport,
    LangfuseJSONLImporter,
    parse,
)


def jsonl(*records: dict[str, Any]) -> bytes:
    """Encode records as JSONL."""
    return b"\n".join(json.dumps(record).encode() for record in records)


def params(source_instance: str | None = None) -> dict[str, Any]:
    """Build importer parameters."""
    return {"source_instance": source_instance} if source_instance else {}


def sessions(
    content: bytes, importer_params: dict[str, Any] | None = None
) -> list[ImportedSession]:
    """Return successfully imported sessions."""
    return [
        item
        for item in LangfuseJSONLImporter().parse(content, importer_params or {})
        if isinstance(item, ImportedSession)
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


def flatten(nodes: list[ImportedNode]) -> list[ImportedNode]:
    """Flatten imported nodes depth-first for assertions."""
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
    assert isinstance(parsed[0], ImportedSession)
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

    assert isinstance(parsed[0], ImportedSession)
    assert parsed[0].nodes[0].trace_id == "trace-1"


def test_selects_text_inside_json_tool_call_arguments() -> None:
    """Expose structured model answers without nested JSON expansion."""
    output = [
        {
            "role": "assistant",
            "parts": [
                {"type": "thinking", "content": ""},
                {"type": "redacted_thinking", "data": "encrypted"},
                {
                    "type": "tool_call",
                    "name": "final_result",
                    "arguments": json.dumps(
                        {
                            "action": "refund",
                            "customer_reply": "Your refund has been issued.",
                        }
                    ),
                },
            ],
        }
    ]

    session = sessions(
        jsonl(
            observation(
                "generation",
                "trace-1",
                observation_type="GENERATION",
                output=output,
            )
        )
    )[0]
    node = session.nodes[0]

    assert node.outputs[0]["parts"][2]["arguments"] == {
        "action": "refund",
        "customer_reply": "Your refund has been issued.",
    }
    assert node.output_text_selector == "/0/parts/2/arguments/customer_reply"


def test_keeps_visible_assistant_text_ahead_of_tool_arguments() -> None:
    """Prefer ordinary model text when a response also requests a tool."""
    output = [
        {
            "role": "assistant",
            "parts": [
                {"type": "text", "content": "I will check that now."},
                {
                    "type": "tool_call",
                    "name": "lookup_order",
                    "arguments": json.dumps({"message": "Lookup order 42"}),
                },
            ],
        }
    ]

    session = sessions(
        jsonl(
            observation(
                "generation",
                "trace-1",
                observation_type="GENERATION",
                output=output,
            )
        )
    )[0]

    assert session.nodes[0].output_text_selector == "/0/parts/0/content"


def test_does_not_guess_between_structured_tool_outputs() -> None:
    """Leave the selector empty when multiple tool outputs contain text."""
    output = [
        {
            "role": "assistant",
            "parts": [
                {
                    "type": "tool_call",
                    "name": "first_result",
                    "arguments": json.dumps({"answer": "First"}),
                },
                {
                    "type": "tool_call",
                    "name": "second_result",
                    "arguments": json.dumps({"answer": "Second"}),
                },
            ],
        }
    ]

    session = sessions(
        jsonl(
            observation(
                "generation",
                "trace-1",
                observation_type="GENERATION",
                output=output,
            )
        )
    )[0]

    assert session.nodes[0].output_text_selector is None


def test_preserves_plain_tool_call_arguments() -> None:
    """Keep non-JSON arguments usable as a direct model output."""
    output = [
        {
            "role": "assistant",
            "parts": [
                {
                    "type": "tool_call",
                    "name": "final_result",
                    "arguments": "The refund has been issued.",
                }
            ],
        }
    ]

    session = sessions(
        jsonl(
            observation(
                "generation",
                "trace-1",
                observation_type="GENERATION",
                output=output,
            )
        )
    )[0]
    node = session.nodes[0]

    assert node.outputs[0]["parts"][0]["arguments"] == ("The refund has been issued.")
    assert node.output_text_selector == "/0/parts/0/arguments"


def test_nests_tool_call_under_requesting_model_by_default() -> None:
    """Nest a tool under its requesting model while retaining source parentage."""
    content = jsonl(
        observation("root", "trace-1", input_={"message": "Weather?"}),
        observation(
            "generation",
            "trace-1",
            parent_id="root",
            observation_type="GENERATION",
            output=[
                {
                    "role": "assistant",
                    "parts": [
                        {
                            "type": "tool_call",
                            "id": "call-weather",
                            "name": "get_weather",
                        }
                    ],
                }
            ],
            start_time="2026-07-24T10:00:00.100000Z",
        ),
        observation(
            "tool",
            "trace-1",
            parent_id="root",
            observation_type="TOOL",
            input_={"city": "Delft"},
            output={"temperature": 18},
            start_time="2026-07-24T10:00:00.200000Z",
            metadata={
                "attributes": {
                    "gen_ai.tool.call.id": "call-weather",
                    "gen_ai.tool.name": "get_weather",
                }
            },
        ),
    )

    [session] = sessions(content)
    nodes = flatten_imported_nodes(session.nodes)
    tool = next(node for node in nodes if node.external_id == "trace-1:tool")

    assert tool.parent_index == 1
    assert tool.secondary_parent_indexes == [0]
    assert session.metadata["langfuse.inferred_tool_call_link_count"] == 1


def test_tool_call_link_inference_can_be_disabled() -> None:
    """Keep provider parentage unchanged when inference is disabled."""
    content = jsonl(
        observation("root", "trace-1"),
        observation(
            "generation",
            "trace-1",
            parent_id="root",
            observation_type="GENERATION",
            output={"tool_calls": [{"id": "call-weather"}]},
        ),
        observation(
            "tool",
            "trace-1",
            parent_id="root",
            observation_type="TOOL",
            metadata={"attributes": {"gen_ai.tool.call.id": "call-weather"}},
        ),
    )

    [session] = sessions(content, {"infer_tool_call_links": False})
    tool = next(
        node
        for node in flatten_imported_nodes(session.nodes)
        if node.external_id == "trace-1:tool"
    )

    assert tool.parent_index == 0
    assert tool.secondary_parent_indexes == []
    assert "langfuse.inferred_tool_call_link_count" not in session.metadata


def test_tool_call_link_inference_skips_ambiguous_ids() -> None:
    """Leave a tool unchanged when several model outputs reuse its call id."""
    content = jsonl(
        observation("root", "trace-1"),
        observation(
            "generation-1",
            "trace-1",
            parent_id="root",
            observation_type="GENERATION",
            output={"tool_calls": [{"id": "reused-call"}]},
            start_time="2026-07-24T10:00:00.100000Z",
        ),
        observation(
            "generation-2",
            "trace-1",
            parent_id="root",
            observation_type="GENERATION",
            output={"tool_calls": [{"id": "reused-call"}]},
            start_time="2026-07-24T10:00:00.200000Z",
        ),
        observation(
            "tool",
            "trace-1",
            parent_id="root",
            observation_type="TOOL",
            start_time="2026-07-24T10:00:00.300000Z",
            metadata={"attributes": {"gen_ai.tool.call.id": "reused-call"}},
        ),
    )

    [session] = sessions(content, {"infer_tool_call_links": True})
    tool = next(
        node
        for node in flatten_imported_nodes(session.nodes)
        if node.external_id == "trace-1:tool"
    )

    assert tool.parent_index == 0
    assert tool.secondary_parent_indexes == []
    assert session.metadata["langfuse.inferred_tool_call_link_count"] == 0


def test_rejects_non_boolean_tool_call_link_option() -> None:
    """Reject string values that could enable inference by accident."""
    with pytest.raises(InvalidImport, match="infer_tool_call_links must be a boolean"):
        LangfuseJSONLImporter().parse(
            jsonl(observation("root", "trace-1")),
            {"infer_tool_call_links": "true"},
        )


def test_redacted_session_ids_fall_back_to_trace_ids() -> None:
    """Do not merge unrelated traces that share a redaction placeholder."""
    parsed = LangfuseJSONLImporter().parse(
        jsonl(
            observation("root-1", "trace-1", session_id="[Scrubbed due to 'session']"),
            observation("root-2", "trace-2", session_id="[Scrubbed due to 'session']"),
        ),
        {},
    )

    imported = [item for item in parsed if isinstance(item, ImportedSession)]
    assert [session.external_id for session in imported] == [
        "project-1:trace-1",
        "project-1:trace-2",
    ]


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
    assert isinstance(session, ImportedSession)
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


def test_trace_without_session_id_becomes_one_turn_session() -> None:
    """Use a trace id when Langfuse has no session id."""
    parsed = sessions(
        jsonl(observation("root", "trace-1", session_id=None, input_="hello")),
    )

    assert parsed[0].metadata["langfuse.session_id"] == "trace-1"
    assert parsed[0].metadata["source_trace_count"] == 1


def test_joins_traces_by_metadata_key_and_json_pointer() -> None:
    """Join turns using a user-selected key inside a JSON position."""
    parsed = LangfuseJSONLImporter().parse(
        jsonl(
            observation(
                "root-1",
                "trace-1",
                session_id=None,
                input_="first",
                metadata={"customer": {"case_id": "case-42"}},
            ),
            observation(
                "root-2",
                "trace-2",
                session_id=None,
                input_="second",
                start_time="2026-07-24T10:01:00Z",
                metadata={"customer": {"case_id": "case-42"}},
            ),
        ),
        {"join_path": "/metadata/customer", "join_key": "case_id"},
    )

    assert len(parsed) == 1
    session = parsed[0]
    assert isinstance(session, ImportedSession)
    assert session.external_id == "project-1:case-42"
    assert session.metadata["source_trace_count"] == 2
    assert session.metadata["langfuse.join_paths"] == ["/metadata/customer/case_id"]
    assert [turn["source_trace_id"] for turn in session.inputs["turns"]] == [
        "trace-1",
        "trace-2",
    ]


def test_isolates_trace_missing_selected_join_value() -> None:
    """Report a missing explicit join value without fragmenting the session."""
    parsed = LangfuseJSONLImporter().parse(
        jsonl(
            observation(
                "root-1",
                "trace-1",
                session_id=None,
                metadata={"case_id": "case-42"},
            ),
            observation("root-2", "trace-2", session_id=None, metadata={}),
        ),
        {"join_on": "/metadata/case_id"},
    )

    assert len(parsed) == 2
    assert isinstance(parsed[0], ImportedSession)
    assert isinstance(parsed[1], ImportFailure)
    assert parsed[1].external_id == "trace-2"
    assert "join selector" in parsed[1].error


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


def test_surfaces_flattened_model_metadata_and_terminal_output() -> None:
    """Prefer public model fields and use the last node as session output."""
    session = sessions(
        jsonl(
            observation("root", "trace-1", input_={"message": "hello"}),
            observation(
                "generation",
                "trace-1",
                parent_id="root",
                observation_type="GENERATION",
                input_={"messages": [{"role": "user", "content": "hello"}]},
                output={"role": "assistant", "content": "hi"},
                model="resolved-model",
                modelId="internal-model-id",
                metadata={
                    "attributes.gen_ai.request.model": "requested-model",
                    "attributes.gen_ai.provider.name": "openai",
                },
            ),
        )
    )[0]
    generation = next(
        node for node in flatten(session.nodes) if node.node_type is NodeType.LLM_CALL
    )

    assert session.outputs == {"role": "assistant", "content": "hi"}
    assert generation.requested_model == "requested-model"
    assert generation.model == "resolved-model"
    assert generation.model_provider == "openai"


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


def test_imports_out_of_order_legacy_ingestion_updates() -> None:
    """Resolve an update when its create event appears later in the upload."""
    parsed = sessions(
        jsonl(
            {
                "type": "span-update",
                "body": {
                    "id": "root",
                    "endTime": "2026-07-24T10:00:01Z",
                    "output": {"answer": "hi"},
                },
            },
            {
                "type": "trace-create",
                "body": {
                    "id": "trace-1",
                    "sessionId": "conversation-1",
                    "projectId": "project-1",
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
        )
    )

    assert parsed[0].nodes[0].outputs == {"answer": "hi"}


def test_preserves_explicit_zero_token_usage() -> None:
    """Prefer an explicit zero over a later alias with a nonzero value."""
    session = sessions(
        jsonl(
            observation(
                "generation",
                "trace-1",
                observation_type="GENERATION",
                usageDetails={
                    "input": 0,
                    "input_tokens": 7,
                    "output": 0,
                    "output_tokens": 5,
                    "input_cached_tokens": 0,
                    "cache_read_tokens": 3,
                },
            )
        )
    )[0]

    assert session.nodes[0].tokens is not None
    assert session.nodes[0].tokens.input_tokens == 0
    assert session.nodes[0].tokens.output_tokens == 0
    assert session.nodes[0].tokens.cached_input_tokens == 0


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


def test_node_order_is_stable_across_upload_order() -> None:
    """Ignore JSONL row order when ordering nodes."""
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

    assert isinstance(forward, ImportedSession)
    assert isinstance(reversed_, ImportedSession)
    assert [node.external_id for node in forward.nodes] == [
        node.external_id for node in reversed_.nodes
    ]


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
    assert tool.model_provider == "openai"
    assert tool.metadata == {
        "langfuse.gen_ai.system": "openai",
        "langfuse.name": "get_weather",
        "langfuse.service.name": "weather-agent",
    }


def test_maps_flattened_openai_agents_function_span_as_tool() -> None:
    """Recognize function names from flattened Langfuse metadata."""
    session = sessions(
        jsonl(
            observation("root", "trace-1", input_={"message": "weather"}),
            observation(
                "function",
                "trace-1",
                parent_id="root",
                observation_type="SPAN",
                input_={"city": "Delft"},
                output="18 C",
                name="Function: get_weather",
                metadata={
                    "name": "unrelated-metadata-name",
                    "attributes.name": "get_weather",
                    "attributes.gen_ai.system": "openai",
                },
            ),
        )
    )[0]

    nodes = {node.external_id: node for node in flatten(session.nodes)}
    tool = nodes["trace-1:function"]
    assert tool.node_type is NodeType.TOOL_CALL
    assert tool.tool_name == "get_weather"
    assert tool.model_provider == "openai"


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
    assert node.model_provider == "openai"
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
