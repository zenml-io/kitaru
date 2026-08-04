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
"""Braintrust importer tests based on a sanitized project-log export."""

import json
from decimal import Decimal
from typing import Any

from kitaru.importers import ImportContext, NodeType, SessionStatus
from kitaru.importers.braintrust import BraintrustProjectLogImporter, parse
from kitaru.task.importer import ImportFailure, ParsedSession


def context(
    source_instance: str | None = None,
    filename: str = "litellm.json",
) -> ImportContext:
    """Build one importer context."""
    return ImportContext(source_instance=source_instance, filename=filename)


def event(
    *,
    event_id: str,
    span_id: str,
    root_span_id: str,
    name: str,
    span_type: str,
    parents: list[str],
    input_: Any,
    output: Any,
    start: float,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build one Braintrust project-log event."""
    return {
        "id": event_id,
        "project_id": "project-1",
        "span_id": span_id,
        "root_span_id": root_span_id,
        "span_parents": parents,
        "span_attributes": {"name": name, "type": span_type},
        "input": input_,
        "output": output,
        "metadata": {
            "session_id": "conversation-1",
            **(metadata or {}),
        },
        "metrics": {"start": start, "end": start + 0.1},
        "created": "2026-07-24T10:00:00Z",
    }


def test_imports_full_project_log_with_hierarchy() -> None:
    """Preserve Braintrust trace identity, node types, and parent links."""
    root = event(
        event_id="event-root",
        span_id="root",
        root_span_id="root",
        name="weather-agent",
        span_type="task",
        parents=[],
        input_={"messages": [{"role": "user", "content": "Weather?"}]},
        output={"role": "assistant", "content": "Sunny."},
        start=1_785_000_000.0,
    )
    llm = event(
        event_id="event-llm",
        span_id="llm",
        root_span_id="root",
        name="weather-model",
        span_type="llm",
        parents=["root"],
        input_={"messages": [{"role": "user", "content": "Weather?"}]},
        output={"tool_calls": [{"name": "get_weather"}]},
        start=1_785_000_000.1,
        metadata={"model": "fixture-model"},
    )
    tool = event(
        event_id="event-tool",
        span_id="tool",
        root_span_id="root",
        name="get_weather",
        span_type="tool",
        parents=["root", "llm"],
        input_={"city": "Berlin"},
        output={"condition": "sunny"},
        start=1_785_000_000.2,
    )

    batch = BraintrustProjectLogImporter().parse(
        json.dumps({"events": [tool, llm, root]}).encode(),
        context(),
    )

    assert batch.errors == []
    assert len(batch.sessions) == 1
    session = batch.sessions[0]
    assert session.source_id == "conversation-1"
    assert session.source_instance == "project-1"
    assert session.readiness.level == "ready"
    assert session.readiness.tool_call_count == 1
    nodes = {node.source_id: node for node in session.nodes}
    assert nodes["root:llm"].node_type is NodeType.LLM_CALL
    assert nodes["root:llm"].parent_source_id == "root:root"
    assert nodes["root:tool"].node_type is NodeType.TOOL_CALL
    assert nodes["root:tool"].parent_source_id == "root:llm"
    assert session.inputs["turns"][0]["outputs"] == {
        "role": "assistant",
        "content": "Sunny.",
    }


def test_unified_parse_returns_prefixed_external_id() -> None:
    """Expose normalized sessions through the unified plugin entrypoint."""
    root = event(
        event_id="event-root",
        span_id="root",
        root_span_id="root",
        name="assistant",
        span_type="task",
        parents=[],
        input_={"role": "user", "content": "Hello"},
        output={"role": "assistant", "content": "Hi"},
        start=1_785_000_000.0,
    )

    parsed = list(parse(json.dumps({"events": [root]}).encode(), {}))

    assert len(parsed) == 1
    assert isinstance(parsed[0], ParsedSession)
    assert parsed[0].external_id == "project-1:conversation-1"


def test_unified_parse_isolates_invalid_token_metrics() -> None:
    """Report malformed token metrics without aborting valid sessions."""
    invalid = event(
        event_id="event-invalid",
        span_id="invalid",
        root_span_id="invalid",
        name="invalid",
        span_type="llm",
        parents=[],
        input_="bad",
        output="bad",
        start=1_785_000_000.0,
        metadata={"session_id": "invalid-session"},
    )
    invalid["metrics"]["prompt_tokens"] = "not-a-number"
    valid = event(
        event_id="event-valid",
        span_id="valid",
        root_span_id="valid",
        name="valid",
        span_type="task",
        parents=[],
        input_="good",
        output="good",
        start=1_785_000_001.0,
        metadata={"session_id": "valid-session"},
    )

    parsed = list(parse(json.dumps({"events": [invalid, valid]}).encode(), {}))

    assert len(parsed) == 2
    assert isinstance(parsed[0], ParsedSession)
    assert parsed[0].external_id == "project-1:valid-session"
    assert isinstance(parsed[1], ImportFailure)
    assert "prompt_tokens" in parsed[1].error


def test_groups_multiple_root_traces_into_one_session() -> None:
    """Treat root traces sharing metadata.session_id as ordered turns."""
    first = event(
        event_id="event-1",
        span_id="root-1",
        root_span_id="root-1",
        name="turn-1",
        span_type="task",
        parents=[],
        input_="first",
        output="one",
        start=1_785_000_000.0,
    )
    second = event(
        event_id="event-2",
        span_id="root-2",
        root_span_id="root-2",
        name="turn-2",
        span_type="task",
        parents=[],
        input_="second",
        output="two",
        start=1_785_000_001.0,
    )

    batch = BraintrustProjectLogImporter().parse(
        b"\n".join(json.dumps(row).encode() for row in (second, first)),
        context(),
    )

    assert [turn.inputs for turn in batch.sessions[0].turns] == [
        "first",
        "second",
    ]


def test_accepts_flat_ui_export_as_partial() -> None:
    """Accept Braintrust UI JSON with filename-based project identity."""
    rows = [
        {
            "created": "2026-07-24T10:00:00Z",
            "name": "weather-model",
            "input": {"messages": [{"role": "user", "content": "Weather?"}]},
            "output": {"tool_calls": [{"name": "get_weather"}]},
            "metadata": {
                "session_id": "conversation-1",
                "turn_index": 0,
                "model": "fixture-model",
            },
            "metrics": {
                "start": 1_785_000_000.1,
                "end": 1_785_000_000.2,
                "prompt_tokens": 5,
                "completion_tokens": 2,
            },
        },
        {
            "created": "2026-07-24T10:00:00Z",
            "name": "weather-agent",
            "input": {"messages": [{"role": "user", "content": "Weather?"}]},
            "output": {"role": "assistant", "content": "Sunny."},
            "metadata": {
                "session_id": "conversation-1",
                "turn_index": 0,
            },
            "metrics": {
                "start": 1_785_000_000.0,
                "end": 1_785_000_000.4,
            },
        },
    ]

    batch = BraintrustProjectLogImporter().parse(
        json.dumps(rows).encode(),
        context(),
    )

    session = batch.sessions[0]
    assert session.source_instance == "litellm"
    assert session.readiness.level == "partial"
    assert session.readiness.graph_complete is False
    assert "omits span identity" in session.warnings[0]
    assert len(session.turns) == 1


def test_maps_otel_conversation_model_provider_and_cost() -> None:
    """Map the OpenTelemetry fields emitted by the corpus integrations."""
    root = event(
        event_id="event-root",
        span_id="root",
        root_span_id="root",
        name="agent",
        span_type="task",
        parents=[],
        input_={"message": "hello"},
        output={"answer": "hi"},
        start=1_785_000_000.0,
        metadata={"gen_ai.conversation.id": "otel-conversation"},
    )
    llm = event(
        event_id="event-llm",
        span_id="llm",
        root_span_id="root",
        name="model",
        span_type="llm",
        parents=["root"],
        input_={"messages": ["hello"]},
        output={"text": "hi"},
        start=1_785_000_000.1,
        metadata={
            "gen_ai.conversation.id": "otel-conversation",
            "gen_ai.provider.name": "openai",
            "gen_ai.request.model": "requested-model",
            "gen_ai.response.model": "resolved-model",
        },
    )
    root["metadata"].pop("session_id")
    llm["metadata"].pop("session_id")
    llm["metrics"].update(
        {
            "estimated_cost": 0.00125,
            "prompt_tokens": 5,
            "completion_tokens": 2,
        }
    )

    batch = BraintrustProjectLogImporter().parse(
        json.dumps({"events": [llm, root]}).encode(),
        context(),
    )

    session = batch.sessions[0]
    assert session.source_id == "otel-conversation"
    assert session.source_metadata["braintrust.session_id"] == "otel-conversation"
    node = next(node for node in session.nodes if node.node_type is NodeType.LLM_CALL)
    assert node.requested_model == "requested-model"
    assert node.model == "resolved-model"
    assert node.provider == "openai"
    assert node.cost == Decimal("0.00125")
    assert node.tokens is not None
    assert node.tokens.input_tokens == 5


def test_recovered_tool_failure_does_not_fail_session() -> None:
    """Use the root run outcome after a retry succeeds."""
    root = event(
        event_id="event-root",
        span_id="root",
        root_span_id="root",
        name="agent",
        span_type="task",
        parents=[],
        input_={"message": "hello"},
        output={"answer": "recovered"},
        start=1_785_000_000.0,
    )
    tool = event(
        event_id="event-tool",
        span_id="tool",
        root_span_id="root",
        name="lookup",
        span_type="tool",
        parents=["root"],
        input_={"id": "one"},
        output=None,
        start=1_785_000_000.1,
    )
    tool["error"] = "temporary failure"

    session = (
        BraintrustProjectLogImporter()
        .parse(
            json.dumps({"events": [root, tool]}).encode(),
            context(),
        )
        .sessions[0]
    )

    assert session.status is SessionStatus.COMPLETED
    assert session.error is None
    assert session.readiness.level == "partial"


def test_embedded_tool_activity_without_tool_spans_is_partial() -> None:
    """Do not claim readiness when the export omits implied tool spans."""
    root = event(
        event_id="event-root",
        span_id="root",
        root_span_id="root",
        name="agent",
        span_type="task",
        parents=[],
        input_={"message": "weather"},
        output={"answer": "sunny"},
        start=1_785_000_000.0,
    )
    llm = event(
        event_id="event-llm",
        span_id="llm",
        root_span_id="root",
        name="model",
        span_type="llm",
        parents=["root"],
        input_={"messages": [{"role": "user", "content": "weather"}]},
        output={"tool_calls": [{"name": "weather", "arguments": {"city": "Delft"}}]},
        start=1_785_000_000.1,
    )

    session = (
        BraintrustProjectLogImporter()
        .parse(
            json.dumps({"events": [root, llm]}).encode(),
            context(),
        )
        .sessions[0]
    )

    assert session.readiness.level == "partial"
    assert session.readiness.tool_call_count == 0
    assert "no explicit tool spans" in session.warnings[0]


def test_incomplete_llm_span_is_partial() -> None:
    """Do not claim readiness when an LLM output is absent."""
    root = event(
        event_id="event-root",
        span_id="root",
        root_span_id="root",
        name="agent",
        span_type="task",
        parents=[],
        input_={"message": "hello"},
        output={"answer": "hi"},
        start=1_785_000_000.0,
    )
    llm = event(
        event_id="event-llm",
        span_id="llm",
        root_span_id="root",
        name="model",
        span_type="llm",
        parents=["root"],
        input_={"messages": ["hello"]},
        output=None,
        start=1_785_000_000.1,
    )

    session = (
        BraintrustProjectLogImporter()
        .parse(
            json.dumps({"events": [root, llm]}).encode(),
            context(),
        )
        .sessions[0]
    )

    assert session.readiness.level == "partial"
    assert "lack recorded input or output" in session.warnings[0]
