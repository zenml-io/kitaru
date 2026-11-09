# Copyright (c) ZenML GmbH 2026. All Rights Reserved.
"""Imported model requests retain their instructions and offered tools."""

import json
from typing import Any, Literal
from uuid import uuid4

import pytest

from kitaru.api_models.v1.session_node import (
    NodeType,
    SessionNodeCreateRequest,
    SessionNodeResponse,
)
from kitaru.task.importer import ImportedSession, flatten_nodes
from kitaru_langfuse_importer.importer import LangfuseJSONLImporter
from kitaru_logfire_importer.importer import LogfireRecordsImporter
from kitaru_model_swap.endpoint import build_body, build_responses_body
from kitaru_model_swap.normalize import ReplayRequest, Skipped, build_replay_request
from kitaru_model_swap.params import Candidate
from kitaru_phoenix_importer.importer import PhoenixTraceImporter

MESSAGES = [{"role": "user", "content": "Classify the claim."}]
ANSWER = [{"role": "assistant", "content": "Denied"}]
FUNCTION = {
    "type": "function",
    "function": {
        "name": "classify",
        "strict": True,
        "parameters": {
            "type": "object",
            "properties": {"decision": {"enum": ["approved", "denied"]}},
            "required": ["decision"],
            "additionalProperties": False,
        },
    },
}


Provider = Literal["langfuse", "logfire", "phoenix"]


def import_node(
    provider: Provider, attributes: dict[str, Any]
) -> SessionNodeCreateRequest:
    """Import one provider's model-call export through its public parser."""
    start = "2026-07-24T10:00:00Z"
    if provider == "langfuse":
        payload = {
            "id": "generation",
            "traceId": "trace-1",
            "type": "GENERATION",
            "name": "chat",
            "input": MESSAGES,
            "output": ANSWER,
            "startTime": start,
            "metadata": {"attributes": attributes},
        }
        imported = LangfuseJSONLImporter().parse(
            json.dumps(payload).encode(), {"source_instance": "test"}
        )
    elif provider == "logfire":
        payload = {
            "project_id": "project-1",
            "trace_id": "trace-1",
            "span_id": "generation",
            "span_name": "chat same-model",
            "start_timestamp": start,
            "attributes": {"gen_ai.operation.name": "chat", **attributes},
        }
        imported = LogfireRecordsImporter().parse(json.dumps(payload).encode(), {})
    else:
        payload = {
            "id": "generation",
            "context": {"trace_id": "trace-1", "span_id": "generation"},
            "name": "chat",
            "span_kind": "LLM",
            "start_time": start,
            "attributes": attributes,
        }
        imported = PhoenixTraceImporter().parse(
            json.dumps(payload).encode(), {"project": "test"}
        )
    [session] = imported
    assert isinstance(session, ImportedSession)
    return next(
        node
        for node in flatten_nodes(session.nodes)
        if node.node_type is NodeType.LLM_CALL
    )


def rebuild(imported: SessionNodeCreateRequest) -> ReplayRequest | Skipped:
    """Rebuild an imported node through the persisted server DTO."""
    node = SessionNodeResponse.model_validate(
        {**imported.model_dump(), "id": uuid4(), "session_id": uuid4()}
    )
    return build_replay_request(
        node,
        tool_schemas={},
        tool_descriptions={},
        use_recorded_tools=True,
        infer_missing_schemas=False,
    )


def build_candidate(api: str) -> Candidate:
    """Configure the request shape without contacting a provider."""
    return Candidate.model_validate(
        {
            "name": "candidate",
            "model": "same-model",
            "base_url": "http://localhost/v1",
            "provider_settings": {"api": api},
        }
    )


@pytest.mark.parametrize("api", ["chat_completions", "responses"])
def test_langfuse_preserves_separate_system_instructions(api: str) -> None:
    policy = "Always deny claims without an invoice."
    request = rebuild(
        import_node(
            "langfuse",
            {
                "gen_ai.system_instructions": json.dumps(
                    [{"type": "text", "content": policy}]
                )
            },
        )
    )
    assert isinstance(request, ReplayRequest)
    configured = build_candidate(api)
    if api == "responses":
        body = build_responses_body(
            configured, request.messages, request.tools, request.controls
        )
        assert body["instructions"] == policy
    else:
        body = build_body(configured, request.messages, request.tools, request.controls)
        assert body["messages"][0] == {"role": "system", "content": policy}


@pytest.mark.parametrize("provider", ["langfuse", "logfire", "phoenix"])
def test_imported_strict_tools_remain_strict(
    provider: Provider,
) -> None:
    request = rebuild(
        import_node(
            provider,
            {
                "gen_ai.request.model": "same-model",
                "gen_ai.input.messages": MESSAGES,
                "gen_ai.output.messages": ANSWER,
                "gen_ai.tool.definitions": json.dumps([FUNCTION]),
            },
        )
    )
    assert isinstance(request, ReplayRequest)
    body = build_responses_body(
        build_candidate("responses"), request.messages, request.tools, request.controls
    )
    assert body["tools"][0]["strict"] is True


@pytest.mark.parametrize("provider", ["langfuse", "logfire", "phoenix"])
def test_imported_no_argument_tools_remain_offered(provider: Provider) -> None:
    request = rebuild(
        import_node(
            provider,
            {
                "gen_ai.request.model": "same-model",
                "gen_ai.input.messages": MESSAGES,
                "gen_ai.output.messages": ANSWER,
                "gen_ai.tool.definitions": [{"type": "function", "name": "approve"}],
            },
        )
    )
    assert isinstance(request, ReplayRequest)
    assert request.tools == [
        {
            "type": "function",
            "function": {
                "name": "approve",
                "parameters": {"type": "object", "properties": {}},
            },
        }
    ]


@pytest.mark.parametrize("provider", ["langfuse", "logfire", "phoenix"])
@pytest.mark.parametrize("with_function", [False, True])
@pytest.mark.parametrize(
    "unsupported",
    [
        {"type": "web_search_preview"},
        {"type": "custom", "name": "freeform", "format": {"type": "text"}},
    ],
    ids=["web_search", "custom"],
)
def test_unsupported_offered_tools_skip_the_whole_replay(
    provider: Provider, with_function: bool, unsupported: dict[str, Any]
) -> None:
    offered = ([FUNCTION] if with_function else []) + [unsupported]
    request = rebuild(
        import_node(
            provider,
            {
                "gen_ai.request.model": "same-model",
                "gen_ai.input.messages": MESSAGES,
                "gen_ai.output.messages": ANSWER,
                "gen_ai.tool.definitions": json.dumps(offered),
            },
        )
    )
    assert isinstance(request, Skipped)
    assert request.reason == "unsupported_tool"
