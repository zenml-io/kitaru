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
"""Synthetic ATIF contracts informed by Harbor v1.7 trajectory exports."""

import json
from decimal import Decimal
from typing import Any

import pytest

from kitaru.api_models.v1.imports import ImportFailure
from kitaru.api_models.v1.session import SessionStatus
from kitaru.api_models.v1.session_node import NodeStatus, NodeType
from kitaru.task.importer import ImportedSession, flatten_nodes
from kitaru_atif_importer import parse


def _trajectory() -> dict[str, Any]:
    """Build a small trajectory without private benchmark content."""
    return {
        "schema_version": "ATIF-v1.7",
        "session_id": "session-fixture",
        "agent": {
            "name": "fixture-agent",
            "version": "1.0",
            "model_name": "fixture-model",
            "extra": {"condition": "synthetic"},
        },
        "steps": [
            {"step_id": 1, "source": "system", "message": "Answer briefly."},
            {"step_id": 2, "source": "user", "message": "Read the sample."},
            {
                "step_id": 3,
                "source": "agent",
                "message": "I will read the sample.",
                "reasoning_content": "The sample contains the answer.",
                "model_name": "step-model",
                "llm_call_count": 1,
                "tool_calls": [
                    {
                        "tool_call_id": "call-read",
                        "function_name": "read_file",
                        "arguments": {"path": "sample.txt"},
                        "extra": {"raw_arguments": '{"path":"sample.txt"}'},
                    }
                ],
                "observation": {
                    "results": [
                        {
                            "source_call_id": "call-read",
                            "content": "The sample says hello.",
                            "extra": {"tool_result_is_error": False},
                        }
                    ]
                },
                "metrics": {
                    "prompt_tokens": 20,
                    "completion_tokens": 8,
                    "cached_tokens": 5,
                    "cost_usd": 0.002,
                    "extra": {"reasoning_output_tokens": 3},
                },
                "extra": {"api_call_id": "api-synthetic", "custom": [1, 2]},
            },
            {
                "step_id": 4,
                "source": "agent",
                "message": "Hello.",
                "llm_call_count": 1,
            },
        ],
        "final_metrics": {
            "total_prompt_tokens": 20,
            "total_completion_tokens": 8,
            "total_cached_tokens": 5,
            "total_cost_usd": 0.002,
            "total_steps": 4,
        },
        "extra": {"benchmark": "synthetic"},
    }


def _parse_session(
    value: dict[str, Any], params: dict[str, Any] | None = None
) -> ImportedSession:
    """Require exactly one successfully parsed session."""
    items = list(parse(json.dumps(value).encode(), params or {}))
    assert len(items) == 1
    assert isinstance(items[0], ImportedSession), items[0]
    return items[0]


def test_maps_messages_tools_results_and_reasoning() -> None:
    """Expose readable conversation and link each tool to its generating step."""
    source = _trajectory()
    session = _parse_session(source)
    nodes = flatten_nodes(session.nodes)
    model = next(node for node in nodes if node.reasoning is not None)
    tool = next(node for node in nodes if node.node_type is NodeType.TOOL_CALL)

    assert session.status is SessionStatus.COMPLETED
    assert session.inputs["messages"] == [
        {"source": "system", "step_id": 1, "message": "Answer briefly."},
        {"source": "user", "step_id": 2, "message": "Read the sample."},
    ]
    assert session.outputs["message"] == "Hello."
    assert model.node_type is NodeType.LLM_CALL
    assert model.model == "step-model"
    assert model.reasoning == "The sample contains the answer."
    assert model.outputs["message"] == source["steps"][2]["message"]
    assert model.outputs["tool_calls"] == source["steps"][2]["tool_calls"]
    assert tool.parent_index == model.index
    assert tool.tool_name == "read_file"
    assert tool.inputs == {"path": "sample.txt"}
    assert tool.outputs["results"] == source["steps"][2]["observation"]["results"]
    assert tool.status is NodeStatus.COMPLETED


def test_preserves_source_extras_and_token_ids_without_double_counting() -> None:
    """Keep source-only metrics while counting usage on the measured step once."""
    source = _trajectory()
    source["steps"][2]["metrics"]["prompt_token_ids"] = [10, 20]
    source["steps"][2]["metrics"]["completion_token_ids"] = [30, 40]
    source["steps"][2]["metrics"]["extra"]["cache_creation_input_tokens"] = 2
    session = _parse_session(source)
    nodes = flatten_nodes(session.nodes)
    measured = [node for node in nodes if node.tokens is not None]

    assert len(measured) == 1
    model = measured[0]
    assert model.tokens is not None
    assert model.tokens.input_tokens == 20
    assert model.tokens.output_tokens == 8
    assert model.tokens.cached_input_tokens == 5
    assert model.tokens.reasoning_tokens == 3
    assert model.cost == Decimal("0.002")
    assert model.metadata["atif"]["metrics"] == source["steps"][2]["metrics"]
    assert model.metadata["atif"]["extra"] == source["steps"][2]["extra"]
    assert session.metadata["atif"]["extra"] == source["extra"]
    assert session.metadata["atif"]["final_metrics"] == source["final_metrics"]
    session.model_dump_json()


def test_does_not_invent_timing_when_timestamps_are_missing() -> None:
    """A valid untimed source does not become a zero-duration trace."""
    session = _parse_session(_trajectory())

    assert session.started_at is None
    assert session.ended_at is None
    assert all(
        node.started_at is None and node.ended_at is None
        for node in flatten_nodes(session.nodes)
    )


def test_final_cost_is_used_when_no_step_reports_cost() -> None:
    """Keep Claude-style total cost when the source has no per-step cost values."""
    source = _trajectory()
    source["steps"][2]["metrics"].pop("cost_usd")
    session = _parse_session(source)
    nodes = flatten_nodes(session.nodes)
    costs = [node.cost for node in nodes if node.cost is not None]

    assert costs == [Decimal("0.002")]
    assert nodes[0].cost == Decimal("0.002")
    assert "final_metrics" in session.metadata["normalization"]["cost"]


@pytest.mark.parametrize("step_cost", [0, 0.001])
def test_partial_step_cost_prevents_adding_final_total(step_cost: float) -> None:
    """Even a reported zero cost prevents a second aggregate cost contribution."""
    source = _trajectory()
    source["steps"][2]["metrics"]["cost_usd"] = step_cost
    source["final_metrics"]["total_cost_usd"] = 0.5
    nodes = flatten_nodes(_parse_session(source).nodes)

    assert nodes[0].cost is None
    assert sum(node.cost for node in nodes if node.cost is not None) == Decimal(
        str(step_cost)
    )


def test_child_final_cost_fallback_prevents_parent_total_double_counting() -> None:
    """A child's recorded cost total contributes once to the imported session."""
    source = _trajectory()
    source["schema_version"] = "ATIF-v1.8"
    source["steps"][2]["metrics"].pop("cost_usd")
    source["final_metrics"]["total_cost_usd"] = 0.5
    child = _trajectory()
    child["trajectory_id"] = "child"
    child["steps"][2]["metrics"].pop("cost_usd")
    source["subagent_trajectories"] = [child]
    result = source["steps"][2]["observation"]["results"][0]
    result["subagent_trajectory_ref"] = [{"trajectory_id": "child"}]
    nodes = flatten_nodes(_parse_session(source).nodes)

    assert nodes[0].cost is None
    assert [node.cost for node in nodes if node.cost is not None] == [Decimal("0.002")]


@pytest.mark.parametrize("copied_in_child", [False, True])
def test_copied_context_suppresses_ambiguous_final_cost(copied_in_child: bool) -> None:
    """Do not count a source total that may include copied historical execution."""
    source = _trajectory()
    source["schema_version"] = "ATIF-v1.8"
    source["steps"][2]["metrics"].pop("cost_usd")
    if copied_in_child:
        child = _trajectory()
        child["trajectory_id"] = "child"
        child["steps"][2]["is_copied_context"] = True
        source["subagent_trajectories"] = [child]
        result = source["steps"][2]["observation"]["results"][0]
        result["subagent_trajectory_ref"] = [{"trajectory_id": "child"}]
    else:
        source["steps"][2]["is_copied_context"] = True
    session = _parse_session(source)

    assert all(node.cost is None for node in flatten_nodes(session.nodes))
    assert session.metadata["atif"]["final_metrics"]["total_cost_usd"] == 0.002


def test_step_timestamp_does_not_imply_model_or_tool_duration() -> None:
    """Keep ATIF's event timestamp without treating it as a timed span."""
    source = _trajectory()
    source["steps"][2]["timestamp"] = "2026-09-10T10:00:00Z"
    session = _parse_session(source)
    model = next(node for node in flatten_nodes(session.nodes) if node.reasoning)

    assert model.metadata["atif"]["timestamp"] == "2026-09-10T10:00:00Z"
    assert model.ended_at is None


@pytest.mark.parametrize("count", [2, 4])
def test_aggregate_agent_steps_are_not_counted_as_one_model_call(count: int) -> None:
    """Preserve an agent step that represents zero or multiple model requests."""
    source = _trajectory()
    source["steps"][2]["llm_call_count"] = count
    session = _parse_session(source)
    step = next(node for node in flatten_nodes(session.nodes) if node.reasoning)

    assert step.node_type is NodeType.SPAN
    assert step.metadata["atif"]["llm_call_count"] == count


def test_zero_call_agent_step_has_no_invented_model_usage() -> None:
    """A local agent action does not imply a model call."""
    source = _trajectory()
    source["steps"][2]["llm_call_count"] = 0
    source["steps"][2].pop("metrics")
    source["steps"][2].pop("reasoning_content")
    session = _parse_session(source)
    nodes = flatten_nodes(session.nodes)
    tool = next(node for node in nodes if node.node_type is NodeType.TOOL_CALL)
    step = next(node for node in nodes if node.index == tool.parent_index)

    assert step.node_type is NodeType.SPAN
    assert step.model is None
    assert step.tokens is None
    assert step.cost is None


def test_absent_call_count_records_model_step_convention() -> None:
    """Older sources retain the importer convention as visible provenance."""
    source = _trajectory()
    source["steps"][2].pop("llm_call_count")
    session = _parse_session(source)
    model = next(node for node in flatten_nodes(session.nodes) if node.reasoning)

    assert model.node_type is NodeType.LLM_CALL
    assert "llm_call_count" in model.metadata["normalization"]


def test_explicit_tool_error_does_not_mark_whole_session_failed() -> None:
    """An agent may recover from a tool failure and still complete normally."""
    source = _trajectory()
    result = source["steps"][2]["observation"]["results"][0]
    result["extra"]["tool_result_is_error"] = True
    result["content"] = "The sample file was unavailable."
    session = _parse_session(source)
    tool = next(
        node
        for node in flatten_nodes(session.nodes)
        if node.node_type is NodeType.TOOL_CALL
    )

    assert tool.status is NodeStatus.FAILED
    assert session.status is SessionStatus.COMPLETED


@pytest.mark.parametrize("reward", [0, 1])
def test_verifier_reward_does_not_change_execution_status(reward: int) -> None:
    """An unsuccessful benchmark answer still represents a completed run."""
    result = {"verifier_result": {"rewards": {"reward": reward}}}
    session = _parse_session(
        {"trajectories": [{"trajectory": _trajectory(), "harbor_result": result}]}
    )

    assert session.status is SessionStatus.COMPLETED
    assert session.error is None
    assert session.metadata["harbor_result"] == result


def test_harbor_exception_marks_execution_failed_and_keeps_reward() -> None:
    """Execution exceptions are independent of the benchmark's reward value."""
    result = {
        "verifier_result": {"rewards": {"reward": 1}},
        "exception_info": {
            "exception_type": "AgentTimeoutError",
            "exception_message": "The fixture exceeded its time budget.",
            "exception_traceback": "synthetic traceback",
        },
    }
    session = _parse_session(
        {"trajectories": [{"trajectory": _trajectory(), "harbor_result": result}]}
    )

    assert session.status is SessionStatus.FAILED
    assert session.error is not None
    assert "time budget" in session.error
    assert session.metadata["harbor_result"] == result


def test_document_identity_is_deterministic_and_distinguishes_continuations() -> None:
    """Two documents sharing a conversation ID must not silently deduplicate."""
    source = _trajectory()
    reordered = dict(reversed(list(source.items())))
    continuation = _trajectory()
    continuation["steps"][-1]["message"] = "A later answer."

    first = _parse_session(source)
    assert first.external_id == _parse_session(reordered).external_id
    assert first.external_id != _parse_session(continuation).external_id
    assert (
        first.external_id != _parse_session(source, {"namespace": "other"}).external_id
    )


def test_trial_source_id_preserves_identity_when_harbor_result_is_added() -> None:
    """A stable trial-relative source ID identifies one evolving trial artifact."""
    entry = {"trajectory": _trajectory(), "source_id": "job/trial"}
    first = _parse_session({"trajectories": [entry]})
    entry["harbor_result"] = {"verifier_result": {"rewards": {"reward": 0}}}

    assert first.external_id == _parse_session({"trajectories": [entry]}).external_id
    entry["source_id"] = "job/other-trial"
    assert first.external_id != _parse_session({"trajectories": [entry]}).external_id


def test_batch_isolates_invalid_records_in_source_order() -> None:
    """Keep valid plain and wrapped entries around a malformed trajectory."""
    invalid = _trajectory()
    invalid["steps"][2]["step_id"] = 2
    payload = {"trajectories": [_trajectory(), invalid, {"trajectory": _trajectory()}]}
    items = list(parse(json.dumps(payload).encode(), {}))

    assert [type(item) for item in items] == [
        ImportedSession,
        ImportFailure,
        ImportedSession,
    ]
    assert isinstance(items[1], ImportFailure)
    assert items[1].line == 2


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("prompt_tokens", -1),
        ("completion_tokens", True),
        ("cached_tokens", "invalid"),
        ("cost_usd", -0.1),
        ("cost_usd", float("nan")),
        ("cost_usd", float("inf")),
    ],
)
def test_invalid_metrics_fail_only_the_affected_record(field: str, value: Any) -> None:
    """Prevent malformed numeric data from escaping into ingest requests."""
    invalid = _trajectory()
    invalid["steps"][2]["metrics"][field] = value
    items = list(
        parse(json.dumps({"trajectories": [invalid, _trajectory()]}).encode(), {})
    )

    assert [type(item) for item in items] == [ImportFailure, ImportedSession]
    for item in items:
        item.model_dump_json()


@pytest.mark.parametrize("value", ["broken\ud800", float("nan"), float("inf")])
def test_unserializable_source_extras_fail_locally(value: Any) -> None:
    """Source metadata must survive JSON serialization before a session is yielded."""
    invalid = _trajectory()
    invalid["extra"]["invalid"] = value
    items = list(
        parse(json.dumps({"trajectories": [invalid, _trajectory()]}).encode(), {})
    )

    assert [type(item) for item in items] == [ImportFailure, ImportedSession]
    for item in items:
        item.model_dump_json()


def test_unmatched_tool_result_fails_only_its_trajectory() -> None:
    """Do not attach a result to an unrelated or absent tool call."""
    invalid = _trajectory()
    invalid["steps"][2]["observation"]["results"][0]["source_call_id"] = "missing"
    items = list(
        parse(json.dumps({"trajectories": [invalid, _trajectory()]}).encode(), {})
    )

    assert [type(item) for item in items] == [ImportFailure, ImportedSession]


def test_embedded_subagent_is_attached_to_its_explicit_reference() -> None:
    """Nested documents become nodes only at a recorded delegation reference."""
    source = _trajectory()
    source["schema_version"] = "ATIF-v1.8"
    child = _trajectory()
    child["trajectory_id"] = "child"
    child["agent"]["name"] = "child-agent"
    source["subagent_trajectories"] = [child]
    result = source["steps"][2]["observation"]["results"][0]
    result["subagent_trajectory_ref"] = [{"trajectory_id": "child"}]
    session = _parse_session(source)
    nodes = flatten_nodes(session.nodes)
    call = next(node for node in nodes if node.node_type is NodeType.SUBAGENT_CALL)
    parent = next(node for node in nodes if node.index == call.parent_index)
    child_root = next(node for node in nodes if node.parent_index == call.index)

    assert call.subagent_id == "child"
    assert parent.node_type is NodeType.TOOL_CALL
    assert child_root.name == "child-agent"
    assert call.metadata["resolution"] == "embedded"
    assert len({node.external_id for node in nodes}) == len(nodes)
    assert sum(node.tokens.input_tokens or 0 for node in nodes if node.tokens) == 40


def test_repeated_embedded_reference_does_not_double_count_child_usage() -> None:
    """A second reference to one embedded document does not import its steps twice."""
    source = _trajectory()
    source["schema_version"] = "ATIF-v1.8"
    child = _trajectory()
    child["trajectory_id"] = "child"
    source["subagent_trajectories"] = [child]
    result = source["steps"][2]["observation"]["results"][0]
    result["subagent_trajectory_ref"] = [
        {"trajectory_id": "child"},
        {"trajectory_id": "child"},
    ]
    nodes = flatten_nodes(_parse_session(source).nodes)

    assert sum(node.node_type is NodeType.SUBAGENT_CALL for node in nodes) == 2
    assert sum(node.tokens.input_tokens or 0 for node in nodes if node.tokens) == 40


def test_source_identifiers_cannot_collide_with_generated_node_paths() -> None:
    """A trajectory ID containing node-path syntax must remain a distinct ID."""
    source = _trajectory()
    source["schema_version"] = "ATIF-v1.8"
    children = []
    for trajectory_id in ("child", "child/step/1"):
        child = _trajectory()
        child["trajectory_id"] = trajectory_id
        children.append(child)
    source["subagent_trajectories"] = children
    result = source["steps"][2]["observation"]["results"][0]
    result["subagent_trajectory_ref"] = [
        {"trajectory_id": child["trajectory_id"]} for child in children
    ]
    nodes = flatten_nodes(_parse_session(source).nodes)

    assert len({node.external_id for node in nodes}) == len(nodes)
    assert sum(node.node_type is NodeType.SUBAGENT_CALL for node in nodes) == 2


def test_long_subagent_identifier_preserves_source_without_breaking_ingest() -> None:
    """Long ATIF identities remain available despite native field length limits."""
    source = _trajectory()
    source["schema_version"] = "ATIF-v1.8"
    trajectory_id = "child-" + "x" * 300
    result = source["steps"][2]["observation"]["results"][0]
    result["subagent_trajectory_ref"] = [{"trajectory_id": trajectory_id}]
    nodes = flatten_nodes(_parse_session(source).nodes)
    subagent = next(node for node in nodes if node.node_type is NodeType.SUBAGENT_CALL)

    assert subagent.subagent_id is not None
    assert len(subagent.subagent_id) <= 255
    assert subagent.metadata["atif"]["trajectory_id"] == trajectory_id


def test_unreferenced_embedded_document_remains_source_metadata() -> None:
    """Do not invent a delegation relationship for an unreferenced document."""
    source = _trajectory()
    source["schema_version"] = "ATIF-v1.8"
    child = _trajectory()
    child["trajectory_id"] = "child"
    source["subagent_trajectories"] = [child]
    nodes = flatten_nodes(_parse_session(source).nodes)

    assert not any(node.node_type is NodeType.SUBAGENT_CALL for node in nodes)
    assert nodes[0].metadata["unreferenced_subagent_trajectories"] == [child]


def test_preserves_external_media_and_subagent_refs_without_resolution() -> None:
    """Import unavailable media and external trajectories as source references."""
    source = _trajectory()
    source["schema_version"] = "ATIF-v1.8"
    message = [
        {"type": "text", "text": "Inspect the attached sample."},
        {
            "type": "image",
            "source": {"path": "/missing/fixture.png", "media_type": "image/png"},
        },
    ]
    source["steps"][1]["message"] = message
    ref = {
        "trajectory_path": "https://invalid.example/child.json",
        "session_id": "child",
    }
    source["steps"][2]["observation"]["results"][0]["subagent_trajectory_ref"] = [ref]
    session = _parse_session(source)
    subagent = next(
        node
        for node in flatten_nodes(session.nodes)
        if node.node_type is NodeType.SUBAGENT_CALL
    )

    assert session.inputs["messages"][1]["message"] == message
    assert session.input_text_selector == "/messages/1/message/0/text"
    assert subagent.metadata["atif"] == ref
    assert subagent.metadata["resolution"] == "external_or_unresolved"


def test_external_reference_does_not_attach_same_id_from_another_document() -> None:
    """An explicit external path takes precedence over a coincident embedded ID."""
    source = _trajectory()
    source["schema_version"] = "ATIF-v1.8"
    child = _trajectory()
    child["trajectory_id"] = "child"
    source["subagent_trajectories"] = [child]
    result = source["steps"][2]["observation"]["results"][0]
    result["subagent_trajectory_ref"] = [
        {"trajectory_id": "child", "trajectory_path": "../other/trajectory.json"}
    ]
    nodes = flatten_nodes(_parse_session(source).nodes)
    subagent = next(node for node in nodes if node.node_type is NodeType.SUBAGENT_CALL)

    assert subagent.metadata["resolution"] == "external_or_unresolved"
    assert not any(node.parent_index == subagent.index for node in nodes)
    assert sum(node.tokens.input_tokens or 0 for node in nodes if node.tokens) == 20


def test_tool_without_recorded_result_is_not_marked_successful() -> None:
    """Interrupted traces retain a tool call even when its result never arrived."""
    source = _trajectory()
    source["steps"][2].pop("observation")
    nodes = flatten_nodes(_parse_session(source).nodes)
    tool = next(node for node in nodes if node.node_type is NodeType.TOOL_CALL)

    assert tool.outputs == {"results": []}
    assert tool.status is NodeStatus.IN_PROGRESS


def test_harbor_agent_execution_times_define_session_duration() -> None:
    """Use the agent execution interval rather than setup or verifier timing."""
    result = {
        "started_at": "2026-09-10T09:55:00Z",
        "finished_at": "2026-09-10T10:05:00Z",
        "agent_execution": {
            "started_at": "2026-09-10T10:00:00Z",
            "finished_at": "2026-09-10T10:02:00Z",
        },
    }
    session = _parse_session(
        {"trajectories": [{"trajectory": _trajectory(), "harbor_result": result}]}
    )

    assert session.started_at is not None
    assert session.ended_at is not None
    assert (session.ended_at - session.started_at).total_seconds() == 120
    assert session.framework == "harbor"
    assert all(
        node.ended_at is None
        for node in flatten_nodes(session.nodes)
        if node.parent_index is not None
    )


def test_copied_context_preserves_evidence_without_counting_execution_again() -> None:
    """Copied historical steps retain their source data without duplicating usage."""
    source = _trajectory()
    source["schema_version"] = "ATIF-v1.8"
    source["steps"][2]["is_copied_context"] = True
    child = _trajectory()
    child["trajectory_id"] = "child"
    child["agent"]["name"] = "historical-child"
    source["subagent_trajectories"] = [child]
    result = source["steps"][2]["observation"]["results"][0]
    result["subagent_trajectory_ref"] = [{"trajectory_id": "child"}]
    nodes = flatten_nodes(_parse_session(source).nodes)
    copied_step = next(node for node in nodes if node.reasoning)
    copied_children = [node for node in nodes if node.parent_index == copied_step.index]

    assert copied_step.node_type is NodeType.SPAN
    assert copied_step.tokens is None
    assert copied_step.cost is None
    assert copied_step.metadata["atif"]["metrics"] == source["steps"][2]["metrics"]
    assert copied_step.metadata["atif"]["is_copied_context"] is True
    assert copied_children
    assert all(node.node_type is NodeType.SPAN for node in copied_children)
    assert sum(node.node_type is NodeType.LLM_CALL for node in nodes) == 1
    assert not any(node.tokens for node in nodes)
    assert not any(node.node_type is NodeType.SUBAGENT_CALL for node in nodes)
    reference = next(
        node for node in nodes if node.metadata.get("resolution") == "copied_context"
    )
    assert not any(node.parent_index == reference.index for node in nodes)


def test_tool_argument_text_selector_escapes_json_pointer_characters() -> None:
    """A single text argument remains selectable when its key contains slashes."""
    source = _trajectory()
    source["steps"][2]["tool_calls"][0]["arguments"] = {"path/to~file": "sample.txt"}
    nodes = flatten_nodes(_parse_session(source).nodes)
    tool = next(node for node in nodes if node.node_type is NodeType.TOOL_CALL)

    assert tool.input_text_selector == "/path~1to~0file"


def test_large_trajectory_flattens_beyond_one_ingest_batch() -> None:
    """Hundreds of source steps must not create an invalid deep node tree."""
    source = _trajectory()
    source["steps"] = [
        {"step_id": index + 1, "source": "agent", "message": f"Step {index + 1}."}
        for index in range(550)
    ]
    session = _parse_session(source)
    nodes = flatten_nodes(session.nodes)

    assert len(nodes) > 500
    assert len({node.index for node in nodes}) == len(nodes)
    assert len({node.external_id for node in nodes}) == len(nodes)
    session.model_dump_json()


@pytest.mark.parametrize("params", [{"unknown": True}, {"namespace": 1}])
def test_rejects_unsupported_parameters(params: dict[str, Any]) -> None:
    """Misconfigured imports should fail before yielding any sessions."""
    items = list(parse(json.dumps(_trajectory()).encode(), params))

    assert len(items) == 1
    assert isinstance(items[0], ImportFailure)


@pytest.mark.parametrize(
    "payload", [b"", b"\xff", b"not JSON", b"[]", b'{"trajectories":[]}']
)
def test_rejects_invalid_uploads(payload: bytes) -> None:
    """Malformed upload framing cannot masquerade as an empty successful import."""
    items = list(parse(payload, {}))

    assert len(items) == 1
    assert isinstance(items[0], ImportFailure)
