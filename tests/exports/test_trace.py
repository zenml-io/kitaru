"""Tests for target trace conversion."""

import uuid
from copy import deepcopy
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from kitaru.api_models.v1.session import (
    SessionOrigin,
    SessionResponse,
    SessionStatus,
)
from kitaru.api_models.v1.session_node import SessionWithNodesResponse
from kitaru.exports.models import ExportError
from kitaru.exports.trace import TraceFormat, convert_trace, redact_secret_values


def _context() -> SessionWithNodesResponse:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    return SessionWithNodesResponse(
        session=SessionResponse(
            id=uuid.UUID(int=1),
            owner_id=uuid.UUID(int=2),
            agent_id=uuid.UUID(int=3),
            agent_version_id=uuid.UUID(int=4),
            number=7,
            origin=SessionOrigin.RECORDED,
            status=SessionStatus.COMPLETED,
            name="frozen case",
            inputs={"question": "What is six times seven?"},
            outputs={"answer": "old"},
            metadata={"cohort": "canonical"},
            llm_call_count=0,
            tool_call_count=0,
            created=now,
            updated=now,
        ),
        nodes=[],
    )


def _atif_trace() -> dict[str, Any]:
    return {
        "schema_version": "ATIF-v1.7",
        "session_id": "run-1",
        "agent": {"name": "fixture", "version": "1"},
        "steps": [
            {
                "step_id": 1,
                "timestamp": "2026-01-02T03:04:05Z",
                "source": "user",
                "message": "What is six times seven?",
            },
            {
                "step_id": 2,
                "timestamp": "2026-01-02T03:04:06Z",
                "source": "agent",
                "model_name": "fixture-model",
                "message": "I will calculate it.",
                "reasoning_content": "Use the calculator",
                "tool_calls": [
                    {
                        "tool_call_id": "call-1",
                        "function_name": "multiply",
                        "arguments": {"a": 6, "b": 7},
                    }
                ],
                "observation": {
                    "results": [{"source_call_id": "call-1", "content": "42"}]
                },
                "metrics": {
                    "prompt_tokens": 10,
                    "completion_tokens": 4,
                    "cached_tokens": 2,
                    "cost_usd": 0.01,
                },
            },
            {
                "step_id": 3,
                "timestamp": "2026-01-02T03:04:07Z",
                "source": "agent",
                "message": "The answer is 42.",
                "llm_call_count": 1,
            },
        ],
    }


def _verifiers_trace() -> dict[str, Any]:
    return {
        "version": 1,
        "id": "trace-1",
        "task": {"type": "Task", "data": {"question": "ignored context"}},
        "agent": {"config": {}, "name": "solver", "trainable": True},
        "nodes": [
            {
                "parent": None,
                "message": {"role": "user", "content": "Calculate 6 * 7"},
                "sampled": False,
                "timestamp": 1767323045.0,
            },
            {
                "parent": 0,
                "message": {
                    "role": "assistant",
                    "content": None,
                    "reasoning_content": "Call the tool",
                    "tool_calls": [
                        {
                            "id": "call-1",
                            "name": "multiply",
                            "arguments": '{"a":6,"b":7}',
                        }
                    ],
                },
                "sampled": True,
                "timestamp": 1767323046.0,
            },
            {
                "parent": 1,
                "message": {
                    "role": "tool",
                    "tool_call_id": "call-1",
                    "name": "multiply",
                    "content": "42",
                },
                "sampled": False,
                "timestamp": 1767323047.0,
            },
            {
                "parent": 2,
                "message": {"role": "assistant", "content": "The answer is 42."},
                "sampled": True,
                "timestamp": 1767323048.0,
            },
        ],
        "calls": [
            {
                "node": 1,
                "model": "fixture-model",
                "usage": {
                    "prompt_tokens": 8,
                    "completion_tokens": 3,
                    "cached_input_tokens": 2,
                    "reasoning_tokens": 1,
                    "cost": 0.02,
                },
                "time": {"start": 1767323045.5, "end": 1767323046.0},
            },
            {
                "node": 3,
                "model": "fixture-model",
                "usage": {"prompt_tokens": 12, "completion_tokens": 5},
                "time": {"start": 1767323047.5, "end": 1767323048.0},
            },
        ],
        "is_completed": True,
        "ok": True,
        "errors": [],
        "timing": {"start": 1767323045.0},
    }


def _kitaru_trace(
    *,
    status: SessionStatus = SessionStatus.COMPLETED,
    atif_trace: dict[str, Any] | None = None,
) -> dict[str, Any]:
    view = convert_trace(atif_trace or _atif_trace(), format="atif", context=_context())
    source_id = uuid.UUID(int=99)
    error = "agent failed" if status == SessionStatus.FAILED else None
    nodes = []
    for node in view.nodes:
        nodes.append(
            node.model_copy(
                update={
                    "id": uuid.uuid5(source_id, f"export-trace-node:{node.index}"),
                    "session_id": source_id,
                    "parent_id": uuid.uuid5(
                        source_id, f"export-trace-node:{node.parent_index}"
                    )
                    if node.parent_index is not None
                    else None,
                    "secondary_parent_ids": [
                        uuid.uuid5(source_id, f"export-trace-node:{parent}")
                        for parent in node.secondary_parent_indexes
                    ],
                }
            ).model_dump(mode="json")
        )
    return {
        "session": view.session.model_copy(
            update={
                "id": source_id,
                "status": status,
                "inputs": {"wrong": True},
                "outputs": {"partial": "42"} if error else "The answer is 42.",
                "error": error,
                "llm_call_count": 7,
                "tool_call_count": 3,
            }
        ).model_dump(mode="json"),
        "nodes": nodes,
    }


def test_convert_atif_preserves_messages_tools_usage_and_output() -> None:
    view = convert_trace(_atif_trace(), format="atif", context=_context())

    assert view.session.inputs == {"question": "What is six times seven?"}
    assert view.session.outputs == "The answer is 42."
    assert [node.node_type.value for node in view.nodes] == [
        "span",
        "llm_call",
        "tool_call",
        "llm_call",
    ]
    assert view.nodes[0].inputs == {
        "role": "user",
        "content": "What is six times seven?",
    }
    assert view.nodes[1].reasoning == "Use the calculator"
    assert view.nodes[1].inputs == {
        "messages": [{"role": "user", "content": "What is six times seven?"}]
    }
    assert view.nodes[1].outputs["tool_calls"][0]["id"] == "call-1"
    assert view.nodes[2].inputs == {"a": 6, "b": 7}
    assert view.nodes[2].outputs == "42"
    assert view.nodes[3].outputs == {"message": "The answer is 42."}
    assert view.nodes[3].inputs == {
        "messages": [
            {"role": "user", "content": "What is six times seven?"},
            {
                "role": "assistant",
                "content": "I will calculate it.",
                "reasoning_content": "Use the calculator",
                "tool_calls": [
                    {
                        "id": "call-1",
                        "name": "multiply",
                        "arguments": {"a": 6, "b": 7},
                    }
                ],
            },
            {
                "role": "tool",
                "tool_call_id": "call-1",
                "name": "multiply",
                "content": "42",
            },
        ]
    }
    assert view.session.tokens is not None
    assert view.session.tokens.input_tokens == 10
    assert view.session.tokens.output_tokens == 4
    assert view.session.tokens.cached_input_tokens == 2
    assert view.session.cost == Decimal("0.01")


def test_convert_atif_keeps_parallel_tool_calls_as_siblings_and_joins() -> None:
    trace = _atif_trace()
    trace["steps"][1]["tool_calls"] = [
        {
            "tool_call_id": "call-left",
            "function_name": "multiply",
            "arguments": {"a": 6, "b": 7},
        },
        {
            "tool_call_id": "call-right",
            "function_name": "add",
            "arguments": {"a": 40, "b": 2},
        },
    ]
    trace["steps"][1]["observation"] = {
        "results": [
            {"source_call_id": "call-left", "content": "42"},
            {"source_call_id": "call-right", "content": "42"},
        ]
    }

    view = convert_trace(trace, format="atif", context=_context())

    assert [node.parent_index for node in view.nodes] == [None, 0, 1, 1, 2]
    assert view.nodes[4].secondary_parent_indexes == [3]
    assert view.nodes[4].secondary_parent_ids == [view.nodes[3].id]


def test_convert_atif_prefers_declared_final_usage_and_cost() -> None:
    trace = _atif_trace()
    trace["final_metrics"] = {
        "total_prompt_tokens": 30,
        "total_completion_tokens": 9,
        "total_cached_tokens": 4,
        "total_cost_usd": 0.025,
        "total_steps": 3,
    }

    view = convert_trace(trace, format="atif", context=_context())

    assert view.session.tokens is not None
    assert view.session.tokens.input_tokens == 30
    assert view.session.tokens.output_tokens == 9
    assert view.session.tokens.cached_input_tokens == 4
    assert view.session.cost == Decimal("0.025")


@pytest.mark.parametrize(
    ("declared_count", "expected_count", "expected_type", "warns"),
    [
        (0, 1, "span", False),
        (1, 2, "llm_call", False),
        (3, 4, "llm_call", True),
    ],
)
def test_convert_atif_preserves_declared_llm_call_counts(
    declared_count: int,
    expected_count: int,
    expected_type: str,
    warns: bool,
) -> None:
    trace = _atif_trace()
    trace["steps"][1]["llm_call_count"] = declared_count
    if declared_count == 0:
        trace["steps"][1].pop("metrics")
        trace["steps"][1].pop("reasoning_content")

    view = convert_trace(trace, format="atif", context=_context())

    assert view.session.llm_call_count == expected_count
    assert view.nodes[1].node_type.value == expected_type
    assert view.nodes[1].metadata["declared_llm_call_count"] == declared_count
    assert ("conversion_warnings" in view.nodes[1].metadata) is warns


def test_convert_verifiers_preserves_linear_message_trace() -> None:
    view = convert_trace(_verifiers_trace(), format="verifiers-v1", context=_context())

    assert view.session.outputs == "The answer is 42."
    assert view.session.llm_call_count == 2
    assert view.session.tool_call_count == 1
    assert [node.index for node in view.nodes] == [0, 1, 2, 3]
    assert view.nodes[1].outputs["tool_calls"][0]["arguments"] == '{"a":6,"b":7}'
    assert view.nodes[1].inputs == {
        "messages": [{"role": "user", "content": "Calculate 6 * 7"}]
    }
    assert view.nodes[2].external_id == "call-1"
    assert view.nodes[2].outputs == "42"
    assert view.nodes[3].tokens is not None
    assert view.nodes[3].tokens.output_tokens == 5
    assert view.session.tokens is not None
    assert view.session.tokens.input_tokens == 22
    assert view.session.cost == Decimal("0.02")


def test_convert_verifiers_preserves_branches_without_linearizing() -> None:
    trace = _verifiers_trace()
    trace["nodes"] = [
        trace["nodes"][0],
        {
            "parent": 0,
            "message": {"role": "assistant", "content": "branch one"},
            "sampled": True,
            "timestamp": 1767323046.0,
        },
        {
            "parent": 0,
            "message": {"role": "assistant", "content": "branch two"},
            "sampled": True,
            "timestamp": 1767323047.0,
        },
        {
            "parent": 1,
            "message": {"role": "user", "content": "continue branch one"},
            "sampled": False,
            "timestamp": 1767323048.0,
        },
        {
            "parent": 3,
            "message": {"role": "assistant", "content": "joined outcome"},
            "sampled": True,
            "timestamp": 1767323049.0,
        },
    ]
    trace["calls"] = [
        {"node": 1, "model": "fixture-model"},
        {"node": 2, "model": "fixture-model"},
        {"node": 4, "model": "fixture-model"},
    ]

    view = convert_trace(trace, format="verifiers-v1", context=_context())

    assert [node.parent_index for node in view.nodes] == [None, 0, 0, 1, 3]
    assert view.nodes[4].inputs == {
        "messages": [
            {"role": "user", "content": "Calculate 6 * 7"},
            {"role": "assistant", "content": "branch one"},
            {"role": "user", "content": "continue branch one"},
        ]
    }


def test_convert_verifiers_preserves_recovered_retry_history() -> None:
    trace = _verifiers_trace()
    retry = {
        "node": None,
        "model": "fixture-model",
        "usage": {"prompt_tokens": 2, "completion_tokens": 0, "cost": 0.005},
        "time": {"start": 1767323045.1, "end": 1767323045.4},
        "error": {"type": "ProviderError", "message": "temporary failure"},
    }
    trace["calls"].insert(0, retry)
    trace["errors"] = [{"type": "ProviderError", "message": "temporary failure"}]

    view = convert_trace(trace, format="verifiers-v1", context=_context())

    assert view.session.status == SessionStatus.COMPLETED
    assert view.session.error is None
    assert view.session.llm_call_count == 3
    assert view.session.tokens is not None
    assert view.session.tokens.input_tokens == 24
    assert view.session.cost == Decimal("0.025")
    conversion = view.session.metadata["trace_conversion"]
    assert conversion["uncommitted_model_calls"][0]["error"]["message"] == (
        "temporary failure"
    )
    assert conversion["errors"][0]["message"] == "temporary failure"


def test_convert_verifiers_preserves_final_failed_retry() -> None:
    trace = _verifiers_trace()
    trace["ok"] = False
    trace["errors"] = [{"type": "RuntimeError", "message": "retry exhausted"}]
    trace["calls"].append(
        {
            "node": None,
            "model": "fixture-model",
            "error": {"type": "ProviderError", "message": "provider down"},
        }
    )

    view = convert_trace(trace, format="verifiers-v1", context=_context())

    assert view.session.status == SessionStatus.FAILED
    assert view.session.error == "retry exhausted"
    assert view.session.outputs == "The answer is 42."
    assert view.session.llm_call_count == 3
    assert view.session.metadata["trace_conversion"]["uncommitted_model_calls"] == [
        {
            "node": None,
            "model": "fixture-model",
            "error": {"type": "ProviderError", "message": "provider down"},
        }
    ]


def test_convert_full_session_keeps_context_identity_and_real_nodes() -> None:
    emitted = _kitaru_trace()

    view = convert_trace(emitted, format="kitaru", context=_context())

    assert view.session.id == uuid.UUID(int=1)
    assert view.session.inputs == {"question": "What is six times seven?"}
    assert view.session.outputs == "The answer is 42."
    assert view.session.llm_call_count == 7
    assert view.session.tool_call_count == 3
    assert len(view.nodes) == 4
    assert all(node.session_id == uuid.UUID(int=1) for node in view.nodes)


def test_convert_full_session_preserves_secondary_ancestry() -> None:
    atif_trace = _atif_trace()
    atif_trace["steps"][1]["tool_calls"].append(
        {
            "tool_call_id": "call-2",
            "function_name": "add",
            "arguments": {"a": 40, "b": 2},
        }
    )
    atif_trace["steps"][1]["observation"]["results"].append(
        {"source_call_id": "call-2", "content": "42"}
    )
    emitted = _kitaru_trace(atif_trace=atif_trace)

    view = convert_trace(emitted, format="kitaru", context=_context())

    assert view.nodes[4].parent_index == 2
    assert view.nodes[4].secondary_parent_indexes == [3]
    assert view.nodes[4].secondary_parent_ids == [view.nodes[3].id]


@pytest.mark.parametrize(
    ("status", "error", "expected"),
    [
        (SessionStatus.COMPLETED, None, SessionStatus.COMPLETED),
        (SessionStatus.FAILED, "agent failed", SessionStatus.FAILED),
    ],
)
def test_convert_kitaru_preserves_terminal_outcome(
    status: SessionStatus, error: str | None, expected: SessionStatus
) -> None:
    trace = _kitaru_trace(status=status)
    trace["session"]["error"] = error

    view = convert_trace(trace, format="kitaru", context=_context())

    assert view.session.status == expected
    assert view.session.error == error


def test_convert_kitaru_rejects_nonterminal_session() -> None:
    trace = _kitaru_trace()
    trace["session"]["status"] = "in_progress"

    with pytest.raises(ExportError) as raised:
        convert_trace(trace, format="kitaru", context=_context())

    assert raised.value.code == "incomplete_trace"


@pytest.mark.parametrize(
    ("mutate", "code"),
    [
        (lambda nodes: nodes[1].update(parent_index="0"), "malformed_trace_parent"),
        (lambda nodes: nodes[2].update(parent_index=99), "trace_parent_out_of_range"),
        (lambda nodes: nodes[2].update(parent_index=3), "forward_trace_parent"),
        (
            lambda nodes: nodes[3].update(parent_index=1, secondary_parent_indexes=[1]),
            "duplicate_trace_parent",
        ),
        (
            lambda nodes: nodes[1].update(index=10),
            "missing_trace_parent",
        ),
    ],
)
def test_convert_kitaru_rejects_invalid_parent_graphs(mutate: Any, code: str) -> None:
    trace = _kitaru_trace()
    mutate(trace["nodes"])

    with pytest.raises(ExportError) as raised:
        convert_trace(trace, format="kitaru", context=_context())

    assert raised.value.code == code


@pytest.mark.parametrize(
    ("parent", "code"),
    [
        ("0", "malformed_trace_parent"),
        (99, "trace_parent_out_of_range"),
        (2, "forward_trace_parent"),
        ([0, 1], "malformed_trace_parent"),
    ],
)
def test_convert_verifiers_rejects_invalid_parents(parent: Any, code: str) -> None:
    trace = _verifiers_trace()
    trace["nodes"][1]["parent"] = parent

    with pytest.raises(ExportError) as raised:
        convert_trace(trace, format="verifiers-v1", context=_context())

    assert raised.value.code == code


def test_convert_verifiers_rejects_missing_parent() -> None:
    trace = _verifiers_trace()
    del trace["nodes"][1]["parent"]

    with pytest.raises(ExportError) as raised:
        convert_trace(trace, format="verifiers-v1", context=_context())

    assert raised.value.code == "missing_trace_parent"


@pytest.mark.parametrize("format", ["atif", "verifiers-v1"])
def test_convert_trace_rejects_missing_tool_results_with_stable_code(
    format: TraceFormat,
) -> None:
    trace = deepcopy(_atif_trace() if format == "atif" else _verifiers_trace())
    if format == "atif":
        trace["steps"][1]["observation"]["results"] = []
    else:
        trace["nodes"].pop(2)
        trace["nodes"][2]["parent"] = 1
        trace["calls"][1]["node"] = 2

    with pytest.raises(ExportError) as raised:
        convert_trace(trace, format=format, context=_context())

    assert raised.value.code == "missing_tool_result"


@pytest.mark.parametrize(
    ("format", "trace", "code"),
    [
        (
            "atif",
            {"schema_version": "ATIF-v1.7", "agent": {}, "steps": []},
            "invalid_trace",
        ),
        (
            "atif",
            {
                "schema_version": "ATIF-v1.7",
                "agent": {"name": "x", "version": "1"},
                "steps": [{"step_id": 2, "source": "agent", "message": "x"}],
            },
            "invalid_trace_order",
        ),
        (
            "verifiers-v1",
            {
                **_verifiers_trace(),
                "nodes": [
                    {"parent": None, "message": {"role": "user", "content": "x"}},
                    {"parent": None, "message": {"role": "assistant", "content": "y"}},
                ],
            },
            "unsupported_trace_shape",
        ),
        (
            "kitaru",
            {"session": _context().session.model_dump(mode="json"), "nodes": []},
            "incomplete_trace",
        ),
    ],
)
def test_convert_trace_rejects_incomplete_or_malformed_traces(
    format: TraceFormat, trace: dict[str, object], code: str
) -> None:
    with pytest.raises(ExportError) as raised:
        convert_trace(trace, format=format, context=_context())

    assert raised.value.code == code


def test_redact_secret_values_recurses_without_mutating_source() -> None:
    source = {
        "message": "token=secret-value",
        "nested": ["secret-value", {"other": "safe"}],
        "number": 3,
    }

    redacted = redact_secret_values(source, ["secret-value", ""])

    assert redacted == {
        "message": "token=[REDACTED]",
        "nested": ["[REDACTED]", {"other": "safe"}],
        "number": 3,
    }
    assert source["message"] == "token=secret-value"


def test_convert_trace_redacts_secrets_before_building_session() -> None:
    trace = _atif_trace()
    trace["steps"][2]["message"] = "The token is secret-value"

    view = convert_trace(
        trace, format="atif", context=_context(), secret_values=["secret-value"]
    )

    assert view.session.outputs == "The token is [REDACTED]"
    assert "secret-value" not in view.model_dump_json()
