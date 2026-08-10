"""Tests for target trace conversion."""

import uuid
from datetime import UTC, datetime
from typing import Any

import pytest

from kitaru._exports.models import ExportError
from kitaru._exports.trace import TraceFormat, convert_trace, redact_secret_values
from kitaru.api_models.v1.session import (
    SessionOrigin,
    SessionResponse,
    SessionStatus,
)
from kitaru.api_models.v1.session_node import SessionWithNodesResponse


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
    assert view.nodes[1].outputs["tool_calls"][0]["id"] == "call-1"
    assert view.nodes[2].inputs == {"a": 6, "b": 7}
    assert view.nodes[2].outputs == "42"
    assert view.nodes[3].outputs == {"message": "The answer is 42."}
    assert view.session.tokens is not None
    assert view.session.tokens.input_tokens == 10
    assert view.session.tokens.output_tokens == 4
    assert view.session.tokens.cached_input_tokens == 2


def test_convert_verifiers_preserves_linear_message_trace() -> None:
    view = convert_trace(_verifiers_trace(), format="verifiers-v1", context=_context())

    assert view.session.outputs == "The answer is 42."
    assert view.session.llm_call_count == 2
    assert view.session.tool_call_count == 1
    assert [node.index for node in view.nodes] == [0, 1, 2, 3]
    assert view.nodes[1].outputs["tool_calls"][0]["arguments"] == '{"a":6,"b":7}'
    assert view.nodes[2].external_id == "call-1"
    assert view.nodes[2].outputs == "42"
    assert view.nodes[3].tokens is not None
    assert view.nodes[3].tokens.output_tokens == 5


def test_convert_full_session_keeps_context_identity_and_real_nodes() -> None:
    atif_view = convert_trace(_atif_trace(), format="atif", context=_context())
    emitted = {
        "session": atif_view.session.model_copy(
            update={"id": uuid.UUID(int=99), "inputs": {"wrong": True}}
        ).model_dump(mode="json"),
        "nodes": [
            node.model_copy(update={"session_id": uuid.UUID(int=99)}).model_dump(
                mode="json"
            )
            for node in atif_view.nodes
        ],
    }

    view = convert_trace(emitted, format="kitaru", context=_context())

    assert view.session.id == uuid.UUID(int=1)
    assert view.session.inputs == {"question": "What is six times seven?"}
    assert view.session.outputs == "The answer is 42."
    assert len(view.nodes) == 4
    assert all(node.session_id == uuid.UUID(int=1) for node in view.nodes)


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
            "invalid_trace_order",
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
