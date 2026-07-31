"""Contract tests for the canonical support agent example."""

import importlib
import json
import sys
from pathlib import Path

from pydantic_ai.messages import (
    ModelRequest,
    TextPart,
    ToolCallPart,
    ToolReturnPart,
    UserPromptPart,
)
from pydantic_ai.models import ModelRequestParameters
from pydantic_ai.models.function import AgentInfo

from kitaru.api_models.v1.session import SessionResponse
from kitaru.task.evaluator import SessionView
from kitaru.worker.process import parse_inline_dependencies

REPOSITORY_ROOT = Path(__file__).parents[1]
EXAMPLE_DIR = REPOSITORY_ROOT / "examples" / "support_agent"
sys.path.insert(0, str(REPOSITORY_ROOT))
agent_module = importlib.import_module("examples.support_agent.agent")
evaluator_module = importlib.import_module("examples.support_agent.evaluator")
importer_module = importlib.import_module("examples.support_agent.trace_importer")
ORDER_DATA = agent_module.ORDER_DATA
_support_model = agent_module._support_model
lookup_order = agent_module.lookup_order
evaluate = evaluator_module.evaluate
parse = importer_module.parse


def _agent_info() -> AgentInfo:
    """Build the model callback context used by the contract test."""
    return AgentInfo(
        function_tools=[],
        allow_text_output=True,
        output_tools=[],
        model_settings=None,
        model_request_parameters=ModelRequestParameters(),
        instructions=None,
    )


def test_importer_parses_production_sessions_and_nodes() -> None:
    """Parse every fixture line into a session with an LLM child node."""
    sessions = list(
        parse(
            (EXAMPLE_DIR / "production_traces.jsonl").read_bytes(),
            {"source": "contract-test"},
        )
    )

    assert len(sessions) == 3
    assert {session.external_id for session in sessions} == {
        "ticket-1042",
        "ticket-2088",
        "ticket-3091",
    }
    assert all(
        session.metadata["import_source"] == "contract-test" for session in sessions
    )
    assert all(session.nodes[0].children[0].name == "respond" for session in sessions)


def test_plugin_scripts_follow_pep_723_without_extra_dependencies() -> None:
    """Keep both registered scripts runnable in the worker environment."""
    assert parse_inline_dependencies(EXAMPLE_DIR / "trace_importer.py") == []
    assert parse_inline_dependencies(EXAMPLE_DIR / "evaluator.py") == []


def test_evaluator_compares_output_to_expected_value() -> None:
    """Pass exact outcomes and reject stale support responses."""
    passing = SessionView(
        session=SessionResponse.model_construct(
            outputs="expected", expected="expected"
        ),
        nodes=[],
    )
    failing = SessionView(
        session=SessionResponse.model_construct(outputs="stale", expected="expected"),
        nodes=[],
    )
    replay = SessionView(
        session=SessionResponse.model_construct(
            inputs={"expected_output": "expected"},
            outputs="expected",
            expected=None,
        ),
        nodes=[],
    )

    assert evaluate(passing).passed is True
    assert evaluate(failing).passed is False
    assert evaluate(replay).passed is True


def test_candidate_model_grounds_response_in_order_lookup() -> None:
    """Request a tool lookup before returning its sorted JSON result."""
    inputs = {"order_id": "A-1042", "question": "Where is my order?"}
    initial = _support_model(
        [ModelRequest(parts=[UserPromptPart(json.dumps(inputs))])],
        _agent_info(),
    )
    call = initial.parts[0]

    assert isinstance(call, ToolCallPart)
    assert call.tool_name == "lookup_order"
    assert call.args == {"order_id": "A-1042"}

    outcome = lookup_order("A-1042")
    final = _support_model(
        [ModelRequest(parts=[ToolReturnPart("lookup_order", outcome)])],
        _agent_info(),
    )
    text = final.parts[0]

    assert isinstance(text, TextPart)
    assert text.content == json.dumps(ORDER_DATA["A-1042"], sort_keys=True)
