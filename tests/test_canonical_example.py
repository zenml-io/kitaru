"""Contract tests for the canonical returns-resolution example."""

import json
import runpy
import uuid
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from examples.pydantic_ai_ticket_resolver.agent import (
    _build_ci_model,
    build_agent,
    build_prompt,
    get_instructions,
    get_ticket_input,
)
from examples.pydantic_ai_ticket_resolver.fixtures import CASES
from examples.pydantic_ai_ticket_resolver.models import ResolutionAction
from examples.pydantic_ai_ticket_resolver.store import MockCommerceStore

from kitaru.api_models.v1.session import SessionResponse
from kitaru.api_models.v1.session_node import NodeType, SessionNodeResponse
from kitaru.task.evaluator import SessionView
from kitaru.task.importer import ImportedSession, flatten_nodes

REPOSITORY_ROOT = Path(__file__).parents[1]
EXAMPLE_DIR = REPOSITORY_ROOT / "examples" / "pydantic_ai_ticket_resolver"
TRACE_PATH = EXAMPLE_DIR / "traces" / "langfuse-traces.jsonl"
IMPORTER_PATH = (
    REPOSITORY_ROOT
    / "plugins/packages/langfuse-importer/src/kitaru_langfuse_importer/importer.py"
)
parse = runpy.run_path(str(IMPORTER_PATH))["parse"]


def test_fixture_corpus_covers_ten_distinct_resolution_scenarios() -> None:
    """Keep the baseline compact while covering useful behavioral branches."""
    assert len(CASES) == 10
    assert len({case.scenario for case in CASES}) == 10
    assert len({case.ticket.ticket_id for case in CASES}) == 10
    assert {case.expected_action for case in CASES} == {
        ResolutionAction.REFUND,
        ResolutionAction.REPLACEMENT,
        ResolutionAction.ESCALATE,
    }
    assert all(case.ticket.email.endswith("@example.test") for case in CASES)


def test_mock_store_records_only_local_refund_side_effects() -> None:
    """Record a valid refund in an isolated store and reject an over-refund."""
    store = MockCommerceStore()

    accepted = store.issue_refund("48213", Decimal("98.00"))
    rejected = MockCommerceStore().issue_refund("48213", Decimal("120.00"))

    assert accepted.accepted is True
    assert accepted.receipt_id == "mock-refund-48213"
    assert store.orders["48213"].already_refunded is True
    assert rejected.accepted is False
    assert rejected.receipt_id is None


def test_order_lookup_can_retry_by_email_after_a_wrong_number() -> None:
    """Provide one natural repeated-tool path for the starting-point evaluator."""
    store = MockCommerceStore()

    missing = store.lookup_order(order_id="48228")
    recovered = store.lookup_order(email="riley@example.test")

    assert missing.found is False
    assert recovered.found is True
    assert [order.order_id for order in recovered.orders] == ["48222"]


def test_policy_lookup_normalizes_product_aliases_without_crashing() -> None:
    """Keep model-generated category variants recoverable inside the trace."""
    result = MockCommerceStore().get_return_policy("tote")

    assert result.found is True
    assert result.policy is not None
    assert result.policy.category == "accessories"


def test_agent_input_is_replay_safe_and_does_not_include_expected_action() -> None:
    """Unwrap imported inputs without exposing fixture labels to the agent."""
    ticket = CASES[0].ticket
    imported = {
        "schema_version": 1,
        "turns": [{"source_trace_id": "trace-1", "inputs": ticket.model_dump()}],
    }

    assert get_ticket_input(imported) == ticket
    prompt = build_prompt(ticket)
    assert ticket.body in prompt
    assert "expected_action" not in prompt


def test_baseline_agent_exposes_the_mock_commerce_tools() -> None:
    """Keep the example trace graph focused on investigation and terminal actions."""
    agent = build_agent(MockCommerceStore(), "test")

    assert set(agent._function_toolset.tools) == {
        "lookup_order",
        "get_return_policy",
        "check_shipping",
        "issue_refund",
        "create_replacement",
        "escalate_to_human",
    }


def test_strict_agent_instructions_require_approval_before_refunds() -> None:
    """Make the second agent version inspect approval and risk rules itself."""
    baseline = get_instructions()
    strict = get_instructions(strict_policy=True)

    assert "Do not assume that an action tool enforces" not in baseline
    assert "Do not assume that an action tool enforces" in strict
    assert "Assume the action tools enforce" in baseline
    assert "Assume the action tools enforce" not in strict
    assert "human approval threshold" in strict
    assert "risk flag" in strict


@pytest.mark.parametrize(
    ("ticket_id", "expected_action", "expected_amount"),
    [
        ("ticket-001", ResolutionAction.REFUND, Decimal("98.00")),
        ("ticket-004", ResolutionAction.ESCALATE, None),
        ("ticket-007", ResolutionAction.ESCALATE, None),
        ("ticket-009", ResolutionAction.REFUND, Decimal("80.00")),
        ("ticket-010", ResolutionAction.REFUND, Decimal("98.00")),
    ],
)
def test_ci_model_replays_target_and_control_cases_without_provider_calls(
    ticket_id: str,
    expected_action: ResolutionAction,
    expected_amount: Decimal | None,
) -> None:
    """Exercise the target and control paths through real example tools."""
    ticket = next(case.ticket for case in CASES if case.ticket.ticket_id == ticket_id)
    store = MockCommerceStore()
    model = _build_ci_model(ticket, strict_policy=True)

    result = build_agent(store, model, strict_policy=True).run_sync(
        build_prompt(ticket)
    )

    assert result.output.action is expected_action
    assert result.output.amount == expected_amount
    assert len(store.actions) == 1
    assert store.actions[0].accepted is True
    assert store.actions[0].action is expected_action


def _load_documented_evaluator() -> dict[str, Any]:
    """Load the evaluator implementation taught in the manual walkthrough."""
    readme = (EXAMPLE_DIR / "README.md").read_text()
    marker = '```python\n# /// script\n# requires-python = ">=3.11"'
    source = (
        '# /// script\n# requires-python = ">=3.11"'
        + readme.split(marker, maxsplit=1)[1].split("```", maxsplit=1)[0]
    )
    namespace: dict[str, Any] = {}
    exec(compile(source, "documented_evaluator.py", "exec"), namespace)
    return namespace


def _create_tool_node(
    tool_name: str, *, amount: str | None = None
) -> SessionNodeResponse:
    """Create one accepted terminal tool node for the documented evaluator."""
    return SessionNodeResponse.model_construct(
        id=uuid.uuid4(),
        session_id=uuid.uuid4(),
        node_type=NodeType.TOOL_CALL,
        tool_name=tool_name,
        outputs={"accepted": True, "amount": amount},
    )


def test_documented_evaluator_catches_conflicting_actions() -> None:
    """Keep the manual evaluator aligned with the reviewed trace behavior."""
    evaluate = _load_documented_evaluator()["evaluate"]
    passing = SessionView(
        session=SessionResponse.model_construct(
            inputs={"ticket_id": "ticket-001"},
            outputs={"action": "refund", "amount": "98.00"},
        ),
        nodes=[_create_tool_node("issue_refund", amount="98.00")],
    )
    conflicting = SessionView(
        session=SessionResponse.model_construct(
            inputs={"ticket_id": "ticket-007"},
            outputs={"action": "escalate", "amount": None},
        ),
        nodes=[
            _create_tool_node("issue_refund", amount="120.00"),
            _create_tool_node("escalate_to_human"),
        ],
    )

    assert evaluate(passing).passed is True
    assert evaluate(conflicting).passed is False


def test_documented_evaluator_rejects_missing_tool_evidence() -> None:
    """Keep missing evidence explicit in the manual evaluator."""
    evaluate = _load_documented_evaluator()["evaluate"]
    view = SessionView(
        session=SessionResponse.model_construct(
            inputs={"ticket_id": "ticket-001"},
            outputs={"action": "refund", "amount": "98.00"},
        ),
        nodes=[],
    )

    with pytest.raises(ValueError, match="tool-call evidence"):
        evaluate(view)


def test_checked_in_langfuse_export_contains_replayable_tool_traces() -> None:
    """Keep one imported baseline session per ticket with LLM and tool nodes."""
    evaluate = _load_documented_evaluator()["evaluate"]
    sessions = [
        item
        for item in parse(
            TRACE_PATH.read_bytes(),
            {"source_instance": "canonical-returns-example"},
        )
        if isinstance(item, ImportedSession)
    ]

    assert len(sessions) == len(CASES)
    assert {session.name for session in sessions} == {
        f"Returns ticket: {case.ticket.ticket_id}" for case in CASES
    }
    assert {
        session.inputs["turns"][-1]["inputs"]["ticket_id"] for session in sessions
    } == {case.ticket.ticket_id for case in CASES}
    expected_actions = {
        case.ticket.ticket_id: case.expected_action.value for case in CASES
    }
    mismatches = {
        session.inputs["turns"][-1]["inputs"]["ticket_id"]
        for session in sessions
        if session.outputs["action"]
        != expected_actions[session.inputs["turns"][-1]["inputs"]["ticket_id"]]
    }
    assert mismatches == {"ticket-004", "ticket-007"}
    policy_failures = {
        session.inputs["turns"][-1]["inputs"]["ticket_id"]
        for session in sessions
        if not evaluate(
            SessionView(
                session=SessionResponse.model_construct(
                    inputs=session.inputs,
                    outputs=session.outputs,
                ),
                nodes=[
                    SessionNodeResponse.model_construct(
                        node_type=node.node_type,
                        tool_name=node.tool_name,
                        outputs=node.outputs,
                    )
                    for node in flatten_nodes(session.nodes)
                ],
            )
        ).passed
    }
    assert policy_failures == mismatches
    for session in sessions:
        nodes = flatten_nodes(session.nodes)
        assert any(node.node_type is NodeType.LLM_CALL for node in nodes)
        assert any(node.node_type is NodeType.TOOL_CALL for node in nodes)


def test_trace_generator_uses_real_model_and_langfuse_credentials() -> None:
    """Keep generation separate from Kitaru resource creation."""
    script = (EXAMPLE_DIR / "generate.sh").read_text()
    generator = (EXAMPLE_DIR / "generate_traces.py").read_text()

    assert "--with-editable" in script
    assert "plugins/packages/pydantic-ai[openai]" in script
    assert "--extra examples" in script
    assert "langfuse-traces.jsonl" in script
    assert "Agent.instrument_all()" in generator
    assert 'trace_name=f"Returns ticket: {ticket.ticket_id}"' in generator
    assert "kitaru session import" not in script


def test_replay_defaults_to_the_model_used_by_checked_in_traces() -> None:
    """Keep default latency comparisons on the same model."""
    agent_source = (EXAMPLE_DIR / "agent.py").read_text()

    assert 'os.environ.get("BASELINE_MODEL", "openai:gpt-5-nano")' in agent_source


def test_readme_teaches_the_complete_returns_improvement_loop() -> None:
    """Teach import, evaluation, cohorting, improvement, replay, and comparison."""
    readme = (EXAMPLE_DIR / "README.md").read_text()

    assert "source .env" in readme
    assert "--env-file .env" not in readme
    assert "$TICKET_004_SESSION_ID:outcome=" in readme
    assert "--question-key outcome" in readme
    assert '--selector "{\\"node_id\\":\\"$TICKET_004_REFUND_NODE_ID\\"}"' in readme
    assert "kitaru investigation session verdict" in readme
    assert '"judgment":"problematic"' not in readme
    assert "The investigation-session verdict is the classification." in readme
    assert (
        "uv pip install --editable '../../plugins/packages/pydantic-ai[openai]'"
        in readme
    )
    assert "scripts/smoke_plugin_artifacts.py" in readme
    assert "UV_FIND_LINKS" in readme

    for command in (
        "kitaru login --local",
        "--importer kitaru/langfuse@latest",
        "kitaru importer list",
        "kitaru evaluator list",
        "kitaru agent register",
        "kitaru worker start",
        "kitaru session import",
        "kitaru session list",
        "kitaru session evaluate",
        "kitaru evaluation list",
        "kitaru cohort create unsafe-refund-baseline",
        "kitaru cohort create safe-refund-control",
        "--cohort unsafe-refund-baseline@1",
        "--cohort safe-refund-control@1",
        "kitaru evaluator scaffold",
        "kitaru evaluator test",
        "kitaru evaluator register",
        "--evaluator returns-policy@1",
        "kitaru agent version register",
        '--command "python -m examples.pydantic_ai_ticket_resolver.agent"',
        "RETURNS_POLICY_MODE=strict",
        "kitaru experiment create",
        "--agent returns-resolver",
        "kitaru experiment run start",
        "kitaru experiment run list",
        "kitaru experiment run get",
        "kitaru experiment run jobs",
        "--origin replay",
    ):
        assert command in readme
    assert "--agent returns-resolver@1" in readme
    assert "--tag returns-baseline" in readme
    assert "--evaluator kitaru/cost@latest" in readme
    assert "--evaluator kitaru/latency@latest" in readme
    assert "--evaluator kitaru/tool-call-patterns@latest" in readme
    assert "TICKET_004_SESSION_ID" in readme
    assert "TICKET_007_SESSION_ID" in readme
    assert "TICKET_010_SESSION_ID" in readme
    assert "TARGET_COHORT_VERSION_ID" in readme
    assert "CONTROL_COHORT_VERSION_ID" in readme
    assert "cohort version get unsafe-refund-baseline@1" in readme
    assert "cohort version get safe-refund-control@1" in readme
    assert "jq -r '.item.id'" in readme
    assert '--cohort-version "$TARGET_COHORT_VERSION_ID"' in readme
    assert '--cohort-version "$CONTROL_COHORT_VERSION_ID"' in readme
    assert "returns-resolver@2" in readme
    assert "policy_correct" in readme
    assert "The starter example does not contain `evaluator.py`" in readme
    assert "accepted terminal tool call" in readme
    assert "Which agent outcome matters most?" in readme
    assert "Which successful cases must remain correct?" in readme


def test_coding_agent_readme_delegates_evaluator_authoring() -> None:
    """Keep evaluator code out of the user's coding-agent responsibilities."""
    readme = (EXAMPLE_DIR / "README_AGENT_GUIDED.md").read_text()

    assert "You do not need to know Kitaru terminology" in readme
    assert "does not contain `evaluator.py`" in readme
    assert "connected Kitaru MCP server" in readme
    assert "create, test, and register the evaluator" in readme
    assert "You do not select IDs" in readme
    assert "stores the answer as an annotation" in readme
    assert "exact node selector" in readme
    assert "accepted terminal tool calls" in readme
    assert "counterexamples" in readme


def test_trace_export_has_no_real_email_domains() -> None:
    """Prevent accidental customer data from entering the checked-in trace corpus."""
    for line in TRACE_PATH.read_text().splitlines():
        trace = json.loads(line)
        assert "@example.test" in json.dumps(trace)
        assert "@gmail.com" not in json.dumps(trace)
        assert "expected_action" not in json.dumps(trace)
