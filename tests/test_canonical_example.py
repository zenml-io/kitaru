"""Contract tests for the canonical returns-resolution example."""

import json
from decimal import Decimal
from pathlib import Path

from examples.canonical_example.agent import (
    build_agent,
    build_prompt,
    get_instructions,
    get_ticket_input,
)
from examples.canonical_example.evaluator import evaluate
from examples.canonical_example.fixtures import CASES
from examples.canonical_example.models import ResolutionAction
from examples.canonical_example.store import MockCommerceStore

from kitaru.api_models.v1.session import SessionResponse
from kitaru.api_models.v1.session_node import NodeType
from kitaru.importers.langfuse import parse
from kitaru.task.evaluator import SessionView
from kitaru.task.importer import ParsedSession, flatten_nodes

REPOSITORY_ROOT = Path(__file__).parents[1]
EXAMPLE_DIR = REPOSITORY_ROOT / "examples" / "canonical_example"
TRACE_PATH = EXAMPLE_DIR / "traces" / "langfuse-traces.jsonl"


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


def test_policy_evaluator_scores_reviewed_actions() -> None:
    """Score native and imported session shapes with the same rubric."""
    passing = SessionView(
        session=SessionResponse.model_construct(
            inputs={"ticket_id": "ticket-001"}, outputs={"action": "refund"}
        ),
        nodes=[],
    )
    failing = SessionView(
        session=SessionResponse.model_construct(
            inputs={"turns": [{"inputs": {"ticket_id": "ticket-007"}}]},
            outputs={"action": "refund"},
        ),
        nodes=[],
    )

    pass_result = evaluate(passing)
    fail_result = evaluate(failing)

    assert pass_result.score is True
    assert pass_result.passed is True
    assert fail_result.score is False
    assert fail_result.passed is False
    assert "expected escalate, observed refund" in fail_result.explanation


def test_checked_in_langfuse_export_contains_replayable_tool_traces() -> None:
    """Keep one imported baseline session per ticket with LLM and tool nodes."""
    sessions = [
        item
        for item in parse(
            TRACE_PATH.read_bytes(),
            {"source_instance": "canonical-returns-example"},
        )
        if isinstance(item, ParsedSession)
    ]

    assert len(sessions) == len(CASES)
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
                nodes=[],
            )
        ).passed
    }
    assert policy_failures == mismatches
    for session in sessions:
        nodes = flatten_nodes(session.nodes)
        assert any(node.node_type is NodeType.LLM_CALL for node in nodes)
        assert any(node.node_type is NodeType.TOOL_CALL for node in nodes)
        assert session.metadata["replay_readiness"]["level"] == "ready"


def test_trace_generator_uses_real_model_and_langfuse_credentials() -> None:
    """Keep generation separate from Kitaru resource creation."""
    script = (EXAMPLE_DIR / "generate.sh").read_text()
    generator = (EXAMPLE_DIR / "generate_traces.py").read_text()

    assert "--extra pydantic-ai" in script
    assert "--extra examples" in script
    assert "langfuse-traces.jsonl" in script
    assert "Agent.instrument_all()" in generator
    assert "kitaru session import" not in script


def test_readme_teaches_the_complete_returns_improvement_loop() -> None:
    """Teach import, evaluation, cohorting, improvement, replay, and comparison."""
    readme = (EXAMPLE_DIR / "README.md").read_text()

    for command in (
        "kitaru login --local",
        "python ../../scripts/seed_default_plugins.py",
        "kitaru importer list",
        "kitaru evaluator list",
        "kitaru agent register",
        "kitaru worker start",
        "kitaru session import",
        "kitaru session list",
        "kitaru session evaluate",
        "kitaru evaluation list",
        "kitaru cohort create cheap-baseline",
        "kitaru cohort create expensive-baseline",
        "--cohort cheap-baseline@1",
        "--cohort expensive-baseline@1",
        "kitaru evaluator test",
        "kitaru evaluator register",
        "--evaluator returns-policy@1",
        "kitaru agent version register",
        "RETURNS_POLICY_MODE=strict",
        "kitaru experiment create",
        "kitaru experiment run start",
        "kitaru experiment run list",
        "kitaru experiment run get",
        "kitaru experiment run jobs",
        "--origin replay",
    ):
        assert command in readme
    assert "--agent returns-resolver@1" in readme
    assert "--tag returns-baseline" in readme
    assert '"field":"name","op":"eq","value":"cost"' in readme
    assert "--evaluator cost@latest" in readme
    assert "--evaluator latency@latest" in readme
    assert "--evaluator tool-call-patterns@latest" in readme
    assert "CHEAP_SESSION_ID_3" in readme
    assert "EXPENSIVE_SESSION_ID_3" in readme
    assert "CHEAP_COHORT_VERSION_ID" in readme
    assert "EXPENSIVE_COHORT_VERSION_ID" in readme
    assert "cohort version get cheap-baseline@1" in readme
    assert "cohort version get expensive-baseline@1" in readme
    assert "jq -r '.item.id'" in readme
    assert "--cohort-version \"$CHEAP_COHORT_VERSION_ID\"" in readme
    assert "--cohort-version \"$EXPENSIVE_COHORT_VERSION_ID\"" in readme
    assert "returns-resolver@2" in readme
    assert "policy_correct" in readme


def test_trace_export_has_no_real_email_domains() -> None:
    """Prevent accidental customer data from entering the checked-in trace corpus."""
    for line in TRACE_PATH.read_text().splitlines():
        trace = json.loads(line)
        assert "@example.test" in json.dumps(trace)
        assert "@gmail.com" not in json.dumps(trace)
        assert "expected_action" not in json.dumps(trace)
