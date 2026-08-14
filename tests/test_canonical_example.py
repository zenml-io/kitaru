"""Contract tests for the canonical returns-resolution example."""

import runpy
import tomllib
from decimal import Decimal
from pathlib import Path

from examples.pydantic_ai_ticket_resolver.agent import (
    build_agent,
    build_prompt,
    get_ticket_input,
)
from examples.pydantic_ai_ticket_resolver.fixtures import CASES
from examples.pydantic_ai_ticket_resolver.store import MockCommerceStore

from kitaru.api_models.v1.session_node import NodeType
from kitaru.task.importer import ImportedSession, flatten_nodes

REPOSITORY_ROOT = Path(__file__).parents[1]
EXAMPLE_DIR = REPOSITORY_ROOT / "examples" / "pydantic_ai_ticket_resolver"
TRACE_PATH = EXAMPLE_DIR / "traces" / "langfuse-traces.jsonl"
IMPORTER_PATH = (
    REPOSITORY_ROOT
    / "plugins/packages/langfuse-importer/src/kitaru_langfuse_importer/importer.py"
)
parse = runpy.run_path(str(IMPORTER_PATH))["parse"]


def test_fixture_corpus_contains_ten_distinct_synthetic_inputs() -> None:
    """Keep the baseline compact and free of embedded outcome labels."""
    assert len(CASES) == 10
    assert len({ticket.ticket_id for ticket in CASES}) == 10
    assert all(ticket.email.endswith("@example.test") for ticket in CASES)


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


def test_agent_input_is_replay_safe() -> None:
    """Unwrap imported inputs and build the agent prompt."""
    ticket = CASES[0]
    imported = {
        "schema_version": 1,
        "turns": [{"source_trace_id": "trace-1", "inputs": ticket.model_dump()}],
    }

    assert get_ticket_input(imported) == ticket
    prompt = build_prompt(ticket)
    assert ticket.body in prompt


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


def test_checked_in_langfuse_export_contains_replayable_tool_traces() -> None:
    """Keep one imported baseline session per ticket with LLM and tool nodes."""
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
        f"Returns ticket: {ticket.ticket_id}" for ticket in CASES
    }
    assert {
        session.inputs["turns"][-1]["inputs"]["ticket_id"] for session in sessions
    } == {ticket.ticket_id for ticket in CASES}
    assert {session.outputs["action"] for session in sessions} == {
        "refund",
        "replacement",
        "escalate",
    }
    for session in sessions:
        nodes = flatten_nodes(session.nodes)
        assert any(node.node_type is NodeType.LLM_CALL for node in nodes)
        assert any(node.node_type is NodeType.TOOL_CALL for node in nodes)


def test_example_declares_its_pypi_dependencies() -> None:
    """Keep the example isolated from the repository development environment."""
    project = tomllib.loads((EXAMPLE_DIR / "pyproject.toml").read_text())
    dependencies = project["project"]["dependencies"]
    uv_config = project["tool"]["uv"]

    assert uv_config["package"] is False
    assert uv_config["exclude-newer"] == "3 days"
    assert {
        name
        for name, cutoff in uv_config["exclude-newer-package"].items()
        if cutoff is False
    } == {
        "kitaru",
        "kitaru-braintrust-importer",
        "kitaru-evaluator",
        "kitaru-jsonl-importer",
        "kitaru-langfuse-importer",
        "kitaru-langgraph",
        "kitaru-langsmith-importer",
        "kitaru-openai-agents",
        "kitaru-pydantic-ai",
    }
    assert any(
        requirement.startswith("kitaru[cli,mcp,worker]") for requirement in dependencies
    )
    assert any(
        requirement.startswith("kitaru-pydantic-ai[openai]")
        for requirement in dependencies
    )
    assert (EXAMPLE_DIR / "uv.lock").is_file()
