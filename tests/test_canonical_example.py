"""Contract tests for the canonical returns-resolution example."""

import json
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


def test_agent_input_is_replay_safe_and_does_not_include_expected_action() -> None:
    """Unwrap imported inputs without exposing fixture labels to the agent."""
    ticket = CASES[0]
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


def test_trace_generator_uses_real_model_and_langfuse_credentials() -> None:
    """Keep generation separate from Kitaru resource creation."""
    script = (EXAMPLE_DIR / "generate.sh").read_text()
    generator = (EXAMPLE_DIR / "generate_traces.py").read_text()

    assert '--project "${example_dir}"' in script
    assert "--with-editable" not in script
    assert "--extra" not in script
    assert "langfuse-traces.jsonl" in script
    assert "Agent.instrument_all()" in generator
    assert 'trace_name=f"Returns ticket: {ticket.ticket_id}"' in generator
    assert "case.scenario" not in generator
    assert 'tags=["returns-resolution", "kitaru-example"]' in generator
    assert "kitaru session import" not in script


def test_replay_defaults_to_the_model_used_by_checked_in_traces() -> None:
    """Keep default latency comparisons on the same model."""
    agent_source = (EXAMPLE_DIR / "agent.py").read_text()

    assert 'os.environ.get("BASELINE_MODEL", "openai:gpt-5-nano")' in agent_source


def test_public_example_has_no_embedded_investigation_answer_key() -> None:
    """Keep fixture labels and deterministic oracles outside the public example."""
    public_source = "\n".join(
        (EXAMPLE_DIR / name).read_text()
        for name in ("agent.py", "fixtures.py", "generate_traces.py", "models.py")
    )

    for token in (
        "expected_action",
        "case.scenario",
        "_build_ci_model",
        "RETURNS_POLICY_MODE",
        "KITARU_EXAMPLE_TEST_MODEL",
    ):
        assert token not in public_source


def test_readme_teaches_unbiased_skill_and_manual_workflows() -> None:
    """Teach evidence-led investigation and replay without an answer key."""
    readme = (EXAMPLE_DIR / "README.md").read_text()

    assert "source .env" in readme
    assert "--env-file .env" not in readme
    assert "npx skills add zenml-io/kitaru-skills" in readme
    assert "kitaru-investigation" in readme
    assert "kitaru-replay-experiment" in readme
    assert "--question-key observation" in readme
    assert '\\"path\\":\\"/outputs/message\\"' in readme
    assert '\\"span\\":{\\"start\\":0,\\"end\\":40}' in readme
    assert "kitaru investigation session verdict" in readme
    assert "kitaru investigation update" in readme
    assert "answer coverage" in readme
    assert "verdict coverage" in readme
    assert "uv sync" in readme
    assert "uv pip install" not in readme
    assert "kitaru login --local" in readme
    assert "kitaru login https://your-kitaru-workspace.example.com" in readme
    assert "plugins/packages/pydantic-ai" not in readme
    assert "scripts/smoke_plugin_artifacts.py" not in readme
    assert "UV_FIND_LINKS" not in readme

    for command in (
        "kitaru login --local",
        "--importer kitaru/langfuse@latest",
        "kitaru importer get kitaru/langfuse",
        "kitaru evaluator list",
        "kitaru agent register",
        "kitaru worker start",
        "kitaru session import",
        "kitaru session list",
        "kitaru session evaluate",
        "kitaru evaluation list",
        "kitaru investigation create returns-discovery",
        "kitaru annotation create",
        "kitaru cohort create returns-regression",
        "--cohort returns-regression@1",
        "kitaru evaluator scaffold",
        "kitaru evaluator test",
        "kitaru evaluator register",
        "--evaluator returns-behavior@1",
        "kitaru agent version register",
        '--command "python -m examples.pydantic_ai_ticket_resolver.agent"',
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
    assert "--evaluator kitaru/session-diagnostics@latest" in readme
    assert "--evaluator kitaru/tool-health@latest" in readme
    assert "--evaluator kitaru/trajectory-signals@latest" in readme
    assert "--evaluator kitaru/llm-call-signals@latest" in readme
    assert "--evaluator kitaru/timing-profile@latest" in readme
    assert "COHORT_VERSION_ID" in readme
    assert "cohort version get returns-regression@1" in readme
    assert "jq -r '.item.id'" in readme
    assert '--cohort-version "$COHORT_VERSION_ID"' in readme
    assert "returns-resolver@2" in readme
    assert "Do not map session or ticket identifiers to expected answers." in readme
    assert "REVIEWED_OUTCOMES" not in readme
    assert "ticket-00" not in readme
    assert "README_AGENT_GUIDED" not in readme


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
    assert any(requirement.startswith("langfuse") for requirement in dependencies)
    assert (EXAMPLE_DIR / "uv.lock").is_file()


def test_example_environment_does_not_override_the_login_target() -> None:
    """Let stored local or remote workspace login select the connection."""
    environment = (EXAMPLE_DIR / ".env.example").read_text()

    assert "KITARU_API_URL" not in environment
    assert "KITARU_API_KEY" not in environment


def test_example_has_one_walkthrough_readme() -> None:
    """Keep the skill-guided and manual routes in one user-facing document."""
    readme = (EXAMPLE_DIR / "README.md").read_text()

    assert not (EXAMPLE_DIR / "README_AGENT_GUIDED.md").exists()
    assert "Recommended route: use the Kitaru skills" in readme
    assert "Manual route: operate the evidence loop yourself" in readme
    assert "Persist my answers as annotations" in readme
    assert "supporting sessions" in readme
    assert "counterexamples" in readme


def test_trace_export_has_no_real_email_domains() -> None:
    """Prevent accidental customer data from entering the checked-in trace corpus."""
    for line in TRACE_PATH.read_text().splitlines():
        trace = json.loads(line)
        assert "@example.test" in json.dumps(trace)
        assert "@gmail.com" not in json.dumps(trace)
        assert "expected_action" not in json.dumps(trace)
        assert "scenario" not in json.dumps(trace)
