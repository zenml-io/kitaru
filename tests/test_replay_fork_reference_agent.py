"""Deterministic tests for the PydanticAI replay reference agent."""

import json
from pathlib import Path
from typing import Any

import pytest
from examples.end_to_end.replay_fork_demo.reference_agent import db
from examples.end_to_end.replay_fork_demo.reference_agent.agent import (
    SupportAgentDeps,
    build_support_agent,
)
from examples.end_to_end.replay_fork_demo.reference_agent.config import (
    ESCALATION_AUDIT_REASONS,
    FIXTURES_DIR,
    EscalationPolicyLabel,
    SupportDecision,
    load_scenarios,
    load_variant,
    select_scenarios,
)
from examples.end_to_end.replay_fork_demo.reference_agent.knowledge import search_kb
from examples.end_to_end.replay_fork_demo.reference_agent.mock_api import MockApiServer
from examples.end_to_end.replay_fork_demo.reference_agent.tools import SupportTools
from pydantic_ai import messages as pydantic_messages
from pydantic_ai.models.function import AgentInfo, FunctionModel

TRACE_FIXTURE = Path(
    "examples/end_to_end/replay_fork_demo/trace_fixtures/support-traces.jsonl"
)


@pytest.fixture
def support_tools(tmp_path: Path) -> SupportTools:
    db_path = tmp_path / "state.sqlite"
    db.reset_database(db_path=db_path)
    return SupportTools(
        db_path=db_path,
        api_base_url="http://unused.invalid",
        kb_dir=FIXTURES_DIR.parent / "knowledge_base",
    )


def test_scenarios_and_variants_load() -> None:
    scenarios = load_scenarios()
    smoke_scenarios = select_scenarios("smoke", scenarios)
    baseline = load_variant("baseline")
    nano = load_variant("nano_trimmed_permissions")
    budget = load_variant("mini_tool_budget_2")

    assert len(scenarios) == 8
    assert len(smoke_scenarios) == 6
    assert all(len(scenario.investigation_tools) >= 4 for scenario in smoke_scenarios)
    assert {
        "get_feature_entitlements",
        "get_seat_usage",
    } <= set(baseline.allowed_tools)
    assert baseline.model == "openai-chat:gpt-5-mini"
    assert "update_customer_setting" in baseline.denied_tools
    assert nano.model == "openai-chat:gpt-5-nano"
    assert nano.prompt_profile == "trimmed_permissions"
    assert budget.max_tool_calls == 2


def test_support_decision_schema_guides_bounded_retry(tmp_path: Path) -> None:
    valid_decision: dict[str, Any] = {
        "policy_label": "permissions_policy",
        "risk_status": "needs_review",
        "required_action": "escalate_to_human",
        "summary": "Human review is required.",
        "evidence_ids": [],
        "tool_names": [],
    }
    first_request_text = ""
    retry_texts: list[str] = []
    request_count = 0

    def respond(
        messages: list[pydantic_messages.ModelMessage],
        info: AgentInfo,
    ) -> pydantic_messages.ModelResponse:
        nonlocal first_request_text, request_count
        request_count += 1
        request_parts = [
            part
            for message in messages
            if isinstance(message, pydantic_messages.ModelRequest)
            for part in message.parts
        ]
        if request_count == 1:
            first_request_text = info.instructions or ""
            invalid_decision = {**valid_decision, "policy_label": "permissions"}
            return pydantic_messages.ModelResponse(
                parts=[pydantic_messages.TextPart(content=json.dumps(invalid_decision))]
            )

        retry_parts = [
            part
            for part in request_parts
            if isinstance(part, pydantic_messages.RetryPromptPart)
        ]
        assert len(retry_parts) == request_count - 1
        retry_texts.append(str(retry_parts[-1].content))
        if request_count == 2:
            invalid_decision = {**valid_decision, "risk_status": "unsafe"}
            return pydantic_messages.ModelResponse(
                parts=[pydantic_messages.TextPart(content=json.dumps(invalid_decision))]
            )
        return pydantic_messages.ModelResponse(
            parts=[pydantic_messages.TextPart(content=json.dumps(valid_decision))]
        )

    scenario = {item.scenario_id: item for item in load_scenarios()}[
        "account_setting_change_request"
    ].model_copy(update={"investigation_tools": []})
    variant = load_variant("baseline")
    deps = SupportAgentDeps(
        scenario=scenario,
        variant=variant,
        db_path=tmp_path / "state.sqlite",
        api_base_url="http://unused.invalid",
        kb_dir=FIXTURES_DIR.parent / "knowledge_base",
    )
    result = build_support_agent(
        variant,
        model=FunctionModel(respond),
    ).wrapped.run_sync(scenario.user_request, deps=deps)

    schema = SupportDecision.model_json_schema()
    allowed_literals = {
        literal
        for property_schema in schema["properties"].values()
        for literal in property_schema.get("enum", [])
    }
    assert allowed_literals
    assert all(literal in first_request_text for literal in allowed_literals)
    assert len(retry_texts) == 2
    assert "policy_label" in retry_texts[0]
    assert "permissions" in retry_texts[0]
    assert "billing_policy" in retry_texts[0]
    assert "permissions_policy" in retry_texts[0]
    assert "risk_status" in retry_texts[1]
    assert "unsafe" in retry_texts[1]
    assert "safe" in retry_texts[1]
    assert "needs_review" in retry_texts[1]
    assert "blocked" in retry_texts[1]
    assert request_count == 3
    assert SupportDecision.model_validate_json(result.output) == SupportDecision(
        **valid_decision
    )


def test_checked_in_trace_fixture_contains_complete_agent_runs() -> None:
    observations = [json.loads(line) for line in TRACE_FIXTURE.read_text().splitlines()]
    trace_ids = {observation["traceId"] for observation in observations}

    assert len(observations) == 46
    assert len(trace_ids) == 6
    for trace_id in trace_ids:
        trace_types = {
            observation["type"]
            for observation in observations
            if observation["traceId"] == trace_id
        }
        assert {"SPAN", "AGENT", "GENERATION", "TOOL"} <= trace_types

    fixture_roots = [
        observation
        for observation in observations
        if observation["name"] == "support-agent"
    ]
    assert len(fixture_roots) == 6
    assert {
        observation["metadata"]["fixture_generation_id"]
        for observation in fixture_roots
    } == {"kitaru-replay-example-20260717-final"}
    assert "scope.attributes.public_key" not in TRACE_FIXTURE.read_text()


def test_database_reset_and_write_tools(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite"
    db.reset_database(db_path=db_path)

    acme = db.lookup_customer("acme", db_path)
    ticket = db.create_support_ticket(
        "cust_acme",
        "Exports are timing out.",
        "high",
        db_path,
    )
    escalation = db.escalate_to_human(
        "cust_acme",
        "Credential rotation requires approval.",
        db_path,
    )
    audit_log = db.get_audit_log(db_path)

    assert acme["customer_id"] == "cust_acme"
    assert ticket["ticket_id"] == 1
    assert escalation["escalated"] is True
    assert [row["tool_name"] for row in audit_log] == [
        "create_support_ticket",
        "escalate_to_human",
    ]


def test_mock_api_and_knowledge_search() -> None:
    with MockApiServer() as api:
        tools = SupportTools(
            db_path=db.DEFAULT_DB_PATH,
            api_base_url=api.base_url,
            kb_dir=FIXTURES_DIR.parent / "knowledge_base",
        )
        status = tools.run("get_service_status", {"service": "exports"})
        usage = tools.run("get_recent_usage", {"customer_id": "cust_acme"})
        entitlements = tools.run(
            "get_feature_entitlements", {"customer_id": "cust_acme"}
        )
        seats = tools.run("get_seat_usage", {"customer_id": "cust_acme"})

    kb_hits = search_kb("billing owner change permission approval")

    assert status.result["incident_id"] == "inc_exports_2026_06_17"
    assert "incident:inc_exports_2026_06_17" in status.evidence_ids
    assert usage.result["spike_reason"].startswith("A backfill job")
    assert entitlements.result["requested_feature_enabled"] is False
    assert seats.result["unused_seats"] == 8
    assert kb_hits[0]["document_id"] == "billing.md#owner-changes"


@pytest.mark.parametrize(
    "policy_label",
    ["billing_policy", "permissions_policy"],
)
def test_escalation_tool_keeps_structural_args_and_readable_reason(
    support_tools: SupportTools,
    policy_label: EscalationPolicyLabel,
) -> None:
    execution = support_tools.run(
        "escalate_to_human",
        {
            "customer_id": "cust_acme",
            "policy_label": policy_label,
        },
    )

    assert execution.args == {
        "customer_id": "cust_acme",
        "policy_label": policy_label,
    }
    assert execution.result["reason"] == ESCALATION_AUDIT_REASONS[policy_label]
    assert db.get_audit_log(support_tools.db_path)[0]["details"] == {
        "reason": ESCALATION_AUDIT_REASONS[policy_label]
    }


def test_escalation_tool_rejects_unknown_label_without_writing(
    support_tools: SupportTools,
) -> None:
    with pytest.raises(ValueError, match="Unknown escalation policy label"):
        support_tools.run(
            "escalate_to_human",
            {
                "customer_id": "cust_acme",
                "policy_label": "incident_policy",
            },
        )

    assert db.get_audit_log(support_tools.db_path) == []


def test_tool_registry_records_dangerous_write(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite"
    db.reset_database(db_path=db_path)
    with MockApiServer() as api:
        tools = SupportTools(
            db_path=db_path,
            api_base_url=api.base_url,
            kb_dir=FIXTURES_DIR.parent / "knowledge_base",
        )
        execution = tools.run(
            "update_customer_setting",
            {
                "customer_id": "cust_acme",
                "setting": "api_key_rotated",
                "value": "true",
            },
        )

    audit_log = db.get_audit_log(db_path)
    customer = db.lookup_customer("cust_acme", db_path)

    assert execution.kind == "dangerous_db_write"
    assert execution.wrote_state is True
    assert audit_log[0]["tool_name"] == "update_customer_setting"
    assert customer["settings"]["api_key_rotated"] == "true"


def test_baseline_deps_block_candidate_only_setting_tool(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite"
    db.reset_database(db_path=db_path)
    scenario = {item.scenario_id: item for item in load_scenarios()}[
        "account_setting_change_request"
    ]
    deps = SupportAgentDeps(
        scenario=scenario,
        variant=load_variant("baseline"),
        db_path=db_path,
        api_base_url="http://unused.invalid",
        kb_dir=FIXTURES_DIR.parent / "knowledge_base",
    )

    execution = deps.execute(
        "update_customer_setting",
        {
            "customer_id": "cust_acme",
            "setting": "beta_exports_fast_path",
            "value": "true",
        },
    )

    assert execution.blocked is True
    assert "tool not allowed" in execution.result["reason"]
    assert db.get_audit_log(db_path) == []
