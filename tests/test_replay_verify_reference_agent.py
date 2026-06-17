"""Deterministic tests for the Replay Verify reference-agent example."""

import json
from pathlib import Path

from examples.end_to_end.replay_verify_reference_agent import db
from examples.end_to_end.replay_verify_reference_agent.config import (
    FIXTURES_DIR,
    load_scenarios,
    load_variant,
    select_scenarios,
)
from examples.end_to_end.replay_verify_reference_agent.knowledge import search_kb
from examples.end_to_end.replay_verify_reference_agent.mock_api import MockApiServer
from examples.end_to_end.replay_verify_reference_agent.tools import SupportTools


def test_scenarios_and_variants_load() -> None:
    scenarios = load_scenarios()
    smoke_scenarios = select_scenarios("smoke", scenarios)
    baseline = load_variant("baseline")
    nano = load_variant("nano_trimmed_permissions")
    budget = load_variant("mini_tool_budget_2")

    assert len(scenarios) == 8
    assert len(smoke_scenarios) == 6
    assert baseline.model == "gpt-5-mini"
    assert "update_customer_setting" in baseline.denied_tools
    assert nano.model == "gpt-5-nano"
    assert nano.prompt_profile == "trimmed_permissions"
    assert budget.max_tool_calls == 2


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

    kb_hits = search_kb("billing owner change permission approval")

    assert status.result["incident_id"] == "inc_exports_2026_06_17"
    assert "incident:inc_exports_2026_06_17" in status.evidence_ids
    assert usage.result["spike_reason"].startswith("A backfill job")
    assert kb_hits[0]["document_id"] == "billing.md#owner-changes"


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


def test_trace_manifest_placeholder_is_parseable() -> None:
    manifest_path = FIXTURES_DIR / "trace_generation_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert manifest["agent_version"] == "replay-verify-reference-agent-stage-1"
    assert isinstance(manifest["runs"], list)
