"""The only domain glue: wrap the bundled (pydantic-typed) reference agent.

A JSON-native agent wouldn't need any of this — `KitaruAdapter(graph)` would be
enough. This agent uses typed state (Scenario / AgentVariant / ToolExecution),
so we (1) build its compiled graph and (2) rehydrate the trace's recorded JSON
back into those types before the live tail re-runs.
"""
from __future__ import annotations

from typing import Any
from uuid import uuid4

from reference_agent import db
from reference_agent.config import (
    DEFAULT_AGENT_VERSION,
    EXAMPLE_DIR,
    load_scenarios,
    load_variant,
)
from reference_agent.graph import build_graph, run_reference_agent
from reference_agent.mock_api import MockApiServer
from reference_agent.tools import SupportTools, ToolExecution

# Node-level fork point: replay/fork re-run decide_action onward, live.
CUT = "decide_action"
# A permission-sensitive scenario — where a cheaper/looser fork is most likely
# to drift in a way that matters.
SCENARIO = "enterprise_permission_request"
VARIANT = "baseline"


def generate_trace(scenario_id: str = SCENARIO, variant_name: str = VARIANT) -> str:
    """Run the agent once with the Langfuse callback; return the trace id."""
    from langfuse import get_client
    from langfuse.langchain import CallbackHandler

    scenario = {s.scenario_id: s for s in load_scenarios()}[scenario_id]
    variant = load_variant(variant_name)
    langfuse = get_client()
    trace_id = uuid4().hex
    metadata = {
        "scenario_id": scenario.scenario_id,
        "case_id": scenario.case_id,
        "variant_name": variant.name,
        "agent_version": DEFAULT_AGENT_VERSION,
        "model": variant.model,
        "prompt_profile": variant.prompt_profile,
        "tool_policy_name": variant.tool_policy_name,
        "tool_selection_mode": "llm_tool_calling",
    }
    tags = ["kitaru", "replay-fork-demo", variant.name, scenario.scenario_id]
    handler = CallbackHandler()
    with MockApiServer() as api:
        db.reset_database()
        with langfuse.start_as_current_observation(
            as_type="span",
            name="reference-agent-scenario",
            input={"scenario_id": scenario.scenario_id, "user_request": scenario.user_request},
            metadata={**metadata, "tags": tags},
            trace_context={"trace_id": trace_id},
        ) as root:
            output = run_reference_agent(
                scenario=scenario, variant=variant, db_path=db.DEFAULT_DB_PATH,
                api_base_url=api.base_url, kb_dir=EXAMPLE_DIR / "knowledge_base",
                callbacks=[handler], metadata=metadata, tags=tags,
            )
            root.update(output=output)
    langfuse.flush()
    return trace_id


def graph() -> Any:
    """The compiled LangGraph agent (the live tail uses no tools, only the model)."""
    db.reset_database()
    tools = SupportTools(
        db_path=db.DEFAULT_DB_PATH,
        api_base_url="http://unused.local",
        kb_dir=EXAMPLE_DIR / "knowledge_base",
    )
    return build_graph(tools=tools, callbacks=[], metadata={}, tags=[])


def rehydrate(case: Any) -> tuple[dict, dict]:
    """(root_state, node_outputs) in the typed shapes the real nodes expect."""
    cfg = case.trace_contract.raw_config
    scenario = {s.scenario_id: s for s in load_scenarios()}[cfg["scenario_id"]]
    variant = load_variant(cfg["variant_name"])

    stashed = case.raw_source_payload.get("langgraph_node_outputs", {})
    node_outputs: dict[str, dict] = {}
    if "collect_evidence_with_tools" in stashed:
        node_outputs["collect_evidence_with_tools"] = {
            "tool_executions": [
                ToolExecution.model_validate(te)
                for te in stashed["collect_evidence_with_tools"].get("tool_executions", [])
            ]
        }
    if "summarize_evidence" in stashed:
        node_outputs["summarize_evidence"] = {
            "evidence_summary": stashed["summarize_evidence"].get("evidence_summary", "")
        }
    return {"scenario": scenario, "variant": variant}, node_outputs


# The reference agent's fixed node order, for the comparison report's timeline.
NODES = ["receive_request", "collect_evidence_with_tools", "summarize_evidence",
         "decide_action", "final_response"]


def write_report(path: str, *, case: Any, replay_run: Any, fork_run: Any,
                 report: Any, edits: dict) -> str:
    """Write the replay-vs-fork comparison HTML (PRD 'compare' view)."""
    import comparison_html

    root_state, _ = rehydrate(case)
    base = root_state["variant"]
    settings = [(k, getattr(base, k, None), v) for k, v in edits.items()
                if getattr(base, k, None) != v]
    return comparison_html.write(
        path,
        case_id=case.case_id,
        scenario=case.trace_contract.raw_config["scenario_id"],
        cut=CUT,
        nodes=NODES,
        settings_changes=settings,
        outcomes=[(c.field, c.baseline_value, c.comparison_value, c.matches) for c in report.fork],
        has_fork_drift=report.has_fork_drift,
        replay_summary=replay_run.decision.get("summary", ""),
        fork_summary=fork_run.decision.get("summary", ""),
    )
