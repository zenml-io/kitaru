"""The only domain glue: wrap the bundled (pydantic-typed) reference agent.

A JSON-native agent wouldn't need any of this — `KitaruAdapter(graph)` would be
enough. This agent uses typed state (Scenario / AgentVariant / ToolExecution),
so we (1) build its compiled graph and (2) rehydrate the trace's recorded JSON
back into those types before the live tail re-runs.
"""
from __future__ import annotations

from typing import Any

from reference_agent import db
from reference_agent.config import EXAMPLE_DIR, load_scenarios, load_variant
from reference_agent.graph import build_graph
from reference_agent.tools import SupportTools, ToolExecution

# Node-level fork point: replay/fork re-run decide_action onward, live.
CUT = "decide_action"


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
