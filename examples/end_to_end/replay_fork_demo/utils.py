"""The only domain glue: wrap the bundled (pydantic-typed) reference agent.

A JSON-native agent wouldn't need any of this — `KitaruAdapter(graph)` would be
enough. This agent uses typed state (Scenario / AgentVariant / ToolExecution),
so we (1) build its compiled graph and (2) rehydrate the trace's recorded JSON
back into those types before the live tail re-runs.
"""

from __future__ import annotations

from dataclasses import dataclass
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
    from langfuse.langchain import CallbackHandler

    from langfuse import get_client

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
            input={
                "scenario_id": scenario.scenario_id,
                "user_request": scenario.user_request,
            },
            metadata={**metadata, "tags": tags},
            trace_context={"trace_id": trace_id},
        ) as root:
            output = run_reference_agent(
                scenario=scenario,
                variant=variant,
                db_path=db.DEFAULT_DB_PATH,
                api_base_url=api.base_url,
                kb_dir=EXAMPLE_DIR / "knowledge_base",
                callbacks=[handler],
                metadata=metadata,
                tags=tags,
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
                for te in stashed["collect_evidence_with_tools"].get(
                    "tool_executions", []
                )
            ]
        }
    if "summarize_evidence" in stashed:
        node_outputs["summarize_evidence"] = {
            "evidence_summary": stashed["summarize_evidence"].get(
                "evidence_summary", ""
            )
        }
    return {"scenario": scenario, "variant": variant}, node_outputs


# The reference agent's fixed node order, for the comparison report's timeline.
NODES = [
    "receive_request",
    "collect_evidence_with_tools",
    "summarize_evidence",
    "decide_action",
    "final_response",
]


@dataclass(frozen=True)
class DemoTraceSummary:
    trace_id: str
    observation_count: int
    recorded_call_count: int
    node_output_names: list[str]
    graph_node_names: list[str]
    cut: str
    missing_upstream_node_outputs: list[str]
    rehydrate_available: bool


def summarize_case(case: Any, *, has_rehydrate: bool = True) -> DemoTraceSummary:
    """Summarize whether an imported trace has enough data for this demo."""
    node_outputs = case.raw_source_payload.get("langgraph_node_outputs", {})
    output_names = list(node_outputs) if isinstance(node_outputs, dict) else []
    cut_index = NODES.index(CUT) if CUT in NODES else len(NODES)
    required_upstream = NODES[:cut_index]
    return DemoTraceSummary(
        trace_id=case.source_ref.source_id,
        observation_count=len(case.source_ref.observation_ids),
        recorded_call_count=len(case.recorded_calls),
        node_output_names=output_names,
        graph_node_names=list(NODES),
        cut=CUT,
        missing_upstream_node_outputs=[
            node for node in required_upstream if node not in output_names
        ],
        rehydrate_available=has_rehydrate,
    )


def validate_case_for_demo(summary: DemoTraceSummary) -> None:
    """Fail before replay/fork when the trace cannot seed the demo graph."""
    if summary.recorded_call_count == 0:
        raise ValueError(
            "Imported trace has 0 recorded calls. This usually means the JSONL "
            "file contains top-level trace/case rows instead of Langfuse "
            "observation rows."
        )
    if not summary.node_output_names:
        raise ValueError(
            "Imported trace has no LangGraph node outputs. Node rows must be "
            "named exactly like graph nodes and include metadata.langgraph_node "
            "plus an output state delta."
        )
    if summary.cut not in summary.graph_node_names:
        raise ValueError(
            f"Cut node {summary.cut!r} is not in the reference graph nodes "
            f"({', '.join(summary.graph_node_names)})."
        )
    if summary.missing_upstream_node_outputs:
        missing = ", ".join(summary.missing_upstream_node_outputs)
        raise ValueError(
            "Imported trace is missing recorded outputs for node(s) before the "
            f"cut {summary.cut!r}: {missing}. Replay/fork would not have the "
            "state needed to run the live tail."
        )


def format_summary(summary: DemoTraceSummary) -> str:
    """Return a readable import summary for CLI users."""
    live_tail_start = (
        summary.graph_node_names.index(summary.cut)
        if summary.cut in summary.graph_node_names
        else 0
    )
    live_tail = " -> ".join(summary.graph_node_names[live_tail_start:])
    node_outputs = ", ".join(summary.node_output_names) or "(none)"
    return "\n".join(
        [
            "Trace import summary",
            f"  trace id:           {summary.trace_id}",
            f"  observation rows:   {summary.observation_count}",
            f"  recorded calls:     {summary.recorded_call_count}",
            f"  graph nodes:        {' -> '.join(summary.graph_node_names)}",
            f"  node outputs found: {node_outputs}",
            f"  cut:                {summary.cut}",
            f"  rehydrate:          {'yes' if summary.rehydrate_available else 'no'}",
            f"  live replay tail:   {live_tail}",
            "  side-effect note:   collect_evidence_with_tools is before the cut, "
            "so replay/fork should reuse its recorded output rather than run "
            "tools live.",
        ]
    )


def _three_way_outcomes(report: Any) -> list[tuple[str, Any, Any, Any, bool, bool]]:
    """Build original→replay→fork outcome rows from a DriftReport."""
    reproduction_by_field = {c.field: c for c in report.reproduction}
    fork_by_field = {c.field: c for c in report.fork}
    fields: list[str] = []
    for comparison in [*report.reproduction, *report.fork]:
        if comparison.field not in fields:
            fields.append(comparison.field)

    outcomes = []
    for field in fields:
        reproduction = reproduction_by_field.get(field)
        fork = fork_by_field.get(field)
        original_value = reproduction.baseline_value if reproduction else None
        replay_value = (
            reproduction.comparison_value
            if reproduction
            else fork.baseline_value
            if fork
            else None
        )
        fork_value = fork.comparison_value if fork else None
        reproduction_matches = reproduction.matches if reproduction else True
        fork_matches = fork.matches if fork else True
        outcomes.append(
            (
                field,
                original_value,
                replay_value,
                fork_value,
                reproduction_matches,
                fork_matches,
            )
        )
    return outcomes


def write_report(
    path: str, *, case: Any, replay_run: Any, fork_run: Any, report: Any, edits: dict
) -> str:
    """Write the original/replay/fork comparison HTML."""
    import comparison_html

    root_state, _ = rehydrate(case)
    base = root_state["variant"]
    settings = [
        (k, getattr(base, k, None), v)
        for k, v in edits.items()
        if getattr(base, k, None) != v
    ]
    return comparison_html.write(
        path,
        case_id=case.case_id,
        scenario=case.trace_contract.raw_config["scenario_id"],
        cut=CUT,
        nodes=NODES,
        settings_changes=settings,
        outcomes=_three_way_outcomes(report),
        has_reproduction_drift=report.has_reproduction_drift,
        has_fork_drift=report.has_fork_drift,
        original_summary=replay_run.original_decision.get("summary", ""),
        replay_summary=replay_run.decision.get("summary", ""),
        fork_summary=fork_run.decision.get("summary", ""),
    )
