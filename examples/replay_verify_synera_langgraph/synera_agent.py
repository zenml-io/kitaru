"""A LangGraph mechanical-engineering design assistant for the Synera demo.

This is a real compiled LangGraph ``StateGraph`` (router -> tools -> finalize),
not a mock, so the Replay Verify demo runs against Synera's actual agent
framework. It is deterministic and needs no model credentials: a tiny rule-based
router stands in for the LLM so the demo runs offline and its tool-selection
trajectory is exactly reproducible.

Baseline vs candidate differ only by a ``config`` flag (``skip_fea_validation``).
The candidate is a "cheaper / faster" configuration that silently stops running
the FEA validation step on simulation requests -- the planted regression the
Replay Verify engine is meant to catch from imported traces.

Tools (mechanical-engineering domain, all side-effect safe / deterministic):

- ``parse_requirements``     read engineering requirements out of the request
- ``generate_cad_geometry``  produce a parametric geometry stub
- ``run_fea_simulation``     run a (mocked) finite-element validation pass
- ``search_standards_kb``    RAG lookup against an engineering-standards corpus
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, TypedDict

from langgraph.graph import END, StateGraph

# Single source of truth for the agent's tool vocabulary. The fixtures, the
# runner, and the demo's allowed-tool gate all reference this set.
TOOL_PARSE = "parse_requirements"
TOOL_GEOMETRY = "generate_cad_geometry"
TOOL_FEA = "run_fea_simulation"
TOOL_STANDARDS = "search_standards_kb"

SAFE_TOOL_NAMES = {TOOL_PARSE, TOOL_GEOMETRY, TOOL_FEA, TOOL_STANDARDS}

# Intent -> the ordered tool plan a correct (baseline) agent should follow.
TOOL_PLAN_BY_INTENT: dict[str, list[str]] = {
    "requirements_only": [TOOL_PARSE],
    "geometry_request": [TOOL_PARSE, TOOL_GEOMETRY],
    "simulation_request": [TOOL_PARSE, TOOL_GEOMETRY, TOOL_FEA],
    "standards_question": [TOOL_STANDARDS],
}

# Intent -> the discipline label the agent reports (reused as ``policy_label``
# in the imported-case comparison contract).
DISCIPLINE_LABEL_BY_INTENT: dict[str, str] = {
    "requirements_only": "requirements_engineering",
    "geometry_request": "cad_design",
    "simulation_request": "structural_simulation",
    "standards_question": "standards_compliance",
}
DEFAULT_DISCIPLINE_LABEL = "requirements_engineering"


class AgentState(TypedDict, total=False):
    """Mutable state threaded through the LangGraph nodes."""

    root_input: Mapping[str, Any]
    config: Mapping[str, Any]
    available_tools: tuple[str, ...]
    retrieval_document_ids: list[str]
    intent: str
    tool_plan: list[str]
    tool_names: list[str]
    tool_results: list[dict[str, Any]]
    discipline_label: str
    risk_status: str
    response: str


def _route(state: AgentState) -> AgentState:
    """Rule-based router (LLM stand-in): pick the tool plan for the intent.

    The candidate's ``skip_fea_validation`` config drops the FEA step from
    simulation requests -- a faster, cheaper config that silently stops
    validating structural integrity. That is the regression to catch.
    """
    root = state["root_input"] if isinstance(state["root_input"], Mapping) else {}
    intent = str(root.get("intent") or "requirements_only")
    plan = list(TOOL_PLAN_BY_INTENT.get(intent, [TOOL_PARSE]))

    config = state.get("config") or {}
    if config.get("skip_fea_validation") and TOOL_FEA in plan:
        plan = [tool for tool in plan if tool != TOOL_FEA]

    return {**state, "intent": intent, "tool_plan": plan}


def _run_tools(state: AgentState) -> AgentState:
    """Execute the planned tools deterministically, honouring imported availability."""
    available = set(state.get("available_tools") or ())
    results: list[dict[str, Any]] = []
    names: list[str] = []
    for tool_name in state.get("tool_plan", []):
        if tool_name not in available:
            # The trace did not import this tool as available; skip rather than
            # fabricate a side effect. Surfaces as tool-selection drift.
            continue
        results.append(_run_tool(tool_name, state))
        names.append(tool_name)

    retrieval_ids: list[str] = []
    if TOOL_STANDARDS in names:
        retrieval_ids = list(state.get("retrieval_document_ids") or [])

    return {
        **state,
        "tool_names": names,
        "tool_results": results,
        "retrieval_document_ids": retrieval_ids,
    }


def _finalize(state: AgentState) -> AgentState:
    """Assign the discipline label, risk status, and a human-readable response."""
    intent = state.get("intent", "requirements_only")
    label = DISCIPLINE_LABEL_BY_INTENT.get(intent, DEFAULT_DISCIPLINE_LABEL)
    names = state.get("tool_names", [])

    # A simulation answer that never ran FEA validation is not safe to trust.
    if intent == "simulation_request" and TOOL_FEA not in names:
        risk_status = "needs_review"
    else:
        risk_status = "safe"

    root = state["root_input"] if isinstance(state["root_input"], Mapping) else {}
    request = root.get("user_message", "engineering request")
    response = f"[{label}] handled '{request}' using tools: {', '.join(names) or 'none'}"

    return {**state, "discipline_label": label, "risk_status": risk_status, "response": response}


def _run_tool(tool_name: str, state: AgentState) -> dict[str, Any]:
    """Deterministic, side-effect-safe mechanical-engineering tool."""
    root = state["root_input"] if isinstance(state["root_input"], Mapping) else {}
    if tool_name == TOOL_PARSE:
        return {
            "tool_name": TOOL_PARSE,
            "part_id": root.get("part_id", "part-demo"),
            "requirements": root.get("requirements", ["load", "material"]),
            "side_effect_status": "safe",
            "executed_live": False,
        }
    if tool_name == TOOL_GEOMETRY:
        return {
            "tool_name": TOOL_GEOMETRY,
            "part_id": root.get("part_id", "part-demo"),
            "geometry": "parametric-stub",
            "side_effect_status": "safe",
            "executed_live": False,
        }
    if tool_name == TOOL_FEA:
        return {
            "tool_name": TOOL_FEA,
            "part_id": root.get("part_id", "part-demo"),
            "max_stress_mpa": 182.0,
            "safety_factor": 1.7,
            "side_effect_status": "safe",
            "executed_live": False,
        }
    if tool_name == TOOL_STANDARDS:
        ids = list(state.get("retrieval_document_ids") or [])
        return {
            "tool_name": TOOL_STANDARDS,
            "query": root.get("user_message"),
            "document_ids": ids,
            "side_effect_status": "safe",
            "executed_live": False,
        }
    msg = f"Synera demo tool {tool_name!r} is not registered."
    raise ValueError(msg)


def build_synera_graph() -> Any:
    """Compile the mechanical-engineering assistant graph."""
    graph = StateGraph(AgentState)
    graph.add_node("route", _route)
    graph.add_node("run_tools", _run_tools)
    graph.add_node("finalize", _finalize)
    graph.set_entry_point("route")
    graph.add_edge("route", "run_tools")
    graph.add_edge("run_tools", "finalize")
    graph.add_edge("finalize", END)
    return graph.compile()


# Compiled once at import; the graph is stateless across invocations.
SYNERA_GRAPH = build_synera_graph()


def run_synera_graph(
    *,
    root_input: Any,
    config: Mapping[str, Any],
    available_tools: Sequence[str],
    retrieval_document_ids: Sequence[str],
) -> AgentState:
    """Invoke the compiled LangGraph agent and return its final state."""
    initial: AgentState = {
        "root_input": root_input,
        "config": dict(config),
        "available_tools": tuple(available_tools),
        "retrieval_document_ids": list(retrieval_document_ids),
    }
    return SYNERA_GRAPH.invoke(initial)
