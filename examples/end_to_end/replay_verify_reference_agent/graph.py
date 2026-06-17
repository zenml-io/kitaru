"""LangGraph implementation of the reference agent."""

from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

from langgraph.graph import END, START, StateGraph
from typing_extensions import TypedDict

from .agent import (
    collect_evidence_with_llm_tools,
    decide_with_llm,
    summarize_evidence_with_llm,
)
from .config import AgentVariant, Scenario, SupportDecision
from .tools import SupportTools, ToolExecution

CollectEvidenceFn = Callable[..., list[ToolExecution]]
SummarizeEvidenceFn = Callable[..., str]
DecideFn = Callable[..., SupportDecision]


class AgentState(TypedDict, total=False):
    """State passed between LangGraph nodes."""

    scenario: Scenario
    variant: AgentVariant
    tool_executions: list[ToolExecution]
    evidence_summary: str
    decision: SupportDecision
    final_output: dict[str, Any]


def run_reference_agent(
    *,
    scenario: Scenario,
    variant: AgentVariant,
    db_path: Path,
    api_base_url: str,
    kb_dir: Path,
    callbacks: list[Any],
    metadata: dict[str, Any],
    tags: list[str],
) -> dict[str, Any]:
    """Run one scenario and variant through the LangGraph agent."""
    tools = SupportTools(db_path=db_path, api_base_url=api_base_url, kb_dir=kb_dir)
    graph = build_graph(
        tools=tools,
        callbacks=callbacks,
        metadata=metadata,
        tags=tags,
    )
    result = graph.invoke(
        {"scenario": scenario, "variant": variant},
        config={"callbacks": callbacks, "metadata": metadata, "tags": tags},
    )
    return cast(dict[str, Any], result["final_output"])


def build_graph(
    *,
    tools: SupportTools,
    callbacks: list[Any],
    metadata: dict[str, Any],
    tags: list[str],
    checkpointer: Any | None = None,
    collect_evidence_fn: CollectEvidenceFn = collect_evidence_with_llm_tools,
    summarize_evidence_fn: SummarizeEvidenceFn = summarize_evidence_with_llm,
    decide_fn: DecideFn = decide_with_llm,
) -> Any:
    """Build the small LangGraph state machine."""
    builder = StateGraph(cast(Any, AgentState))

    def receive_request(state: AgentState) -> AgentState:
        return {"tool_executions": []}

    def collect_evidence_with_tools(state: AgentState) -> AgentState:
        executions = collect_evidence_fn(
            scenario=state["scenario"],
            variant=state["variant"],
            tools=tools,
            callbacks=callbacks,
            metadata=metadata,
            tags=tags,
        )
        return {"tool_executions": executions}

    def summarize_evidence(state: AgentState) -> AgentState:
        summary = summarize_evidence_fn(
            scenario=state["scenario"],
            variant=state["variant"],
            tool_executions=state["tool_executions"],
            callbacks=callbacks,
            metadata=metadata,
            tags=tags,
        )
        return {"evidence_summary": summary}

    def decide_action(state: AgentState) -> AgentState:
        decision = decide_fn(
            scenario=state["scenario"],
            variant=state["variant"],
            evidence_summary=state["evidence_summary"],
            tool_executions=state["tool_executions"],
            callbacks=callbacks,
            metadata=metadata,
            tags=tags,
        )
        return {"decision": decision}

    def final_response(state: AgentState) -> AgentState:
        executions = state["tool_executions"]
        final_output = {
            "scenario_id": state["scenario"].scenario_id,
            "case_id": state["scenario"].case_id,
            "variant_name": state["variant"].name,
            "model": state["variant"].model,
            "prompt_profile": state["variant"].prompt_profile,
            "tool_policy_name": state["variant"].tool_policy_name,
            "tool_selection_mode": "llm_tool_calling",
            "decision": state["decision"].model_dump(),
            "evidence_summary": state["evidence_summary"],
            "tool_executions": [execution.model_dump() for execution in executions],
            "audit_relevant_tool_names": [
                execution.name for execution in executions if execution.wrote_state
            ],
        }
        return {"final_output": final_output}

    builder.add_node("receive_request", receive_request)
    builder.add_node("collect_evidence_with_tools", collect_evidence_with_tools)
    builder.add_node("summarize_evidence", summarize_evidence)
    builder.add_node("decide_action", decide_action)
    builder.add_node("final_response", final_response)
    builder.add_edge(START, "receive_request")
    builder.add_edge("receive_request", "collect_evidence_with_tools")
    builder.add_edge("collect_evidence_with_tools", "summarize_evidence")
    builder.add_edge("summarize_evidence", "decide_action")
    builder.add_edge("decide_action", "final_response")
    builder.add_edge("final_response", END)
    if checkpointer is None:
        return builder.compile()
    return builder.compile(checkpointer=checkpointer)
