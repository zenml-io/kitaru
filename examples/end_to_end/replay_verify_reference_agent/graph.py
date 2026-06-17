"""LangGraph implementation of the reference agent."""

from pathlib import Path
from typing import Any, cast

from langgraph.graph import END, START, StateGraph
from typing_extensions import TypedDict

from .agent import decide_with_llm, summarize_evidence_with_llm
from .config import AgentVariant, Scenario, SupportDecision
from .tools import WRITE_TOOL_NAMES, SupportTools, ToolExecution, blocked_tool_execution


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
) -> Any:
    """Build the small LangGraph state machine."""
    builder = StateGraph(cast(Any, AgentState))

    def receive_request(state: AgentState) -> AgentState:
        return {"tool_executions": []}

    def collect_evidence_with_tools(state: AgentState) -> AgentState:
        scenario = state["scenario"]
        variant = state["variant"]
        plan = list(scenario.tool_plan)
        if variant.prompt_profile == "trimmed_permissions":
            plan.extend(scenario.weakened_permission_tool_plan)

        executions: list[ToolExecution] = []
        for call in plan:
            if (
                len([item for item in executions if not item.blocked])
                >= variant.max_tool_calls
            ):
                executions.append(
                    blocked_tool_execution(
                        call.name,
                        call.args,
                        f"max_tool_calls={variant.max_tool_calls} reached",
                    )
                )
                continue
            if not variant.allows_tool(call.name):
                executions.append(
                    blocked_tool_execution(
                        call.name,
                        call.args,
                        f"tool not allowed by {variant.tool_policy_name}",
                    )
                )
                continue
            if variant.dry_run_writes and call.name in WRITE_TOOL_NAMES:
                executions.append(
                    blocked_tool_execution(
                        call.name,
                        call.args,
                        f"dry_run_writes blocked {call.name}",
                    )
                )
                continue
            executions.append(tools.run(call.name, call.args))
        return {"tool_executions": executions}

    def summarize_evidence(state: AgentState) -> AgentState:
        summary = summarize_evidence_with_llm(
            scenario=state["scenario"],
            variant=state["variant"],
            tool_executions=state["tool_executions"],
            callbacks=callbacks,
            metadata=metadata,
            tags=tags,
        )
        return {"evidence_summary": summary}

    def decide_action(state: AgentState) -> AgentState:
        decision = decide_with_llm(
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
    return builder.compile()
