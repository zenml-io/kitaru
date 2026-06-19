"""LangGraph + Kitaru adapter example.

The `graph_call` strategy is local and needs no provider API key. The `calls`
strategy uses a real OpenAI-backed LangChain agent with deterministic local
Python tools so Kitaru can checkpoint actual model/tool handler calls. The
`sandbox` strategy uses the same calls-mode path with Kitaru's active-stack
sandbox command tool.
"""

import argparse
import os
import time
from collections.abc import Callable, Mapping
from typing import Any, Literal, cast

from langchain.agents import create_agent
from langchain.agents.middleware import AgentMiddleware
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt
from typing_extensions import TypedDict

import kitaru
from kitaru import checkpoint, flow
from kitaru.adapters.langgraph import (
    DEFAULT_SANDBOX_COMMAND_TOOL_NAME,
    KitaruGraphRunner,
    LangGraphCallCheckpointPolicy,
    LangGraphRunRequest,
    build_resume_request,
    create_sandbox_command_tool,
)
from kitaru.adapters.langgraph.langchain import KitaruLangGraphMiddleware

Strategy = Literal["graph_call", "calls", "sandbox"]
THREAD_ID = "langgraph-local-demo-thread"
SUMMARY_ARTIFACT = "summary__langgraph_demo"
GRAPH_CALL_RUNNER_NAME = "langgraph_local_interrupt_demo"
CALLS_RUNNER_NAME = "langgraph_local_calls_demo"
SANDBOX_RUNNER_NAME = "langgraph_sandbox_command_demo"
SANDBOX_COMMAND_TOOL_NAME = DEFAULT_SANDBOX_COMMAND_TOOL_NAME
DEFAULT_LANGGRAPH_AGENT_MODEL = "gpt-5-nano"
DEFAULT_SANDBOX_AGENT_MODEL = "gpt-5-nano"
SANDBOX_DEMO_COMMAND = (
    'python -c "import json, os, sys; '
    "print(json.dumps({'cwd': os.getcwd(), 'python': sys.version.split()[0]}))\""
)

TICKETS: dict[str, dict[str, str]] = {
    "ticket-42": {
        "customer": "Amina",
        "issue": "Delayed shipment for order ORD-1007",
        "status": "needs_escalation",
        "priority": "high",
    },
    "ticket-17": {
        "customer": "Jonas",
        "issue": "Address correction requested before dispatch",
        "status": "open",
        "priority": "normal",
    },
}


class ReviewState(TypedDict, total=False):
    ticket: str
    decision: dict[str, Any]
    status: str


class ForceSandboxToolChoiceMiddleware(AgentMiddleware):
    """Force the demo model to call the sandbox command tool once.

    The provider call is still real. This middleware only makes the example's
    side effect deterministic by replacing the sandbox tool call arguments after
    the model has chosen the forced tool and before LangChain runs the tool.
    """

    def wrap_model_call(
        self,
        request: Any,
        handler: Callable[[Any], Any],
    ) -> Any:
        if _message_history_has_tool_result(getattr(request, "messages", [])):
            response = handler(request)
            _reject_followup_sandbox_tool_calls(response)
            return response

        forced_request = request.override(
            tool_choice={
                "type": "function",
                "function": {"name": SANDBOX_COMMAND_TOOL_NAME},
            },
            model_settings=_model_settings_with_single_tool_call(request),
        )
        response = handler(forced_request)
        _force_sandbox_tool_arguments(response)
        return response


def _model_settings_with_single_tool_call(request: Any) -> dict[str, Any]:
    """Return model settings that prevent duplicate forced tool calls."""
    existing = getattr(request, "model_settings", None)
    settings = dict(existing) if isinstance(existing, Mapping) else {}
    settings["parallel_tool_calls"] = False
    return settings


def _force_sandbox_tool_arguments(response: Any) -> None:
    """Replace the single sandbox tool-call args with the demo command."""
    matching_tool_calls: list[Any] = []
    for message in _response_messages(response):
        matching_tool_calls.extend(
            tool_call
            for tool_call in _message_tool_calls(message)
            if _tool_call_name(tool_call) == SANDBOX_COMMAND_TOOL_NAME
        )

    if len(matching_tool_calls) != 1:
        raise RuntimeError(
            f"Sandbox demo expected exactly one {SANDBOX_COMMAND_TOOL_NAME} "
            f"tool call from the real model, got {len(matching_tool_calls)}. "
            "The demo stops before running any sandbox command so its summary "
            "cannot report a different number of commands than actually ran."
        )

    tool_call = matching_tool_calls[0]
    if isinstance(tool_call, dict):
        tool_call["args"] = {"command": SANDBOX_DEMO_COMMAND}
        return
    try:
        tool_call.args = {"command": SANDBOX_DEMO_COMMAND}
    except Exception as exc:
        raise RuntimeError(
            "Sandbox demo could not pin the "
            f"{SANDBOX_COMMAND_TOOL_NAME} arguments before LangChain tool "
            "execution."
        ) from exc


def _reject_followup_sandbox_tool_calls(response: Any) -> None:
    """Reject a second sandbox command request after the first tool result."""
    matching_tool_calls: list[Any] = []
    for message in _response_messages(response):
        matching_tool_calls.extend(
            tool_call
            for tool_call in _message_tool_calls(message)
            if _tool_call_name(tool_call) == SANDBOX_COMMAND_TOOL_NAME
        )

    if matching_tool_calls:
        raise RuntimeError(
            f"Sandbox demo expected no additional {SANDBOX_COMMAND_TOOL_NAME} "
            "tool calls after the first sandbox tool result, got "
            f"{len(matching_tool_calls)}. The demo stops before running a "
            "second sandbox command."
        )


def _response_messages(response: Any) -> list[Any]:
    """Return message-like objects from LangChain's model response shapes."""
    inner = getattr(response, "model_response", response)
    result = getattr(inner, "result", None)
    if isinstance(result, list | tuple):
        return list(result)
    if result is not None:
        return [result]
    if isinstance(inner, list | tuple):
        return list(inner)
    return [inner]


def _message_tool_calls(message: Any) -> list[Any]:
    """Return normalized LangChain tool-call payloads from one message."""
    if isinstance(message, dict):
        value = message.get("tool_calls") or message.get("tool_call_chunks")
    else:
        value = getattr(message, "tool_calls", None) or getattr(
            message, "tool_call_chunks", None
        )
    if isinstance(value, list | tuple):
        return list(value)
    return []


def _tool_call_name(tool_call: Any) -> str | None:
    if isinstance(tool_call, dict):
        name = tool_call.get("name")
    else:
        name = getattr(tool_call, "name", None)
    return name if isinstance(name, str) else None


def _message_history_has_tool_result(messages: Any) -> bool:
    """Return whether LangChain has already appended a tool result message."""
    if messages is None:
        return False
    try:
        message_iter = reversed(messages)
    except TypeError:
        message_iter = iter(messages)
    for message in message_iter:
        if isinstance(message, dict):
            if message.get("role") == "tool" or message.get("type") == "tool":
                return True
            continue
        if getattr(message, "type", None) == "tool":
            return True
        if getattr(message, "role", None) == "tool":
            return True
        if getattr(message, "tool_call_id", None) is not None:
            return True
    return False


def build_interrupt_graph() -> Any:
    """Build a tiny local graph that interrupts once for a human decision."""
    builder = StateGraph(cast(Any, ReviewState))

    def request_decision(state: ReviewState) -> ReviewState:
        decision = interrupt(
            {
                "question": "Approve ticket escalation?",
                "ticket": state["ticket"],
            }
        )
        return {"decision": cast(dict[str, Any], decision)}

    def finalize(state: ReviewState) -> ReviewState:
        approved = bool(state["decision"].get("approved", False))
        return {"status": "approved" if approved else "rejected"}

    builder.add_node("request_decision", request_decision)
    builder.add_node("finalize", finalize)
    builder.add_edge(START, "request_decision")
    builder.add_edge("request_decision", "finalize")
    builder.add_edge("finalize", END)
    return builder.compile(checkpointer=InMemorySaver())


def lookup_ticket(ticket: str) -> str:
    """Look up local support ticket details before deciding on escalation."""
    record = TICKETS.get(ticket)
    if record is None:
        return (
            f"Ticket {ticket} was not found. Ask the user for a valid ticket id "
            "before approving any escalation."
        )
    return (
        f"Ticket {ticket}: customer={record['customer']}, issue={record['issue']}, "
        f"status={record['status']}, priority={record['priority']}"
    )


def approve_ticket(ticket: str, reason: str | None = None) -> str:
    """Approve a local ticket escalation after looking up the ticket details."""
    record = TICKETS.get(ticket)
    if record is None:
        return f"not_approved:{ticket}:ticket_not_found"
    if record["status"] != "needs_escalation":
        return f"not_approved:{ticket}:status_{record['status']}"
    approval_reason = reason or "replacement-authorized"
    return f"approved:{ticket}:{approval_reason}"


def _require_openai_api_key() -> None:
    if os.getenv("OPENAI_API_KEY"):
        return
    raise SystemExit(
        "Missing OPENAI_API_KEY.\n"
        "Set it first, then rerun:\n"
        "  export OPENAI_API_KEY='sk-...'"
    )


def _langgraph_agent_model_name() -> str:
    return os.getenv("LANGGRAPH_AGENT_MODEL", DEFAULT_LANGGRAPH_AGENT_MODEL)


def _sandbox_agent_model_name() -> str:
    return os.getenv(
        "LANGGRAPH_SANDBOX_AGENT_MODEL",
        os.getenv("LANGGRAPH_AGENT_MODEL", DEFAULT_SANDBOX_AGENT_MODEL),
    )


def _openai_chat_model(model_name: str | None = None) -> Any:
    """Build the OpenAI chat model used by provider-backed examples."""
    try:
        from langchain_openai import ChatOpenAI
    except ImportError as error:
        raise SystemExit(
            "Missing LangChain OpenAI provider.\n"
            "Install it with:\n"
            "  uv sync --extra local --extra langgraph-openai"
        ) from error

    return ChatOpenAI(model=model_name or _langgraph_agent_model_name())


def build_calls_agent(ticket: str) -> Any:
    """Build an OpenAI-backed LangChain agent with local ticket tools."""
    return create_agent(
        model=_openai_chat_model(),
        tools=[lookup_ticket, approve_ticket],
        middleware=[KitaruLangGraphMiddleware(graph_name=CALLS_RUNNER_NAME)],
        checkpointer=InMemorySaver(),
        name="openai_ticket_agent",
        system_prompt=(
            "You are a careful customer support assistant. "
            f"The current ticket is {ticket}. "
            "Always call lookup_ticket first. "
            "If the ticket status is needs_escalation, call approve_ticket. "
            "In the final response, include the ticket id, status, approval result, "
            "and next step."
        ),
    )


def build_sandbox_agent() -> Any:
    """Build an OpenAI-backed LangChain agent with Kitaru's sandbox command tool."""
    return create_agent(
        model=_openai_chat_model(_sandbox_agent_model_name()),
        tools=[create_sandbox_command_tool()],
        middleware=[
            ForceSandboxToolChoiceMiddleware(),
            KitaruLangGraphMiddleware(graph_name=SANDBOX_RUNNER_NAME),
        ],
        checkpointer=InMemorySaver(),
        name="openai_sandbox_agent",
        system_prompt=(
            "You are demonstrating Kitaru's LangChain sandbox command tool. "
            f"Call {SANDBOX_COMMAND_TOOL_NAME} exactly once with the command "
            "provided by the user. After the tool returns JSON, summarize the "
            "stdout, stderr, "
            "and exit code. Do not claim to manage Deep Agents files."
        ),
    )


GRAPH_CALL_RUNNER = KitaruGraphRunner(
    build_interrupt_graph(),
    name=GRAPH_CALL_RUNNER_NAME,
    checkpoint_strategy="graph_call",
)


@checkpoint
def persist_summary(summary: dict[str, Any]) -> dict[str, Any]:
    """Persist and return the final summary inside checkpoint scope."""
    kitaru.save(SUMMARY_ARTIFACT, summary, type="context")
    return summary


@flow
def run_demo_flow(strategy: Strategy, ticket: str) -> None:
    """Run one LangGraph adapter demo and persist a readable summary artifact."""
    if strategy == "graph_call":
        summary = _run_graph_call_demo(ticket)
    elif strategy == "calls":
        summary = _run_calls_demo(ticket)
    else:
        summary = _run_sandbox_demo()
    _ = persist_summary(summary)


def _run_graph_call_demo(ticket: str) -> dict[str, Any]:
    """Run the coarse graph-call checkpoint demo."""
    started = GRAPH_CALL_RUNNER.invoke(
        LangGraphRunRequest.start({"ticket": ticket}, thread_id=THREAD_ID)
    )
    if started.status != "interrupted":
        raise RuntimeError(f"Expected interrupted status, got: {started.status}")

    resumed = GRAPH_CALL_RUNNER.invoke(
        build_resume_request(
            started,
            {
                "approved": True,
                "reviewer": "local-example",
            },
        )
    )
    if resumed.status != "completed":
        raise RuntimeError(
            f"Expected completed status after resume, got: {resumed.status}"
        )

    output = cast(dict[str, Any], resumed.output)
    return {
        "strategy": "graph_call",
        "thread_id": THREAD_ID,
        "first_status": started.status,
        "interrupt_payload": started.interrupts[0].value
        if started.interrupts
        else None,
        "resume_status": resumed.status,
        "final_output": output,
        "latest_checkpoint_id": resumed.latest_checkpoint_id,
        "next_nodes_after_resume": resumed.next_nodes,
        "event_artifacts": [
            started.event_log_artifact_name,
            resumed.event_log_artifact_name,
        ],
        "run_summary_artifacts": [
            started.run_summary_artifact_name,
            resumed.run_summary_artifact_name,
        ],
    }


def _run_calls_demo(ticket: str) -> dict[str, Any]:
    """Run the granular LangChain middleware calls-mode demo."""
    _require_openai_api_key()
    model_name = _langgraph_agent_model_name()
    runner = KitaruGraphRunner(
        build_calls_agent(ticket),
        name=CALLS_RUNNER_NAME,
        checkpoint_strategy="calls",
    )
    result = runner.invoke(
        LangGraphRunRequest.start(
            {
                "messages": [
                    {
                        "role": "user",
                        "content": (
                            f"Please handle support ticket {ticket}. Look up the "
                            "ticket first, approve it if escalation is needed, and "
                            "then give me the status, approval result, and next step."
                        ),
                    }
                ]
            },
            thread_id=THREAD_ID,
        )
    )
    if result.status != "completed":
        raise RuntimeError(f"Expected completed status, got: {result.status}")

    output = cast(dict[str, Any], result.output)
    messages = _message_summaries(output.get("messages", []))
    return {
        "strategy": "calls",
        "thread_id": THREAD_ID,
        "model": model_name,
        "status": result.status,
        "message_count": len(messages),
        "messages": messages,
        "final_message": messages[-1]["content"] if messages else None,
        "latest_checkpoint_id": result.latest_checkpoint_id,
        "event_artifact": result.event_log_artifact_name,
        "run_summary_artifact": result.run_summary_artifact_name,
        "typical_kitaru_call_checkpoint_prefixes": [
            "model_call__...",
            "tool_call__lookup_ticket_...",
            "langgraph_summary__...",
        ],
        "model_dependent_kitaru_call_checkpoint_prefixes": [
            "tool_call__approve_ticket_...",
        ],
    }


def _run_sandbox_demo() -> dict[str, Any]:
    """Run the LangChain sandbox command tool through calls-mode checkpointing."""
    _require_openai_api_key()
    model_name = _sandbox_agent_model_name()
    runner = KitaruGraphRunner(
        build_sandbox_agent(),
        name=SANDBOX_RUNNER_NAME,
        checkpoint_strategy="calls",
        call_checkpoint_policy=LangGraphCallCheckpointPolicy(
            model_checkpoint_config=False,
        ),
    )
    result = runner.invoke(
        LangGraphRunRequest.start(
            {
                "messages": [
                    {
                        "role": "user",
                        "content": (
                            f"Use {SANDBOX_COMMAND_TOOL_NAME} to run this exact "
                            f"command: {SANDBOX_DEMO_COMMAND}"
                        ),
                    }
                ]
            },
            thread_id=THREAD_ID,
        )
    )
    if result.status != "completed":
        raise RuntimeError(f"Expected completed status, got: {result.status}")

    output = cast(dict[str, Any], result.output)
    messages = _message_summaries(output.get("messages", []))
    return {
        "strategy": "sandbox",
        "thread_id": THREAD_ID,
        "model": model_name,
        "status": result.status,
        "sandbox_command": SANDBOX_DEMO_COMMAND,
        "message_count": len(messages),
        "messages": messages,
        "final_message": messages[-1]["content"] if messages else None,
        "latest_checkpoint_id": result.latest_checkpoint_id,
        "event_artifact": result.event_log_artifact_name,
        "run_summary_artifact": result.run_summary_artifact_name,
        "kitaru_behavior": (
            "The command ran through the active Kitaru stack sandbox via "
            "run_sandbox_command, and calls mode checkpoints the synchronous "
            "LangChain tool handler."
        ),
        "expected_kitaru_call_checkpoint_prefixes": [
            f"tool_call__{SANDBOX_COMMAND_TOOL_NAME}_...",
            "langgraph_summary__...",
        ],
        "deep_agents_boundary": "This is not a Deep Agents backend.",
    }


def _message_summaries(messages: Any) -> list[dict[str, Any]]:
    """Return JSON-safe summaries for LangChain message objects."""
    summaries: list[dict[str, Any]] = []
    for message in list(messages or []):
        tool_calls = getattr(message, "tool_calls", None) or []
        summaries.append(
            {
                "type": type(message).__name__,
                "name": getattr(message, "name", None),
                "content": getattr(message, "content", None),
                "tool_call_ids": [
                    str(tool_call.get("id"))
                    for tool_call in tool_calls
                    if isinstance(tool_call, dict) and tool_call.get("id") is not None
                ],
            }
        )
    return summaries


def run_workflow(
    strategy: Strategy = "graph_call",
    ticket: str = "ticket-42",
) -> tuple[str, dict[str, Any]]:
    """Run the flow and load the saved summary artifact."""
    if strategy in {"calls", "sandbox"}:
        _require_openai_api_key()

    handle = run_demo_flow.run(strategy, ticket)
    while not handle.status.is_finished:
        time.sleep(1)
    if not handle.status.is_successful:
        raise RuntimeError(f"Flow failed with status: {handle.status.value}")

    client = kitaru.KitaruClient()
    artifacts = client.artifacts.list(handle.exec_id)
    summary_artifact = next(
        artifact for artifact in artifacts if artifact.name == SUMMARY_ARTIFACT
    )
    summary = cast(dict[str, Any], summary_artifact.load())
    return handle.exec_id, summary


def parse_args() -> argparse.Namespace:
    """Parse example CLI arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--strategy",
        choices=["graph_call", "calls", "sandbox"],
        default="graph_call",
        help=(
            "graph_call runs the local interrupt/resume demo; calls runs a real "
            "OpenAI-backed LangChain agent with local ticket tools; sandbox runs "
            "a real OpenAI-backed LangChain agent with Kitaru's active-stack "
            "sandbox command tool. Provider-backed strategies require "
            "OPENAI_API_KEY."
        ),
    )
    parser.add_argument("--ticket", default="ticket-42")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    execution_id, summary = run_workflow(args.strategy, args.ticket)
    print(f"Execution ID: {execution_id}")
    print(f"LangGraph adapter demo summary ({args.strategy}):")
    for key, value in summary.items():
        print(f"- {key}: {value}")
