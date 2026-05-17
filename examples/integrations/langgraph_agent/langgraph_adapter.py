"""Local LangGraph + Kitaru adapter example (no external model API)."""

import argparse
import time
from typing import Any, Literal, cast

from langchain.agents import create_agent
from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel
from langchain_core.messages import AIMessage
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt
from typing_extensions import TypedDict

import kitaru
from kitaru import checkpoint, flow
from kitaru.adapters.langgraph import (
    KitaruGraphRunner,
    LangGraphRunRequest,
    build_resume_request,
)
from kitaru.adapters.langgraph.langchain import KitaruLangGraphMiddleware

Strategy = Literal["graph_call", "calls"]
THREAD_ID = "langgraph-local-demo-thread"
SUMMARY_ARTIFACT = "summary__langgraph_demo"
GRAPH_CALL_RUNNER_NAME = "langgraph_local_interrupt_demo"
CALLS_RUNNER_NAME = "langgraph_local_calls_demo"


class ReviewState(TypedDict, total=False):
    ticket: str
    decision: dict[str, Any]
    status: str


class ToolCallingFakeModel(FakeMessagesListChatModel):
    """Fake chat model that supports LangChain tool binding for this demo."""

    def bind_tools(
        self,
        tools: Any,
        *,
        tool_choice: Any = None,
        **kwargs: Any,
    ) -> Any:
        """Return self so LangChain can run deterministic fake tool calls."""
        return self


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


def approve_ticket(ticket: str) -> str:
    """Approve a ticket and return a deterministic local tool result."""
    return f"approved:{ticket}"


def build_calls_agent(ticket: str) -> Any:
    """Build a deterministic LangChain agent that makes one tool call."""
    model = ToolCallingFakeModel(
        responses=[
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "approve_ticket",
                        "args": {"ticket": ticket},
                        "id": "call-approve-ticket",
                    }
                ],
            ),
            AIMessage(content=f"Ticket {ticket} approved by fake local model."),
        ]
    )
    return create_agent(
        model=model,
        tools=[approve_ticket],
        middleware=[KitaruLangGraphMiddleware(graph_name=CALLS_RUNNER_NAME)],
        checkpointer=InMemorySaver(),
        name="fake_ticket_agent",
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
    else:
        summary = _run_calls_demo(ticket)
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
                        "content": f"Please decide whether to approve {ticket}.",
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
        "status": result.status,
        "message_count": len(messages),
        "messages": messages,
        "final_message": messages[-1]["content"] if messages else None,
        "latest_checkpoint_id": result.latest_checkpoint_id,
        "event_artifact": result.event_log_artifact_name,
        "run_summary_artifact": result.run_summary_artifact_name,
        "expected_kitaru_call_checkpoint_prefixes": [
            "model_call__...",
            "tool_call__approve_ticket_...",
            "model_call__...",
        ],
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
        choices=["graph_call", "calls"],
        default="graph_call",
        help=(
            "graph_call runs the existing interrupt/resume demo; calls runs a "
            "deterministic LangChain middleware demo with model/tool call "
            "checkpoints."
        ),
    )
    parser.add_argument("--ticket", default="ticket-42")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    execution_id, summary = run_workflow(args.strategy, args.ticket)
    print(f"Execution ID: {execution_id}")
    print(f"LangGraph adapter local demo summary ({args.strategy}):")
    for key, value in summary.items():
        print(f"- {key}: {value}")
