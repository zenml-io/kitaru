"""Local LangGraph + Kitaru adapter example (flow + interrupt/resume, no LLM)."""

import time
from typing import Any, cast

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

THREAD_ID = "langgraph-local-demo-thread"
SUMMARY_ARTIFACT = "langgraph_demo_summary"


class ReviewState(TypedDict, total=False):
    ticket: str
    decision: dict[str, Any]
    status: str


def build_graph() -> Any:
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


RUNNER = KitaruGraphRunner(build_graph(), name="langgraph_local_interrupt_demo")


@checkpoint
def persist_summary(summary: dict[str, Any]) -> dict[str, Any]:
    """Persist and return the final summary inside checkpoint scope."""
    kitaru.save(SUMMARY_ARTIFACT, summary, type="context")
    return summary


@flow
def run_demo_flow(ticket: str) -> None:
    """Run start/resume calls and persist a readable summary artifact."""
    started = RUNNER.invoke(
        LangGraphRunRequest.start({"ticket": ticket}, thread_id=THREAD_ID)
    )
    if started.status != "interrupted":
        raise RuntimeError(f"Expected interrupted status, got: {started.status}")

    resumed = RUNNER.invoke(
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
    summary = {
        "thread_id": THREAD_ID,
        "first_status": started.status,
        "interrupt_payload": started.interrupts[0].value
        if started.interrupts
        else None,
        "resume_status": resumed.status,
        "final_output": output,
        "latest_checkpoint_id": resumed.latest_checkpoint_id,
        "next_nodes_after_resume": resumed.next_nodes,
    }
    _ = persist_summary(summary)


def run_workflow(ticket: str = "ticket-42") -> tuple[str, dict[str, Any]]:
    """Run the flow and load the saved summary artifact."""
    handle = run_demo_flow.run(ticket)
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


if __name__ == "__main__":
    execution_id, summary = run_workflow()
    print(f"Execution ID: {execution_id}")
    print("LangGraph adapter local demo summary:")
    for key, value in summary.items():
        print(f"- {key}: {value}")
