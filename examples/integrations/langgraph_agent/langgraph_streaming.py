"""Stream provider-neutral LangGraph live events through Kitaru.

Story:
- A local support-review graph looks up a ticket and drafts a handoff note.
- The graph emits custom progress from inside its nodes.
- Kitaru forwards safe LangGraph stream events while the graph-call checkpoint
  is active, then saves the final LangGraphRunResult as the durable record.

Run:
    uv sync --extra local --extra langgraph
    uv run kitaru init
    uv run kitaru login
    uv run python examples/integrations/langgraph_agent/langgraph_streaming.py
"""

import threading
import time
from typing import Any, cast

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.config import get_stream_writer
from langgraph.graph import END, START, StateGraph
from typing_extensions import TypedDict

from kitaru import flow
from kitaru.adapters.langgraph import (
    LANGGRAPH_STREAM_EVENT_KINDS,
    LANGGRAPH_STREAM_TERMINAL_EVENT_KINDS,
    KitaruGraphRunner,
    LangGraphRunRequest,
    LangGraphRunResult,
)
from kitaru.client import KitaruClient
from kitaru.errors import KitaruBackendError, KitaruFeatureNotAvailableError

THREAD_ID = "langgraph-streaming-demo-thread"
GRAPH_NAME = "langgraph_local_streaming_demo"

TICKETS: dict[str, dict[str, str]] = {
    "ticket-42": {
        "customer": "Amina",
        "issue": "Delayed shipment for order ORD-1007",
        "status": "needs_escalation",
        "priority": "high",
    }
}


class StreamingState(TypedDict, total=False):
    ticket: str
    customer: str
    issue: str
    priority: str
    review_status: str
    handoff_note: str


def build_streaming_graph() -> Any:
    """Build a tiny local graph that emits LangGraph custom stream progress."""
    builder = StateGraph(cast(Any, StreamingState))

    def lookup_ticket(state: StreamingState) -> StreamingState:
        writer = get_stream_writer()
        ticket = state["ticket"]
        writer({"step": "lookup_ticket", "message": f"Looking up {ticket}"})
        time.sleep(0.25)
        record = TICKETS[ticket]
        writer(
            {
                "step": "lookup_ticket",
                "message": f"Found {record['priority']} priority ticket",
            }
        )
        return {
            "customer": record["customer"],
            "issue": record["issue"],
            "priority": record["priority"],
            "review_status": "ticket_loaded",
        }

    def draft_handoff(state: StreamingState) -> StreamingState:
        writer = get_stream_writer()
        writer({"step": "draft_handoff", "message": "Drafting support handoff"})
        time.sleep(0.25)
        return {
            "review_status": "handoff_ready",
            "handoff_note": (
                f"Escalate {state['ticket']} for {state['customer']}: "
                f"{state['issue']} ({state['priority']} priority)."
            ),
        }

    builder.add_node("lookup_ticket", lookup_ticket)
    builder.add_node("draft_handoff", draft_handoff)
    builder.add_edge(START, "lookup_ticket")
    builder.add_edge("lookup_ticket", "draft_handoff")
    builder.add_edge("draft_handoff", END)
    return builder.compile(checkpointer=InMemorySaver())


def _event_data(payload: dict[str, Any]) -> dict[str, Any]:
    data = payload.get("data")
    return data if isinstance(data, dict) else {}


def _watch_langgraph_stream(exec_id: str, stop_event: threading.Event) -> None:
    print("\n=== live LangGraph stream events ===")
    try:
        for event in KitaruClient().executions.events(
            exec_id,
            kinds=list(LANGGRAPH_STREAM_EVENT_KINDS),
        ):
            if stop_event.is_set():
                return
            data = _event_data(event.payload)
            display = data.get("display") or event.kind
            mode = data.get("mode")
            prefix = f"[{mode}] " if isinstance(mode, str) else ""
            print(f"- {prefix}{display}")
            if event.kind in LANGGRAPH_STREAM_TERMINAL_EVENT_KINDS:
                return
    except (KitaruBackendError, KitaruFeatureNotAvailableError) as error:
        print("\nLive event watching is unavailable on this backend.")
        print(f"The durable result will still be read with .wait(): {error}")


def _print_final_result(result: LangGraphRunResult) -> None:
    print("\n=== durable LangGraphRunResult ===")
    print(f"status: {result.status}")
    print(f"thread_id: {result.thread_id}")
    print(f"latest checkpoint id: {result.latest_checkpoint_id}")
    print("final output:")
    print(result.output)


def main() -> None:
    runner = KitaruGraphRunner(
        build_streaming_graph(),
        name=GRAPH_NAME,
        checkpoint_strategy="graph_call",
    )

    @flow
    def streaming_flow(ticket: str) -> LangGraphRunResult:
        return runner.stream(
            LangGraphRunRequest.start({"ticket": ticket}, thread_id=THREAD_ID)
        )

    handle = streaming_flow.run("ticket-42", cache=False)
    exec_id = handle.exec_id
    print(f"Submitted execution: {exec_id}")

    stop_watching = threading.Event()
    watcher = threading.Thread(
        target=_watch_langgraph_stream,
        args=(exec_id, stop_watching),
        daemon=True,
    )
    watcher.start()

    wait_value = handle.wait()
    stop_watching.set()
    watcher.join(timeout=1.0)
    if watcher.is_alive():
        print("\nLive watcher is still open; showing the durable result now.")

    if isinstance(wait_value, LangGraphRunResult):
        result = wait_value
    else:
        model_dump = getattr(wait_value, "model_dump", None)
        result = LangGraphRunResult.model_validate(
            model_dump(mode="python") if callable(model_dump) else wait_value
        )
    _print_final_result(result)


if __name__ == "__main__":
    main()
