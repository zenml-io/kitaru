"""Record a deterministic LangGraph invocation in Kitaru."""

import os
import uuid
from typing import Any, NotRequired, TypedDict

from kitaru_langgraph import KitaruGraphRunner
from langgraph.graph import END, START, StateGraph


class SupportState(TypedDict):
    """State passed through the deterministic support graph."""

    request: str
    normalized_request: NotRequired[str]
    response: NotRequired[str]


def normalize_request(state: SupportState) -> dict[str, str]:
    """Normalize whitespace in the support request."""
    return {"normalized_request": " ".join(state["request"].split())}


def draft_response(state: SupportState) -> dict[str, str]:
    """Build a deterministic response from the normalized request."""
    return {"response": f"Recorded request: {state['normalized_request']}"}


def build_graph() -> Any:
    """Compile the provider-free example graph."""
    builder = StateGraph(SupportState)
    builder.add_node("normalize_request", normalize_request)
    builder.add_node("draft_response", draft_response)
    builder.add_edge(START, "normalize_request")
    builder.add_edge("normalize_request", "draft_response")
    builder.add_edge("draft_response", END)
    return builder.compile()


def _get_optional_uuid(name: str) -> uuid.UUID | None:
    value = os.environ.get(name)
    if value is None:
        return None
    try:
        return uuid.UUID(value)
    except ValueError as error:
        raise RuntimeError(f"{name} must contain a UUID") from error


def main() -> None:
    """Run the graph once and print its deterministic response."""
    agent_id = _get_optional_uuid("KITARU_AGENT_ID")
    agent_version_id = _get_optional_uuid("KITARU_AGENT_VERSION_ID")
    if (
        not os.environ.get("KITARU_TASK_ID")
        and agent_id is None
        and agent_version_id is None
    ):
        raise RuntimeError(
            "Set KITARU_AGENT_ID or KITARU_AGENT_VERSION_ID for a local run"
        )

    runner = KitaruGraphRunner(
        build_graph(),
        agent_id=agent_id,
        agent_version_id=agent_version_id,
        session_name="langgraph-v2-recording-example",
    )
    result = runner.invoke({"request": "  Reset   my password  "})
    print(result["response"])


if __name__ == "__main__":
    main()
