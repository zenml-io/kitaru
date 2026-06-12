"""Live LangGraph requirements-triage flow for Replay Lab model swaps.

The important replay detail is that ``requirements_triage_case`` receives a
Kitaru model alias as normal flow input. Each execution builds a fresh LangGraph
runner inside the flow body, and the graph calls ``kitaru.llm(model=model)`` from
that alias. Candidate replay can therefore swap aliases without fighting a
module-level model object that was created before replay started.
"""

import argparse
import time
from typing import Any, cast

from typing_extensions import TypedDict

import kitaru
from kitaru import flow
from kitaru.adapters.langgraph import KitaruGraphRunner, LangGraphRunRequest
from kitaru.config import resolve_model_selection
from kitaru.errors import KitaruUsageError

try:  # Package import path used by tests and repo-root execution.
    from .requirements_cases import get_case
except ImportError:  # Direct script path used by direct example execution.
    from requirements_cases import get_case  # type: ignore[no-redef]

RUNNER_NAME = "requirements_triage"
REPLAY_ANCHOR = "requirements_triage_langgraph_call"
SUMMARY_ARTIFACT = "scorecard"
FINAL_RESPONSE_ARTIFACT = "final_response"
SYSTEM_PROMPT = (
    "You are a careful requirements triage assistant. Return sectioned text, not "
    "JSON. Use these exact headings: Summary, Known requirements, Missing "
    "information, Risks, Recommended next action. Say what is missing instead of "
    "pretending the request is complete."
)


class TriageState(TypedDict, total=False):
    case: dict[str, Any]
    known_requirements: str
    gaps_and_risks: str
    final_response: str


@flow(cache=False)
def requirements_triage_case(case_id: str, model: str) -> str:
    """Run one live requirements-triage case through a LangGraph graph.

    The LangGraph call (wrapped by the Kitaru adapter) is the single checkpoint
    and the flow's result sink. The graph node saves the Replay Lab artifacts,
    so there is no second terminal checkpoint to disambiguate.
    """
    _require_registered_alias(model)
    runner = KitaruGraphRunner(
        _build_requirements_graph(model),
        name=RUNNER_NAME,
        # One checkpoint for the whole graph; the 3 reasoning model calls are
        # tracked as child events under it. (Per-call checkpoints would each be
        # an unchained terminal, so the flow result can't be auto-extracted.)
        checkpoint_strategy="graph_call",
    )
    result = runner.invoke(
        LangGraphRunRequest.start(
            {"case": get_case(case_id)},
            thread_id=f"requirements-triage-{case_id}",
        )
    )
    if result.status != "completed":
        raise RuntimeError(f"Expected completed LangGraph run, got: {result.status}")
    output = cast(dict[str, Any], result.output)
    final_response = str(output.get("final_response") or "")
    if not final_response.strip():
        raise RuntimeError("Requirements triage graph returned an empty response.")
    return final_response


def _build_requirements_graph(model: str) -> Any:
    """Build a fresh LangGraph graph for one execution/replay."""
    try:
        from langgraph.checkpoint.memory import InMemorySaver
        from langgraph.graph import END, START, StateGraph
    except ImportError as error:  # pragma: no cover - exercised only without extra.
        raise SystemExit(
            "Missing LangGraph dependencies. Install them with:\n"
            "  uv sync --extra local --extra langgraph-openai\n"
            "or another LangGraph provider extra used by your model aliases."
        ) from error

    def extract_requirements(state: TriageState) -> TriageState:
        """Step 1: pull the known requirements out of the raw request."""
        case = state["case"]
        known = kitaru.llm(
            f"Engineering request:\n{case['request']}\n\n"
            "List ONLY the known, explicitly-stated requirements as short "
            "bullet points. Do not invent anything.",
            model=model,
            system="You are a careful engineering requirements analyst.",
            temperature=0,
            max_tokens=400,
            name="extract_requirements",
        )
        return {"known_requirements": known}

    def find_gaps_and_risks(state: TriageState) -> TriageState:
        """Step 2: given the requirements, name missing info and risks."""
        case = state["case"]
        gaps = kitaru.llm(
            f"Engineering request:\n{case['request']}\n\n"
            f"Known requirements:\n{state.get('known_requirements', '')}\n\n"
            "Now list, under headings 'Missing information' and 'Risks', what is "
            "missing and what could go wrong. For safety-critical or load-bearing "
            "parts, call out that an independent sign-off is required.",
            model=model,
            system="You are a careful engineering requirements analyst.",
            temperature=0,
            max_tokens=500,
            name="find_gaps_and_risks",
        )
        return {"gaps_and_risks": gaps}

    def recommend_next_action(state: TriageState) -> TriageState:
        """Step 3: recommend the next action and assemble the final triage."""
        case = state["case"]
        recommendation = kitaru.llm(
            f"Engineering request:\n{case['request']}\n\n"
            f"Known requirements:\n{state.get('known_requirements', '')}\n\n"
            f"Gaps and risks:\n{state.get('gaps_and_risks', '')}\n\n"
            "Give a single 'Recommended next action'. If the part is "
            "safety-critical, require an independent sign-off before approval.",
            model=model,
            system="You are a careful engineering requirements analyst.",
            temperature=0,
            max_tokens=300,
            name="recommend_next_action",
        )
        final_response = (
            f"Known requirements\n{state.get('known_requirements', '')}\n\n"
            f"{state.get('gaps_and_risks', '')}\n\n"
            f"Recommended next action\n{recommendation}"
        )
        # Runs inside the single graph-call checkpoint, so saving the artifacts
        # Replay Lab compares is in scope here.
        kitaru.save(
            SUMMARY_ARTIFACT,
            {
                "case_id": case["case_id"],
                "scenario_version": "requirements_triage_v1",
                "model_alias": model,
                "llm_call_count": 3,
                "tool_call_count": 0,
            },
            type="output",
        )
        kitaru.save(FINAL_RESPONSE_ARTIFACT, final_response, type="response")
        return {"final_response": final_response}

    builder = StateGraph(TriageState)
    builder.add_node("extract_requirements", extract_requirements)
    builder.add_node("find_gaps_and_risks", find_gaps_and_risks)
    builder.add_node("recommend_next_action", recommend_next_action)
    builder.add_edge(START, "extract_requirements")
    builder.add_edge("extract_requirements", "find_gaps_and_risks")
    builder.add_edge("find_gaps_and_risks", "recommend_next_action")
    builder.add_edge("recommend_next_action", END)
    # A checkpointer is required: the Kitaru adapter invokes the graph with a
    # thread_id, and recent LangGraph versions error (missing _put_checkpoint_fut)
    # if the compiled graph has no checkpointer.
    return builder.compile(checkpointer=InMemorySaver())


def _require_registered_alias(model: str) -> None:
    """Reject concrete provider names and require a configured Kitaru alias."""
    if "/" in model:
        raise KitaruUsageError(
            "The requirements-triage demo expects a Kitaru model alias, not a "
            "provider/model string. Register an alias with `kitaru model register` "
            "and pass the alias here."
        )
    selection = resolve_model_selection(model)
    if selection.alias is None:
        raise KitaruUsageError(
            f"Model `{model}` is not a registered Kitaru alias. Register it first "
            "with `kitaru model register <alias> --model <provider/model>`."
        )


def run_workflow(case_id: str, model: str) -> tuple[str, str]:
    """Run the flow and return execution ID plus final response."""
    handle = requirements_triage_case.run(case_id, model=model)
    while not handle.status.is_finished:
        time.sleep(1)
    if not handle.status.is_successful:
        raise RuntimeError(f"Flow failed with status: {handle.status.value}")
    return handle.exec_id, str(handle.wait())


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse example CLI arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("case_id", nargs="?", default="onboarding-workflow-access")
    parser.add_argument(
        "--model",
        required=True,
        help="Kitaru model alias to resolve at execution time.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run one requirements-triage case and print the result."""
    args = parse_args(argv)
    execution_id, final_response = run_workflow(args.case_id, args.model)
    print(f"Execution: {execution_id}")
    print(final_response)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
