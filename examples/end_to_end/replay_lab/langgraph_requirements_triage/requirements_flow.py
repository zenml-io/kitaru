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
from kitaru import checkpoint, flow
from kitaru.adapters.langgraph import KitaruGraphRunner, LangGraphRunRequest
from kitaru.config import resolve_model_selection
from kitaru.errors import KitaruUsageError

try:  # Package import path used by tests and repo-root execution.
    from .requirements_cases import build_triage_prompt, get_case
except ImportError:  # Direct script path used by direct example execution.
    from requirements_cases import (  # type: ignore[no-redef]
        build_triage_prompt,
        get_case,
    )

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
    final_response: str


@checkpoint
def load_requirements_case(case_id: str) -> dict[str, Any]:
    """Load one synthetic requirements-triage case by stable ID."""
    return get_case(case_id)


@checkpoint
def publish_triage_result(
    *,
    case: dict[str, Any],
    final_response: str,
    model: str,
) -> str:
    """Save artifacts that Replay Lab and the evaluator can inspect."""
    scorecard = {
        "case_id": case["case_id"],
        "scenario_version": "requirements_triage_v1",
        "model_alias": model,
        "llm_call_count": 1,
        "tool_call_count": 0,
    }
    kitaru.save(SUMMARY_ARTIFACT, scorecard, type="output")
    kitaru.save(FINAL_RESPONSE_ARTIFACT, final_response, type="response")
    return final_response


@flow(cache=False)
def requirements_triage_case(case_id: str, model: str) -> str:
    """Run one live requirements-triage case through a LangGraph graph."""
    _require_registered_alias(model)
    case = load_requirements_case(case_id)
    runner = KitaruGraphRunner(
        _build_requirements_graph(model),
        name=RUNNER_NAME,
        checkpoint_strategy="graph_call",
    )
    result = runner.invoke(
        LangGraphRunRequest.start(
            {"case": case},
            thread_id=f"requirements-triage-{case_id}",
        )
    )
    if result.status != "completed":
        raise RuntimeError(f"Expected completed LangGraph run, got: {result.status}")
    output = cast(dict[str, Any], result.output)
    final_response = str(output.get("final_response") or "")
    if not final_response.strip():
        raise RuntimeError("Requirements triage graph returned an empty response.")
    return publish_triage_result(
        case=case,
        final_response=final_response,
        model=model,
    )


def _build_requirements_graph(model: str) -> Any:
    """Build a fresh LangGraph graph for one execution/replay."""
    try:
        from langgraph.graph import END, START, StateGraph
    except ImportError as error:  # pragma: no cover - exercised only without extra.
        raise SystemExit(
            "Missing LangGraph dependencies. Install them with:\n"
            "  uv sync --extra local --extra langgraph-openai\n"
            "or another LangGraph provider extra used by your model aliases."
        ) from error

    def triage_requirements(state: TriageState) -> TriageState:
        case = state["case"]
        response = kitaru.llm(
            build_triage_prompt(case),
            model=model,
            system=SYSTEM_PROMPT,
            temperature=0,
            max_tokens=700,
            name="requirements_triage_model_call",
        )
        return {"final_response": response}

    builder = StateGraph(TriageState)
    builder.add_node("triage_requirements", triage_requirements)
    builder.add_edge(START, "triage_requirements")
    builder.add_edge("triage_requirements", END)
    return builder.compile()


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
