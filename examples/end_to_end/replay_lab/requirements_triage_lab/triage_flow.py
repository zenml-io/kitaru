"""Deterministic requirements-triage flow for the Replay Lab demo.

The flow is durable and fully deterministic, so the demo is identical every run.
``agent_profile`` is a normal flow input: Replay Lab's candidate descriptor sets
it to ``"candidate"`` when replaying from the ``draft_response`` checkpoint, so
only the model behavior changes between baseline and candidate lanes.

The same wrapping works for a real LangGraph or PydanticAI agent via the Kitaru
adapters; here the agent is deterministic so the demo never depends on a live
model or network.
"""

import argparse
import sys
from pathlib import Path
from typing import Any

import kitaru
from kitaru import checkpoint, flow

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parent))

try:
    from .requirements_scenarios import (
        build_draft_response,
        evaluate_draft,
        get_scenario,
    )
except ImportError:
    from requirements_scenarios import (  # type: ignore[no-redef]
        build_draft_response,
        evaluate_draft,
        get_scenario,
    )

REPLAY_ANCHOR = "draft_response"


@checkpoint
def load_requirements_case(case_id: str) -> dict[str, Any]:
    """Load one synthetic requirements case by stable id."""
    return get_scenario(case_id)


@checkpoint
def draft_response(
    scenario: dict[str, Any],
    agent_profile: str = "champion",
) -> dict[str, Any]:
    """Draft a deterministic triage response under the given model profile.

    This is the replay anchor: Replay Lab replays from here with the candidate
    ``agent_profile`` so only the model behavior changes.
    """
    return build_draft_response(scenario, agent_profile)


@checkpoint
def score_response(
    draft: dict[str, Any],
    scenario: dict[str, Any],
) -> dict[str, Any]:
    """Score the draft deterministically and return a scorecard."""
    return evaluate_draft(draft, scenario)


@checkpoint
def publish_response(draft: dict[str, Any], scorecard: dict[str, Any]) -> str:
    """Publish the artifacts Replay Lab compares across lanes."""
    final_response = str(draft["response"])
    kitaru.save("scorecard", scorecard, type="output")
    kitaru.save("final_response", final_response, type="response")
    return final_response


@flow(cache=False)
def requirements_triage_case(
    case_id: str,
    agent_profile: str = "champion",
) -> str:
    """Run one deterministic requirements-triage case through stable checkpoints."""
    scenario = load_requirements_case(case_id)
    draft = draft_response(scenario, agent_profile=agent_profile)
    scorecard = score_response(draft, scenario)
    return publish_response(draft, scorecard)


def main(argv: list[str] | None = None) -> int:
    """Run one case and print its execution id and final response."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("case_id", nargs="?", default="bracket-load-signoff")
    parser.add_argument(
        "--agent-profile", choices=("champion", "candidate"), default="champion"
    )
    args = parser.parse_args(argv)
    handle = requirements_triage_case.run(args.case_id, agent_profile=args.agent_profile)
    final_response = handle.wait()
    print(f"Execution: {handle.exec_id}")
    print(final_response)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
