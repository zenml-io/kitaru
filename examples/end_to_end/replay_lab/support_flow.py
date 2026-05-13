"""Deterministic support-agent flow for the Replay Lab end-to-end demo.

Run one observed champion execution::

    uv run python support_flow.py support-refund-delay

For the full Replay Lab path, use ``seed_observed.py`` and
``run_replay_lab.py`` instead. This file contains the durable flow that those
scripts run and replay.
"""

import argparse
from typing import Any

import kitaru
from kitaru import checkpoint, flow

try:  # Package import path used by tests and repo-root execution.
    from .scenarios import build_draft_response, evaluate_draft, get_scenario
except ImportError:  # Direct script path used by README commands.
    from scenarios import build_draft_response, evaluate_draft, get_scenario


@checkpoint
def load_support_case(case_id: str) -> dict[str, Any]:
    """Load one synthetic support case by stable ID."""
    return get_scenario(case_id)


@checkpoint
def draft_response(
    scenario: dict[str, Any],
    agent_profile: str = "champion",
) -> dict[str, Any]:
    """Draft a deterministic support response.

    ``agent_profile`` is a normal flow input. Replay Lab's candidate descriptor
    can set it to ``"candidate"`` when replaying from this checkpoint, without
    making Replay Lab itself understand anything about this demo's internals.
    """
    return build_draft_response(scenario, agent_profile)


@checkpoint
def score_response(
    draft: dict[str, Any],
    scenario: dict[str, Any],
) -> dict[str, Any]:
    """Evaluate the draft and return a scorecard-like dictionary."""
    return evaluate_draft(draft, scenario)


@checkpoint
def publish_response(
    draft: dict[str, Any],
    scorecard: dict[str, Any],
) -> str:
    """Publish final artifacts that Replay Lab knows how to compare."""
    final_response = str(draft["response"])
    kitaru.save("scorecard", scorecard, type="output")
    kitaru.save("final_response", final_response, type="response")
    return final_response


@flow(cache=False)
def support_replay_lab_case(
    case_id: str,
    agent_profile: str = "champion",
) -> str:
    """Run one deterministic support case through stable checkpoints."""
    scenario = load_support_case(case_id)
    draft = draft_response(scenario, agent_profile=agent_profile)
    scorecard = score_response(draft, scenario)
    return publish_response(draft, scorecard)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "case_id",
        nargs="?",
        default="support-refund-delay",
        help="Synthetic case ID to run.",
    )
    parser.add_argument(
        "--agent-profile",
        choices=("champion", "candidate"),
        default="champion",
        help="Deterministic support-agent profile to use.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run one case and print its execution ID and final response."""
    args = parse_args(argv)
    handle = support_replay_lab_case.run(
        args.case_id,
        agent_profile=args.agent_profile,
    )
    final_response = handle.wait()
    print(f"Execution: {handle.exec_id}")
    print(final_response)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
