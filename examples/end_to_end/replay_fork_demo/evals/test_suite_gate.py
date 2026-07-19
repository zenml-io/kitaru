"""CI gate: rerun the frozen `account-setting-fix` suite against this checkout.

The suite, its recorded boundary, its objective, and its protection were
frozen when the attempt was created (README step 8). This test replays that
frozen request with the current checkout as the candidate and fails unless
the verdict is PASS, inside a hard spend ceiling.

It performs a live model call, so it is opt-in: run it with

    KITARU_SUITE_GATE=1 uv run pytest evals/test_suite_gate.py -q

after completing README steps 1-8 in the same environment.
"""

import os

import pytest

from kitaru import RegressionLimits

pytestmark = pytest.mark.skipif(
    os.environ.get("KITARU_SUITE_GATE") != "1",
    reason="Suite-gate reruns call the configured OpenAI model; set KITARU_SUITE_GATE=1.",
)


def test_account_setting_fix_suite_gate() -> None:
    from evals.register import mini_tool_budget_2_agent, support_resolution_objective

    candidate_label = f"ci-{os.environ.get('GITHUB_SHA', 'local')[:12]}"
    mini_tool_budget_2_agent.register(
        label=candidate_label,
        entrypoint="evals.register:mini_tool_budget_2_agent",
    )
    result = mini_tool_budget_2_agent.replay(
        experiment="account-setting-fix",
        idempotency_key=f"suite-gate-{candidate_label}",
        repeats=1,
        scorers=[support_resolution_objective],
        limits=RegressionLimits(max_trials=1, max_cost_usd=0.10),
    )
    result.assert_pass()
