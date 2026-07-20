# ruff: noqa: E402
"""Provider-extended regression gate for the replay-fork support agent."""

import importlib
import os
from typing import Any, cast

import pytest

from kitaru import RegressionLimits

pytestmark = [
    pytest.mark.live_llm,
    pytest.mark.live_openai,
    pytest.mark.provider_extended,
]

pytest.importorskip("pydantic_ai")

from tests.replay_fork_support import bootstrap_account_setting_comparable_suite


def test_account_setting_fix_suite_gate(
    monkeypatch: pytest.MonkeyPatch,
    primed_zenml: None,
    request: pytest.FixtureRequest,
) -> None:
    """Rerun a frozen comparable suite with the current OpenAI candidate."""
    del primed_zenml
    bootstrap = bootstrap_account_setting_comparable_suite(
        monkeypatch,
        request,
        preserve_openai_api_key=True,
    )
    registration = cast(Any, importlib.import_module("evals.register"))
    candidate = registration.mini_tool_budget_2_agent
    candidate_suffix = os.environ.get("GITHUB_SHA", "local")[:12]
    candidate_label = f"ci-{candidate_suffix}"
    idempotency_key = f"suite-gate-{candidate_label}"
    candidate.register(
        label=candidate_label,
        entrypoint="evals.register:mini_tool_budget_2_agent",
    )

    replay_request = {
        "experiment": bootstrap.model_resumed.spec.experiment_id,
        "idempotency_key": idempotency_key,
        "repeats": 1,
        "scorers": [registration.support_resolution_objective],
        "limits": RegressionLimits(
            max_trials=1,
            max_cost_usd=0.10,
            max_incurred_tokens=100_000,
            max_duration_seconds=300,
        ),
    }
    result = candidate.replay(**replay_request)
    result.assert_pass()

    repeated = candidate.replay(**replay_request)

    assert repeated.spec.experiment_id == result.spec.experiment_id
    assert repeated.record == result.record
