"""CI-facing replay verdict assertion and JSON parity tests."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest

from kitaru._experiments._views import ExperimentReplayResult
from kitaru.scoring import ExperimentVerdict


class _Dump:
    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload

    def model_dump(self, *, mode: str) -> dict[str, Any]:
        assert mode == "json"
        return dict(self.payload)


def _result(verdict: ExperimentVerdict) -> ExperimentReplayResult:
    objective = _Dump({"mean": 0.75, "minimum_mean": 0.9, "passed": False})
    failed = SimpleNamespace(
        passed=False,
        model_dump=lambda *, mode: {
            "protection_id": "safe-output",
            "minimum": 0.0,
            "passed": False,
        },
    )
    verdict_result = SimpleNamespace(
        verdict=verdict,
        objective=objective,
        protections=[failed],
    )
    operational_limit = SimpleNamespace(
        facts=SimpleNamespace(one_trial_may_overshoot=True),
        model_dump=lambda *, mode: {
            "verified": True,
            "stopped": False,
            "facts": {
                "incurred_cost_usd": 0.42,
                "cost_complete": True,
                "one_trial_may_overshoot": True,
            },
        },
    )
    record = SimpleNamespace(
        spec=SimpleNamespace(
            suite_key="support-regression",
            experiment_id="exp-attempt",
        ),
        verdict=verdict_result,
        operational_limit=operational_limit,
        counts=_Dump(
            {
                "target_count": 2,
                "intended": 2,
                "submitted": 2,
                "verified": 2,
                "skipped": 0,
                "failed": 0,
                "unverified": 0,
            }
        ),
        model_dump=lambda *, mode: {"status": "completed"},
    )
    submission = SimpleNamespace(
        compare_url="https://example.test/compare",
        to_json=lambda: {"compare_url": "https://example.test/compare"},
    )
    return ExperimentReplayResult(
        record=cast(Any, record),
        submission=cast(Any, submission),
        runs=cast(Any, object()),
    )


def test_assert_pass_reports_the_same_structured_facts_as_json() -> None:
    result = _result(ExperimentVerdict.HOLD)

    with pytest.raises(AssertionError) as exc_info:
        result.assert_pass()

    payload = result.to_json()
    summary = payload["regression"]
    message = str(exc_info.value)
    assert summary == result.regression_summary()
    assert summary["suite_key"] == "support-regression"
    assert summary["attempt_id"] == "exp-attempt"
    assert summary["verdict"] == "hold"
    assert summary["objective"]["passed"] is False
    assert summary["failed_protections"][0]["protection_id"] == "safe-output"
    assert summary["incomplete_counts"]["verified"] == 2
    assert summary["operational_limit"]["facts"]["incurred_cost_usd"] == 0.42
    assert summary["compare_url"] == "https://example.test/compare"
    assert "one trial may cross a ceiling" in summary["limit_note"]
    assert '"suite_key": "support-regression"' in message
    assert '"verdict": "hold"' in message
    assert "https://example.test/compare" in message


def test_assert_pass_returns_normally_for_pass_verdict() -> None:
    _result(ExperimentVerdict.PASS).assert_pass()
