"""Regression-limit contract and conservative accounting tests."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from kitaru._experiments._limits import RegressionLimitTracker
from kitaru.experiments import RegressionLimits
from kitaru.scoring import OperationalLimitReason


def _summary(
    *,
    cost: float = 0.0,
    tokens: int = 0,
    unpriced: int = 0,
) -> dict[str, object]:
    return {
        "cost_policy": "non_reused_is_incurred_v1",
        "display_cost_usd": cost,
        "records_without_cost_count": unpriced,
        "incurred_total_tokens": tokens,
    }


def test_regression_limits_require_a_strict_positive_trial_bound() -> None:
    assert RegressionLimits(max_trials=2).max_trials == 2
    with pytest.raises(ValidationError):
        RegressionLimits(max_trials=0)
    with pytest.raises(ValidationError):
        RegressionLimits(max_trials=True)
    with pytest.raises(ValidationError):
        RegressionLimits(max_trials=1, max_cost_usd=float("inf"))


def test_unpriced_incurred_work_stops_cost_bounded_run_as_unverified() -> None:
    tracker = RegressionLimitTracker(RegressionLimits(max_trials=3, max_cost_usd=1.0))

    reason = tracker.observe_trial([_summary(cost=0.25, unpriced=1)])
    outcome = tracker.outcome(remaining_trials=2)

    assert reason is OperationalLimitReason.COST_UNVERIFIED
    assert outcome.verified is False
    assert outcome.stopped is True
    assert outcome.facts.incurred_cost_usd == 0.25
    assert outcome.facts.cost_complete is False


def test_missing_usage_does_not_affect_duration_only_limit() -> None:
    ticks: Iterator[float] = iter([0.0, 2.0, 2.0])
    tracker = RegressionLimitTracker(
        RegressionLimits(max_trials=2, max_duration_seconds=1.0),
        clock=lambda: next(ticks),
    )

    assert (
        tracker.observe_trial([None]) is OperationalLimitReason.DURATION_LIMIT_REACHED
    )
    outcome = tracker.outcome(remaining_trials=1)

    assert outcome.verified is True
    assert outcome.facts.cost_complete is True
    assert outcome.facts.tokens_complete is True
    assert outcome.facts.duration_seconds == 2.0


def test_recovered_duration_uses_persisted_attempt_start() -> None:
    tracker = RegressionLimitTracker(
        RegressionLimits(max_trials=2, max_duration_seconds=5.0),
        clock=lambda: 10.0,
        wall_clock=lambda: datetime(2026, 7, 18, 10, 0, 10, tzinfo=UTC),
    )

    outcome = tracker.outcome(
        remaining_trials=0,
        started_at="2026-07-18T10:00:00Z",
    )

    assert outcome.reason_code is OperationalLimitReason.DURATION_LIMIT_REACHED
    assert outcome.stopped is True
    assert outcome.facts.duration_seconds == 10.0


def test_cost_and_tokens_accumulate_only_incurred_summary_fields() -> None:
    tracker = RegressionLimitTracker(
        RegressionLimits(
            max_trials=3,
            max_cost_usd=1.0,
            max_incurred_tokens=100,
        )
    )

    assert tracker.observe_trial([_summary(cost=0.2, tokens=40)]) is None
    assert tracker.observe_trial([_summary(cost=0.3, tokens=60)]) == (
        OperationalLimitReason.TOKEN_LIMIT_REACHED
    )
    outcome = tracker.outcome(remaining_trials=1)

    assert outcome.verified is True
    assert outcome.facts.incurred_cost_usd == 0.5
    assert outcome.facts.incurred_tokens == 100
    assert outcome.facts.checked_between_terminal_trials is True
    assert outcome.facts.one_trial_may_overshoot is True
