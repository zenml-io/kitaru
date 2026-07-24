#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Tests for client-side scoring."""

import uuid
from datetime import UTC, datetime
from typing import Any

import pytest

from kitaru.api_models.v1.jobs import ScorerConfig, ScoringPolicy
from kitaru.api_models.v1.sessions import (
    SessionOrigin,
    SessionResponse,
    SessionStatus,
)
from kitaru.scoring import (
    ScoringError,
    SessionView,
    evaluate_scoring_policy,
    load_scorer,
    run_scorer,
)


def constant_scorer(session: SessionView, value: float = 1.0) -> float:
    """Return a configured constant score."""
    return value


def raising_scorer(session: SessionView) -> float:
    """Raise an error."""
    raise RuntimeError("boom")


def string_scorer(session: SessionView) -> Any:
    """Return a non-numeric score."""
    return "high"


NOT_CALLABLE = "not callable"


def make_view() -> SessionView:
    """Build a session view around a minimal completed session."""
    now = datetime.now(UTC)
    session = SessionResponse(
        id=uuid.uuid4(),
        owner_id=uuid.uuid4(),
        agent_id=uuid.uuid4(),
        agent_version_id=None,
        origin=SessionOrigin.RECORDED,
        status=SessionStatus.COMPLETED,
        name=None,
        inputs=None,
        outputs=None,
        expected=None,
        error=None,
        started_at=None,
        ended_at=None,
        external_id=None,
        metadata={},
        provider=None,
        framework=None,
        adapter_version=None,
        log_uri=None,
        scores={},
        cost=None,
        tokens=None,
        llm_call_count=0,
        tool_call_count=0,
        created=now,
        updated=now,
    )
    return SessionView(session=session, nodes=[])


def make_scorer(
    name: str,
    value: float,
    weight: float = 1.0,
    fail_below: float | None = None,
) -> ScorerConfig:
    """Build a constant scorer configuration."""
    return ScorerConfig(
        name=name,
        source="test_scoring:constant_scorer",
        params={"value": value},
        weight=weight,
        fail_below=fail_below,
    )


def test_load_scorer() -> None:
    """Import the referenced function."""
    assert load_scorer("test_scoring:constant_scorer") is constant_scorer


@pytest.mark.parametrize("source", ["noseparator", ":attribute", "module:"])
def test_load_scorer_malformed_source(source: str) -> None:
    """Reject sources that are not 'module:attribute'."""
    with pytest.raises(ScoringError, match="expected 'module:attribute'"):
        load_scorer(source)


def test_load_scorer_missing_module() -> None:
    """Reject a module that does not import."""
    with pytest.raises(ScoringError, match="Failed to import scorer module"):
        load_scorer("kitaru_missing_module:scorer")


def test_load_scorer_missing_attribute() -> None:
    """Reject a missing attribute."""
    with pytest.raises(ScoringError, match="has no attribute"):
        load_scorer("test_scoring:missing_scorer")


def test_load_scorer_not_callable() -> None:
    """Reject a non-callable attribute."""
    with pytest.raises(ScoringError, match="is not callable"):
        load_scorer("test_scoring:NOT_CALLABLE")


def test_run_scorer_passes_params() -> None:
    """Call the scorer with the session view and configured params."""
    assert run_scorer(make_scorer("quality", 0.3), make_view()) == 0.3


@pytest.mark.parametrize("value", [-0.1, 1.5])
def test_run_scorer_out_of_range(value: float) -> None:
    """Reject scores outside 0..1."""
    with pytest.raises(ScoringError, match=r"expected a value in 0\.\.1"):
        run_scorer(make_scorer("quality", value), make_view())


def test_run_scorer_non_numeric() -> None:
    """Reject a non-numeric score."""
    config = ScorerConfig(name="quality", source="test_scoring:string_scorer")
    with pytest.raises(ScoringError, match=r"expected a float in 0\.\.1"):
        run_scorer(config, make_view())


def test_run_scorer_exception_propagates() -> None:
    """Wrap a raising scorer in a ScoringError naming the scorer."""
    config = ScorerConfig(name="quality", source="test_scoring:raising_scorer")
    with pytest.raises(ScoringError, match="'quality' raised RuntimeError: boom"):
        run_scorer(config, make_view())


def test_evaluate_weighted_average() -> None:
    """Weight scores by their normalized weights."""
    policy = ScoringPolicy(
        scorers=[
            make_scorer("low", 0.2, weight=1.0),
            make_scorer("high", 1.0, weight=3.0),
        ],
        pass_threshold=0.5,
    )
    result = evaluate_scoring_policy(policy, make_view())
    assert result.score == pytest.approx(0.8)
    assert result.scores == {"low": 0.2, "high": 1.0}
    assert result.passed is True


def test_evaluate_pass_threshold_equality_passes() -> None:
    """Pass when the weighted average equals the threshold."""
    policy = ScoringPolicy(scorers=[make_scorer("quality", 0.5)], pass_threshold=0.5)
    assert evaluate_scoring_policy(policy, make_view()).passed is True


def test_evaluate_below_pass_threshold_fails() -> None:
    """Fail when the weighted average stays below the threshold."""
    policy = ScoringPolicy(scorers=[make_scorer("quality", 0.49)], pass_threshold=0.5)
    result = evaluate_scoring_policy(policy, make_view())
    assert result.passed is False
    assert result.score == pytest.approx(0.49)


def test_evaluate_fail_below_equality_fails() -> None:
    """Fail outright when a score equals its fail_below."""
    policy = ScoringPolicy(
        scorers=[
            make_scorer("protector", 0.5, fail_below=0.5),
            make_scorer("high", 1.0, weight=9.0),
        ],
        pass_threshold=0.5,
    )
    result = evaluate_scoring_policy(policy, make_view())
    assert result.passed is False
    assert result.score == pytest.approx(0.95)


def test_evaluate_above_fail_below_passes() -> None:
    """Pass when every score stays above its fail_below."""
    policy = ScoringPolicy(
        scorers=[make_scorer("protector", 0.51, fail_below=0.5)],
        pass_threshold=0.5,
    )
    assert evaluate_scoring_policy(policy, make_view()).passed is True


def test_evaluate_zero_total_weight() -> None:
    """Reject a policy whose scorer weights sum to 0."""
    policy = ScoringPolicy(
        scorers=[make_scorer("quality", 1.0, weight=0.0)], pass_threshold=0.5
    )
    with pytest.raises(ScoringError, match="total scorer weight"):
        evaluate_scoring_policy(policy, make_view())


def test_evaluate_scorer_exception_propagates() -> None:
    """Propagate a raising scorer as a ScoringError."""
    policy = ScoringPolicy(
        scorers=[ScorerConfig(name="quality", source="test_scoring:raising_scorer")],
        pass_threshold=0.5,
    )
    with pytest.raises(ScoringError, match="'quality' raised RuntimeError: boom"):
        evaluate_scoring_policy(policy, make_view())
