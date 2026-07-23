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
"""Client-side scoring of sessions with user-defined scorer functions."""

import importlib
from collections.abc import Callable

from pydantic import BaseModel

from kitaru.api_models.v1.replays import ScorerConfig, ScoringPolicy
from kitaru.api_models.v1.session_nodes import SessionNodeResponse
from kitaru.api_models.v1.sessions import SessionResponse


class ScoringError(Exception):
    """Raised when a scorer cannot be loaded or does not produce a valid score."""


class SessionView(BaseModel):
    """Session view."""

    session: SessionResponse
    nodes: list[SessionNodeResponse]


class ScoringResult(BaseModel):
    """Scoring result."""

    passed: bool
    score: float
    scores: dict[str, float]


def load_scorer(source: str) -> Callable[..., float]:
    """Import a scorer function from a source reference.

    A scorer is called as ``score(session: SessionView, **params)`` and
    returns a float in 0..1.

    Args:
        source: Scorer reference as ``module:attribute``.

    Raises:
        ScoringError: The source is malformed, the module does not import,
            or the attribute is missing or not callable.

    Returns:
        Scorer function.
    """
    module_name, separator, attribute = source.partition(":")
    if not separator or not module_name or not attribute:
        raise ScoringError(
            f"Invalid scorer source {source!r}, expected 'module:attribute'"
        )
    try:
        module = importlib.import_module(module_name)
    except ImportError as exc:
        raise ScoringError(
            f"Failed to import scorer module {module_name!r}: {exc}"
        ) from exc
    try:
        scorer = getattr(module, attribute)
    except AttributeError as exc:
        raise ScoringError(
            f"Module {module_name!r} has no attribute {attribute!r}"
        ) from exc
    if not callable(scorer):
        raise ScoringError(f"Scorer {source!r} is not callable")
    return scorer


def run_scorer(config: ScorerConfig, session: SessionView) -> float:
    """Run a single scorer against a session view.

    Args:
        config: Scorer configuration.
        session: Session view to score.

    Raises:
        ScoringError: The scorer failed to load, raised, or returned a
            value outside 0..1.

    Returns:
        Score in 0..1.
    """
    scorer = load_scorer(config.source)
    try:
        value = scorer(session, **config.params)
    except Exception as exc:
        raise ScoringError(
            f"Scorer {config.name!r} raised {type(exc).__name__}: {exc}"
        ) from exc
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ScoringError(
            f"Scorer {config.name!r} returned {value!r}, expected a float in 0..1"
        )
    if not 0 <= value <= 1:
        raise ScoringError(
            f"Scorer {config.name!r} returned {value}, expected a value in 0..1"
        )
    return float(value)


def evaluate_scoring_policy(
    policy: ScoringPolicy, session: SessionView
) -> ScoringResult:
    """Evaluate a scoring policy against a session view.

    Any scorer at or below its ``fail_below`` fails the result outright.
    Otherwise the weighted average of all scores must reach
    ``pass_threshold``.

    Args:
        policy: Scoring policy to evaluate.
        session: Session view to score.

    Raises:
        ScoringError: A scorer failed to load, raised, or returned a value
            outside 0..1, or the total scorer weight is 0.

    Returns:
        Scoring result.
    """
    scores = {config.name: run_scorer(config, session) for config in policy.scorers}
    total_weight = sum(config.weight for config in policy.scorers)
    if total_weight <= 0:
        raise ScoringError("Scoring policy has a total scorer weight of 0")
    score = (
        sum(scores[config.name] * config.weight for config in policy.scorers)
        / total_weight
    )
    hard_failed = any(
        config.fail_below is not None and scores[config.name] <= config.fail_below
        for config in policy.scorers
    )
    passed = not hard_failed and score >= policy.pass_threshold
    return ScoringResult(passed=passed, score=score, scores=scores)
