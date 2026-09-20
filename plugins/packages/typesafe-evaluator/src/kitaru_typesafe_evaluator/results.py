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
"""Map jev answers onto Kitaru evaluation results."""

from collections.abc import Iterable
from typing import Literal

from typesafe_sdk import ChoiceAnswer, NoulAnswer, ScoreAnswer

from kitaru.api_models.v1.evaluation import EvaluationResult
from kitaru_typesafe_evaluator.params import ChoiceQuestion, NoulQuestion, ScoreQuestion

# TypeSafe advises never acting on a choice whose confidence is under 0.5.
CHOICE_MIN_CONFIDENCE = 0.5

Answer = NoulAnswer | ChoiceAnswer | ScoreAnswer
Question = NoulQuestion | ChoiceQuestion | ScoreQuestion


def decide_noul_verdict(
    probability_yes: float,
    pass_when: Literal["yes", "no"] | None,
    decisive_at: float,
) -> bool | None:
    """Turn jev's probability of yes into pass, fail, or held.

    Args:
        probability_yes: Probability jev gave to the answer yes.
        pass_when: Answer that counts as passing, or None for no verdict.
        decisive_at: Probability the good answer must reach to pass. The
            bad answer fails the check at the same level.

    Returns:
        True for pass, False for fail, None when held or when pass_when is None.
    """
    if pass_when is None:
        return None
    good = probability_yes if pass_when == "yes" else 1 - probability_yes
    # Round before comparing at the band edges: 1 - 0.8 is 0.19999999999999996
    # in binary floats, so an unrounded 0.20 would be held instead of failed.
    good = round(good, 9)
    bad = round(1 - good, 9)
    if good >= decisive_at:
        return True
    if bad >= decisive_at:
        return False
    return None


def _explain_noul(
    question: NoulQuestion, probability_yes: float, passed: bool | None
) -> str:
    """Explain a noul verdict in terms of the probability band that decided it."""
    if question.pass_when is None:
        return f"p(yes)={probability_yes:.2f}"
    low, high = 1 - question.decisive_at, question.decisive_at
    if passed is None:
        return f"p(yes)={probability_yes:.2f} · held: between {low:.2f} and {high:.2f}"
    good = probability_yes if question.pass_when == "yes" else 1 - probability_yes
    if passed:
        return (
            f"p(yes)={probability_yes:.2f} · pass: p({question.pass_when})={good:.2f} "
            f"is at or above {high:.2f}"
        )
    return (
        f"p(yes)={probability_yes:.2f} · fail: p({question.pass_when})={good:.2f} "
        f"is at or below {low:.2f}"
    )


def build_result(
    name: str, question: Question, answer: Answer, model: str
) -> EvaluationResult:
    """Build the evaluation result for one answered question."""
    if isinstance(question, NoulQuestion) and isinstance(answer, NoulAnswer):
        passed = decide_noul_verdict(
            answer.noul, question.pass_when, question.decisive_at
        )
        return EvaluationResult(
            name=name,
            score=answer.noul,
            min_score=0.0,
            max_score=1.0,
            passed=passed,
            explanation=f"{model} · {_explain_noul(question, answer.noul, passed)}",
        )
    if isinstance(question, ChoiceQuestion) and isinstance(answer, ChoiceAnswer):
        sure = answer.confidence >= CHOICE_MIN_CONFIDENCE
        passed = (
            answer.choice in question.pass_when if question.pass_when and sure else None
        )
        return EvaluationResult(
            name=name,
            value=answer.choice,
            score=answer.confidence,
            passed=passed,
            explanation=f"{model} · confidence={answer.confidence:.2f}",
        )
    if isinstance(question, ScoreQuestion) and isinstance(answer, ScoreAnswer):
        return EvaluationResult(
            name=name,
            score=answer.score,
            min_score=0.0,
            max_score=float(len(question.criteria) - 1),
            explanation=f"{model} · confidence={answer.confidence:.2f}",
        )
    raise TypeError(
        f"jev answered question '{name}' with a {type(answer).__name__}, which "
        f"does not match its type '{question.type}'"
    )


def build_unavailable_results(
    names: Iterable[str], reason: str
) -> list[EvaluationResult]:
    """Build one unavailable result per question."""
    return [
        EvaluationResult(name=name, value="unavailable", explanation=reason)
        for name in names
    ]
