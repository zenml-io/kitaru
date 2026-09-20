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
"""Tests for mapping jev answers onto evaluation results."""

from typing import Literal

import pytest
from typesafe_sdk import ChoiceAnswer, NoulAnswer, ScoreAnswer

from kitaru.api_models.v1.evaluation import EvaluationDataType
from kitaru_typesafe_evaluator.params import ChoiceQuestion, NoulQuestion, ScoreQuestion
from kitaru_typesafe_evaluator.results import (
    build_result,
    build_unavailable_results,
    decide_noul_verdict,
)


@pytest.mark.parametrize(
    ("probability_yes", "pass_when", "decisive_at", "expected"),
    [
        (0.94, "yes", 0.8, True),
        (0.80, "yes", 0.8, True),
        (0.79, "yes", 0.8, None),
        (0.21, "yes", 0.8, None),
        (0.20, "yes", 0.8, False),
        (0.05, "yes", 0.8, False),
        (0.94, "no", 0.8, False),
        (0.05, "no", 0.8, True),
        (0.71, "no", 0.8, None),
        (0.71, "no", 0.7, False),
        (0.99, None, 0.8, None),
    ],
)
def test_noul_verdict(
    probability_yes: float,
    pass_when: Literal["yes", "no"] | None,
    decisive_at: float,
    expected: bool | None,
) -> None:
    assert decide_noul_verdict(probability_yes, pass_when, decisive_at) is expected


def test_noul_result_keeps_the_raw_probability() -> None:
    question = NoulQuestion(type="noul", instructions="x", pass_when="no")
    result = build_result(
        "invented_timeline", question, NoulAnswer(noul=0.94), "jev-1.13.0"
    )
    assert (result.name, result.score, result.passed) == (
        "invented_timeline",
        0.94,
        False,
    )
    assert (result.min_score, result.max_score) == (0.0, 1.0)
    assert result.data_type is EvaluationDataType.FLOAT
    assert (
        result.explanation
        == "jev-1.13.0 · p(yes)=0.94 · fail: p(no)=0.06 is at or below 0.20"
    )


def test_held_noul_explains_the_band() -> None:
    question = NoulQuestion(type="noul", instructions="x", pass_when="yes")
    result = build_result("q", question, NoulAnswer(noul=0.71), "jev-1.13.0")
    assert result.passed is None
    assert (
        result.explanation == "jev-1.13.0 · p(yes)=0.71 · held: between 0.20 and 0.80"
    )


def test_choice_result_is_categorical() -> None:
    question = ChoiceQuestion(
        type="choice",
        instructions="x",
        criteria={"fine": None, "invented_fact": None},
        pass_when=["fine"],
    )
    answer = ChoiceAnswer(
        choice="invented_fact",
        confidence=0.9,
        probabilities={"fine": 0.1, "invented_fact": 0.9},
    )
    result = build_result("failure_mode", question, answer, "jev-1.13.0")
    assert (result.value, result.score, result.passed) == ("invented_fact", 0.9, False)
    assert result.data_type is EvaluationDataType.CATEGORICAL


def test_unsure_choice_is_held() -> None:
    question = ChoiceQuestion(
        type="choice",
        instructions="x",
        criteria={"a": None, "b": None},
        pass_when=["a"],
    )
    answer = ChoiceAnswer(
        choice="a", confidence=0.49, probabilities={"a": 0.51, "b": 0.49}
    )
    assert build_result("q", question, answer, "m").passed is None


def test_choice_without_pass_when_has_no_verdict() -> None:
    question = ChoiceQuestion(
        type="choice", instructions="x", criteria={"a": None, "b": None}
    )
    answer = ChoiceAnswer(
        choice="a", confidence=1.0, probabilities={"a": 1.0, "b": 0.0}
    )
    assert build_result("q", question, answer, "m").passed is None


def test_score_result_carries_its_scale() -> None:
    question = ScoreQuestion(
        type="score", instructions="x", criteria=["curt", "neutral", "warm"]
    )
    answer = ScoreAnswer(
        score=0.43,
        confidence=0.36,
        legend={0: "curt", 1: "neutral", 2: "warm"},
        probabilities={0: 0.57, 1: 0.43, 2: 0.0},
    )
    result = build_result("tone", question, answer, "jev-1.13.0")
    assert (result.score, result.min_score, result.max_score, result.passed) == (
        0.43,
        0.0,
        2.0,
        None,
    )


def test_unavailable_results_name_every_question() -> None:
    results = build_unavailable_results(["a", "b"], "too large")
    assert [(r.name, r.value, r.explanation) for r in results] == [
        ("a", "unavailable", "too large"),
        ("b", "unavailable", "too large"),
    ]
