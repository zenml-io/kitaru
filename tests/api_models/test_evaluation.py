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
"""Tests for evaluation API models."""

import pytest
from pydantic import ValidationError

from kitaru.api_models.v1.evaluation import EvaluationDataType, EvaluationResult


def test_positional_bool_routes_to_score() -> None:
    """Route a positional bool argument to score."""
    result = EvaluationResult(True, name="is_correct")
    assert result.score is True
    assert result.value is None


def test_positional_float_routes_to_score() -> None:
    """Route a positional float argument to score."""
    result = EvaluationResult(0.95, name="accuracy")
    assert result.score == 0.95
    assert result.value is None


def test_positional_str_routes_to_value() -> None:
    """Route a positional str argument to value."""
    result = EvaluationResult("high", name="category")
    assert result.value == "high"
    assert result.score is None


def test_data_type_bool() -> None:
    """Derive bool for a lone bool score."""
    result = EvaluationResult(name="is_correct", score=True)
    assert result.data_type == EvaluationDataType.BOOL


def test_data_type_float() -> None:
    """Derive float for a lone float score."""
    result = EvaluationResult(name="accuracy", score=0.95)
    assert result.data_type == EvaluationDataType.FLOAT


def test_data_type_str() -> None:
    """Derive str for a lone value."""
    result = EvaluationResult(name="category", value="high")
    assert result.data_type == EvaluationDataType.STR


def test_data_type_categorical() -> None:
    """Derive categorical when both score and value are set."""
    result = EvaluationResult(name="relevance", score=0.9, value="high")
    assert result.data_type == EvaluationDataType.CATEGORICAL


def test_bool_checked_before_float() -> None:
    """Derive bool rather than float for a bool score, even though bool is an int."""
    result = EvaluationResult(name="is_correct", score=False)
    assert result.data_type == EvaluationDataType.BOOL


def test_passed_defaults_to_none() -> None:
    """Leave the pass flag unset when it is not supplied."""
    result = EvaluationResult(name="accuracy", score=0.95)
    assert result.passed is None


def test_passed_is_independent_of_data_type() -> None:
    """Keep the pass flag out of the data type derivation."""
    result = EvaluationResult(name="category", value="high", passed=False)
    assert result.passed is False
    assert result.data_type == EvaluationDataType.STR
    assert result.score is None


def test_positional_score_leaves_passed_unset() -> None:
    """Route a positional argument to score without touching the pass flag."""
    result = EvaluationResult(0.95, name="accuracy", passed=True)
    assert result.score == 0.95
    assert result.passed is True


def test_passed_alone_rejected() -> None:
    """Reject a result carrying only a pass flag."""
    with pytest.raises(ValidationError):
        EvaluationResult(name="accuracy", passed=True)


def test_neither_score_nor_value_rejected() -> None:
    """Reject a result with neither score nor value set."""
    with pytest.raises(ValidationError):
        EvaluationResult(name="accuracy")


@pytest.mark.parametrize(
    "name",
    ["accuracy", "accuracy_3", "accuracy.relevance", "accuracy-3", "A9z"],
)
def test_valid_names_pass(name: str) -> None:
    """Accept names made of alphanumerics and the allowed separators."""
    result = EvaluationResult(name=name, score=1.0)
    assert result.name == name


@pytest.mark.parametrize(
    "name",
    ["", "-accuracy", "accuracy-", ".accuracy", "a b", "a/b", "a:b", "a@b", "ä"],
)
def test_invalid_names_rejected(name: str) -> None:
    """Reject empty names, boundary separators, and disallowed characters."""
    with pytest.raises(ValidationError):
        EvaluationResult(name=name, score=1.0)


def test_name_length_limit() -> None:
    """Reject names longer than the maximum length."""
    EvaluationResult(name="a" * 255, score=1.0)
    with pytest.raises(ValidationError):
        EvaluationResult(name="a" * 256, score=1.0)
