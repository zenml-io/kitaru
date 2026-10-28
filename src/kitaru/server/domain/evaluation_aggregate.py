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
"""Evaluation aggregate value objects and statistics."""

import uuid
from collections import Counter
from typing import Self

from kitaru.api_models.v1.evaluation import EvaluationDataType
from kitaru.server.domain.base import DomainModel
from kitaru.server.domain.evaluation import Evaluation


def _get_shared_value(values: list[float | None]) -> float | None:
    """Get the value shared by every row, if none are missing.

    Args:
        values: Optional values to compare.

    Returns:
        Shared value, or None when values differ or any are missing.
    """
    if not values or any(value is None for value in values):
        return None
    first = values[0]
    return first if all(value == first for value in values) else None


class EvaluationStats(DomainModel):
    """Statistics over evaluations sharing a name, evaluator, and data type."""

    count: int
    mean: float | None = None
    min: float | None = None
    max: float | None = None
    pass_rate: float | None = None
    value_counts: dict[str, int] | None = None
    min_score: float | None = None
    max_score: float | None = None
    target_score: float | None = None

    @classmethod
    def aggregate(
        cls, data_type: EvaluationDataType, evaluations: list[Evaluation]
    ) -> Self:
        """Calculate statistics for a group of evaluations.

        Args:
            data_type: Shared evaluation data type.
            evaluations: Evaluations to aggregate.

        Returns:
            Statistics over the evaluations.
        """
        scores = [
            float(evaluation.score)
            for evaluation in evaluations
            if evaluation.score is not None
        ]
        flags = [
            evaluation.passed
            for evaluation in evaluations
            if evaluation.passed is not None
        ]
        values = [
            evaluation.value
            for evaluation in evaluations
            if evaluation.value is not None
        ]
        scorable = data_type in (EvaluationDataType.FLOAT, EvaluationDataType.BOOL)
        return cls(
            count=len(evaluations),
            mean=sum(scores) / len(scores) if scorable and scores else None,
            min=min(scores) if scorable and scores else None,
            max=max(scores) if scorable and scores else None,
            pass_rate=sum(flags) / len(flags) if flags else None,
            value_counts=dict(Counter(values))
            if data_type is EvaluationDataType.CATEGORICAL
            else None,
            min_score=_get_shared_value(
                [evaluation.min_score for evaluation in evaluations]
            ),
            max_score=_get_shared_value(
                [evaluation.max_score for evaluation in evaluations]
            ),
            target_score=_get_shared_value(
                [evaluation.target_score for evaluation in evaluations]
            ),
        )


class ReplayEvaluations(DomainModel):
    """A replay's baseline and result evaluations for one aggregate group."""

    replay_id: uuid.UUID
    baseline: Evaluation | None = None
    result: Evaluation | None = None


class EvaluationAggregate(DomainModel):
    """Baseline and result statistics with recent replay evaluations."""

    name: str
    evaluator_version_id: uuid.UUID | None
    evaluator_name: str | None
    evaluator_version: int | None
    data_type: EvaluationDataType
    baseline: EvaluationStats
    result: EvaluationStats
    replays: list[ReplayEvaluations]
