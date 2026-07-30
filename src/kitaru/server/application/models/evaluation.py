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
"""Evaluation filter and merge command models."""

import uuid

from kitaru.api_models.v1.evaluation import EvaluationDataType
from kitaru.base import FrozenModel
from kitaru.server.base import ListFilter


class EvaluationFilter(ListFilter):
    """Evaluation list filter."""

    session_id: uuid.UUID | None = None
    task_id: uuid.UUID | None = None
    agent_id: uuid.UUID | None = None
    cohort_id: uuid.UUID | None = None
    experiment_run_id: uuid.UUID | None = None
    evaluator_version_id: uuid.UUID | None = None
    name: str | None = None
    data_type: EvaluationDataType | None = None


class EvaluationMerge(FrozenModel):
    """Evaluation merge command."""

    name: str
    data_type: EvaluationDataType
    score: float | bool | None = None
    value: str | None = None
    explanation: str | None = None
    passed: bool | None = None
