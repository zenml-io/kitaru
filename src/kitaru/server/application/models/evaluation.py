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
from collections.abc import Mapping
from typing import ClassVar

from kitaru.api_models.v1.evaluation import EvaluationDataType
from kitaru.base import FrozenModel
from kitaru.server.base import ListFilter
from kitaru.server.filtering import EQUALITY_OPS, NULLABLE_OPS, STRING_OPS, FilterField


class EvaluationFilter(ListFilter):
    """Evaluation list filter."""

    filterable_fields: ClassVar[Mapping[str, FilterField]] = {
        "session_id": FilterField(value_type=uuid.UUID, ops=EQUALITY_OPS),
        "task_id": FilterField(value_type=uuid.UUID, ops=EQUALITY_OPS | NULLABLE_OPS),
        "evaluator_version_id": FilterField(
            value_type=uuid.UUID, ops=EQUALITY_OPS | NULLABLE_OPS
        ),
        "name": FilterField(value_type=str, ops=STRING_OPS),
        "data_type": FilterField(value_type=EvaluationDataType, ops=EQUALITY_OPS),
    }


class EvaluationMerge(FrozenModel):
    """Evaluation merge command."""

    name: str
    data_type: EvaluationDataType
    score: float | bool | None = None
    value: str | None = None
    explanation: str | None = None
    passed: bool | None = None
