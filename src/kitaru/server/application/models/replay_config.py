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
"""Evaluator config input model, shared by experiment and replay commands."""

import uuid
from typing import Any

from pydantic import Field, model_validator

from kitaru.base import FrozenModel


class EvaluatorConfigInput(FrozenModel):
    """Evaluator config awaiting resolution."""

    evaluator: str | None = None
    evaluator_version_id: uuid.UUID | None = None
    version: int | None = None
    params: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_identity(self) -> "EvaluatorConfigInput":
        if (self.evaluator is None) == (self.evaluator_version_id is None):
            raise ValueError(
                "exactly one of evaluator or evaluator_version_id is required"
            )
        if self.evaluator is None and self.version is not None:
            raise ValueError("version requires evaluator")
        return self
