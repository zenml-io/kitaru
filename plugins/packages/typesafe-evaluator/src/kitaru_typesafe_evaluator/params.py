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
"""Params accepted by the judge evaluator."""

from typing import Annotated, Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from kitaru.api_models.v1.evaluation import EvaluationName

# jev rejects more than 255 choice options and more than 10 score levels.
MAX_CHOICE_OPTIONS = 255
MAX_SCORE_LEVELS = 10


class _Question(BaseModel):
    """Fields shared by every question type."""

    model_config = ConfigDict(extra="forbid")

    instructions: str = Field(min_length=1)

    def to_wire(self) -> dict[str, Any]:
        """Build the question object jev receives."""
        return self.model_dump(include={"type", "instructions", "criteria"})


class NoulQuestion(_Question):
    """Yes/no question answered with the probability of yes."""

    type: Literal["noul"]
    pass_when: Literal["yes", "no"] | None = None
    decisive_at: float = Field(default=0.8, gt=0.5, le=1.0)


class ChoiceQuestion(_Question):
    """Question answered with one label from a fixed set."""

    type: Literal["choice"]
    criteria: dict[str, str | None] = Field(min_length=2, max_length=MAX_CHOICE_OPTIONS)
    pass_when: list[str] | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def _check_pass_when_labels(self) -> Self:
        unknown = set(self.pass_when or []) - set(self.criteria)
        if unknown:
            raise ValueError(
                f"pass_when names labels missing from criteria: {sorted(unknown)}"
            )
        return self


class ScoreQuestion(_Question):
    """Question answered with a position on ordered levels."""

    type: Literal["score"]
    criteria: list[str] = Field(min_length=2, max_length=MAX_SCORE_LEVELS)


Question = Annotated[
    NoulQuestion | ChoiceQuestion | ScoreQuestion, Field(discriminator="type")
]


class JudgeParams(BaseModel):
    """Params block of one judge run."""

    model_config = ConfigDict(extra="forbid")

    state: Literal["outcome", "full"] = "outcome"
    include: list[str] | None = Field(default=None, min_length=1)
    model: str | None = None
    questions: dict[EvaluationName, Question] = Field(min_length=1)
