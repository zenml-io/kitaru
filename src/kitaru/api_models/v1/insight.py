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
"""Insight API models."""

import uuid
from typing import Annotated, Literal, Self

from pydantic import Field, model_validator

from kitaru.api_models.v1.base import (
    DiscriminatedRequestModel,
    FiniteFloat,
    JsonValue,
    OwnedResponseModel,
    RequestModel,
)
from kitaru.api_models.v1.filter import FilterableListParams

MAX_INSIGHT_BATCH_SIZE = 100


class TextInsightData(DiscriminatedRequestModel):
    """Text insight data."""

    type: Literal["text"] = Field(default="text")
    content: str = Field(description="Markdown content.")


class CategoryValue(RequestModel):
    """Category value."""

    label: str = Field(description="Category label.")
    value: FiniteFloat = Field(ge=0, description="Measured value.")


class CategoricalInsightData(DiscriminatedRequestModel):
    """Categorical insight data."""

    type: Literal["categorical"] = Field(default="categorical")
    unit: str | None = Field(default=None, description="Unit of the values.")
    values: list[CategoryValue] = Field(
        min_length=1, description="Values per category."
    )

    @model_validator(mode="after")
    def _unique_labels(self) -> Self:
        """Reject duplicate category labels.

        Raises:
            ValueError: A label repeats.

        Returns:
            The validated data.
        """
        labels = [value.label for value in self.values]
        if len(labels) != len(set(labels)):
            raise ValueError("category labels must be unique")
        return self


class Bin(RequestModel):
    """Bin."""

    lower_bound: FiniteFloat | None = Field(
        default=None,
        description="Inclusive lower bound, None on an open-ended first bin.",
    )
    upper_bound: FiniteFloat | None = Field(
        default=None,
        description="Exclusive upper bound, None on an open-ended last bin.",
    )
    count: int = Field(ge=0, description="Observations in the bin.")


class BinnedInsightData(DiscriminatedRequestModel):
    """Binned insight data."""

    type: Literal["binned"] = Field(default="binned")
    unit: str | None = Field(default=None, description="Unit of the values.")
    bins: list[Bin] = Field(min_length=1, description="Bins, in ascending order.")

    @model_validator(mode="after")
    def _check_bins(self) -> Self:
        """Require bins contiguous and sorted, with open bounds only at the ends.

        Raises:
            ValueError: A bound is open on an interior bin, a bin's lower
                bound does not equal the previous bin's upper bound, or a
                bin's lower bound is not less than its upper bound.

        Returns:
            The validated data.
        """
        last = len(self.bins) - 1
        previous_upper: float | None = None
        for index, bin_ in enumerate(self.bins):
            if bin_.lower_bound is None and index != 0:
                raise ValueError("only the first bin may have an open lower bound")
            if bin_.upper_bound is None and index != last:
                raise ValueError("only the last bin may have an open upper bound")
            if (
                bin_.lower_bound is not None
                and bin_.upper_bound is not None
                and bin_.lower_bound >= bin_.upper_bound
            ):
                raise ValueError("bin lower bound must be less than its upper bound")
            if index != 0 and bin_.lower_bound != previous_upper:
                raise ValueError(
                    "bin lower bound must equal the previous bin's upper bound"
                )
            previous_upper = bin_.upper_bound
        return self


InsightData = Annotated[
    TextInsightData | CategoricalInsightData | BinnedInsightData,
    Field(discriminator="type"),
]


class InsightInput(RequestModel):
    """Insight input."""

    name: str = Field(description="Insight name.")
    title: str = Field(description="Insight title.")
    description: str | None = Field(default=None, description="Insight description.")
    data: InsightData = Field(description="Insight data.")
    metadata: dict[str, JsonValue] = Field(
        default_factory=dict, description="Arbitrary metadata."
    )


class InsightBatchCreateRequest(RequestModel):
    """Insight batch create request."""

    agent_id: uuid.UUID = Field(description="Agent the insights belong to.")
    insights: list[InsightInput] = Field(
        min_length=1,
        max_length=MAX_INSIGHT_BATCH_SIZE,
        description="Insights to create, in input order.",
    )


class InsightUpdateRequest(RequestModel):
    """Insight update request."""

    title: str | None = Field(default=None, description="New insight title.")
    description: str | None = Field(
        default=None, description="New insight description."
    )


class InsightListParams(FilterableListParams):
    """Insight list params."""


class InsightResponse(OwnedResponseModel):
    """Insight response."""

    id: uuid.UUID = Field(description="Insight id.")
    agent_id: uuid.UUID = Field(description="Agent the insight belongs to.")
    name: str = Field(description="Insight name.")
    title: str = Field(description="Insight title.")
    description: str | None = Field(description="Insight description.")
    data: InsightData = Field(description="Insight data.")
    metadata: dict[str, JsonValue] = Field(description="Arbitrary metadata.")
