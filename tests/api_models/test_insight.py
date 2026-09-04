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
"""Tests for insight API models."""

import pytest
from pydantic import ValidationError

from kitaru.api_models.v1.insight import (
    Bin,
    BinnedInsightData,
    CategoricalInsightData,
    CategoryValue,
    InsightInput,
    TextInsightData,
)


def test_duplicate_category_labels_rejected() -> None:
    """Reject categorical data with a repeated label."""
    with pytest.raises(ValidationError):
        CategoricalInsightData(
            values=[
                CategoryValue(label="a", value=1),
                CategoryValue(label="a", value=2),
            ]
        )


def test_unique_category_labels_accepted() -> None:
    """Accept categorical data whose labels are all distinct."""
    data = CategoricalInsightData(
        values=[
            CategoryValue(label="a", value=1),
            CategoryValue(label="b", value=2),
        ]
    )
    assert [value.label for value in data.values] == ["a", "b"]


def test_noncontiguous_bins_rejected() -> None:
    """Reject bins whose lower bound skips the previous bin's upper bound."""
    with pytest.raises(ValidationError):
        BinnedInsightData(
            bins=[
                Bin(lower_bound=0, upper_bound=1, count=1),
                Bin(lower_bound=2, upper_bound=3, count=1),
            ]
        )


def test_contiguous_bins_accepted() -> None:
    """Accept bins whose lower bound matches the previous bin's upper bound."""
    data = BinnedInsightData(
        bins=[
            Bin(lower_bound=0, upper_bound=1, count=1),
            Bin(lower_bound=1, upper_bound=2, count=1),
        ]
    )
    assert len(data.bins) == 2


def test_open_lower_bound_on_interior_bin_rejected() -> None:
    """Reject an open lower bound on a bin that is not first."""
    with pytest.raises(ValidationError):
        BinnedInsightData(
            bins=[
                Bin(lower_bound=None, upper_bound=1, count=1),
                Bin(lower_bound=None, upper_bound=2, count=1),
            ]
        )


def test_open_lower_bound_on_first_bin_accepted() -> None:
    """Accept an open lower bound on the first bin."""
    data = BinnedInsightData(
        bins=[
            Bin(lower_bound=None, upper_bound=1, count=1),
            Bin(lower_bound=1, upper_bound=2, count=1),
        ]
    )
    assert data.bins[0].lower_bound is None


def test_open_upper_bound_on_interior_bin_rejected() -> None:
    """Reject an open upper bound on a bin that is not last."""
    with pytest.raises(ValidationError):
        BinnedInsightData(
            bins=[
                Bin(lower_bound=0, upper_bound=None, count=1),
                Bin(lower_bound=0, upper_bound=2, count=1),
            ]
        )


def test_open_upper_bound_on_last_bin_accepted() -> None:
    """Accept an open upper bound on the last bin."""
    data = BinnedInsightData(
        bins=[
            Bin(lower_bound=0, upper_bound=1, count=1),
            Bin(lower_bound=1, upper_bound=None, count=1),
        ]
    )
    assert data.bins[-1].upper_bound is None


def test_lower_bound_not_less_than_upper_bound_rejected() -> None:
    """Reject a bin whose lower bound is not less than its upper bound."""
    with pytest.raises(ValidationError):
        BinnedInsightData(bins=[Bin(lower_bound=1, upper_bound=1, count=1)])


def test_lower_bound_less_than_upper_bound_accepted() -> None:
    """Accept a bin whose lower bound is less than its upper bound."""
    data = BinnedInsightData(bins=[Bin(lower_bound=0, upper_bound=1, count=1)])
    assert data.bins[0].lower_bound == 0


@pytest.mark.parametrize(
    "data",
    [
        {"type": "text", "content": "hello"},
        {
            "type": "categorical",
            "values": [{"label": "a", "value": 1}],
        },
        {
            "type": "binned",
            "bins": [{"lower_bound": 0, "upper_bound": 1, "count": 1}],
        },
    ],
)
def test_discriminator_round_trip(data: dict[str, object]) -> None:
    """Round-trip every insight data variant through model_validate on a dict."""
    insight = InsightInput.model_validate({"title": "t", "data": data})
    restored = InsightInput.model_validate(insight.model_dump())
    assert restored == insight


def test_text_discriminator_resolves_type() -> None:
    """Resolve the text variant from its discriminator."""
    insight = InsightInput.model_validate(
        {"title": "t", "data": {"type": "text", "content": "hello"}}
    )
    assert isinstance(insight.data, TextInsightData)
    assert insight.data.content == "hello"


def test_categorical_discriminator_resolves_type() -> None:
    """Resolve the categorical variant from its discriminator."""
    insight = InsightInput.model_validate(
        {
            "title": "t",
            "data": {"type": "categorical", "values": [{"label": "a", "value": 1}]},
        }
    )
    assert isinstance(insight.data, CategoricalInsightData)
    assert insight.data.values[0].label == "a"


def test_binned_discriminator_resolves_type() -> None:
    """Resolve the binned variant from its discriminator."""
    insight = InsightInput.model_validate(
        {
            "title": "t",
            "data": {
                "type": "binned",
                "bins": [{"lower_bound": 0, "upper_bound": 1, "count": 1}],
            },
        }
    )
    assert isinstance(insight.data, BinnedInsightData)
    assert insight.data.bins[0].count == 1
