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
"""Tests for task API models."""

import pytest
from pydantic import ValidationError

from kitaru.api_models.v1.task import LabelSelector, TaskKind, WorkerScope


def test_empty_kinds_rejected() -> None:
    """Reject an empty kinds list."""
    with pytest.raises(ValidationError):
        WorkerScope(kinds=[])


def test_nonempty_kinds_accepted() -> None:
    """Accept a non-empty kinds list."""
    scope = WorkerScope(kinds=[TaskKind.AGENT])
    assert scope.kinds == [TaskKind.AGENT]


def test_empty_selectors_rejected() -> None:
    """Reject an empty selectors list."""
    with pytest.raises(ValidationError):
        WorkerScope(selectors=[])


def test_duplicate_selector_keys_rejected() -> None:
    """Reject selectors that repeat the same key."""
    with pytest.raises(ValidationError):
        WorkerScope(
            selectors=[
                LabelSelector(key="team", values=["a"]),
                LabelSelector(key="team", values=["b"]),
            ]
        )


def test_unique_selector_keys_accepted() -> None:
    """Accept selectors with distinct keys."""
    scope = WorkerScope(
        selectors=[
            LabelSelector(key="team", values=["a"]),
            LabelSelector(key="region", values=["b"]),
        ]
    )
    assert scope.selectors is not None
    assert len(scope.selectors) == 2


def test_empty_selector_values_rejected() -> None:
    """Reject a selector with an empty values list."""
    with pytest.raises(ValidationError):
        LabelSelector(key="team", values=[])
