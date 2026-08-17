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
"""Tests for annotation API models."""

import pytest
from pydantic import ValidationError

from kitaru.api_models.v1.annotation import AnnotationSelector, AnnotationSpan


def test_span_without_path_rejected() -> None:
    """Reject a span without a path."""
    with pytest.raises(ValidationError):
        AnnotationSelector(span=AnnotationSpan(start=0, end=4))


def test_span_with_path_accepted() -> None:
    """Accept a span alongside a path."""
    selector = AnnotationSelector(path="/message", span=AnnotationSpan(start=0, end=4))
    assert selector.span == AnnotationSpan(start=0, end=4)


def test_negative_span_offset_rejected() -> None:
    """Reject a span with a negative offset."""
    with pytest.raises(ValidationError):
        AnnotationSpan(start=-1, end=4)


def test_span_end_before_start_rejected() -> None:
    """Reject a span whose end precedes its start."""
    with pytest.raises(ValidationError):
        AnnotationSpan(start=5, end=2)


def test_empty_span_accepted() -> None:
    """Accept a zero-length span."""
    span = AnnotationSpan(start=3, end=3)
    assert span.start == span.end
