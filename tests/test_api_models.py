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
"""Tests for shared API model types."""

import math
import uuid

import pytest
from pydantic import BaseModel, ValidationError

from kitaru.api_models.v1.base import FiniteFloat, JsonValue
from kitaru.api_models.v1.jobs import JobStatus, JobUpdateRequest
from kitaru.api_models.v1.session_nodes import (
    NodeStatus,
    NodeType,
    SessionNodeCreateRequest,
)
from kitaru.api_models.v1.sessions import (
    SessionCreateRequest,
    SessionOrigin,
    SessionScoresRequest,
)


class FloatModel(BaseModel):
    """Float model."""

    value: FiniteFloat


class JsonModel(BaseModel):
    """JSON model."""

    value: JsonValue = None


def test_finite_float_accepts_finite_values() -> None:
    """Validate finite floats."""
    assert FloatModel(value=1.5).value == 1.5
    assert FloatModel(value=-0.0).value == 0.0


@pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf])
def test_finite_float_rejects_non_finite_values(value: float) -> None:
    """Reject NaN and infinities."""
    with pytest.raises(ValidationError):
        FloatModel(value=value)


def test_json_value_accepts_nested_finite_values() -> None:
    """Validate nested JSON values with finite floats."""
    value = {"a": [1, 2.5, {"b": None}], "c": "text", "d": True}
    assert JsonModel(value=value).value == value


@pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf])
def test_json_value_rejects_top_level_non_finite(value: float) -> None:
    """Reject a non-finite top-level float."""
    with pytest.raises(ValidationError, match="non-finite"):
        JsonModel(value=value)


@pytest.mark.parametrize(
    "value",
    [
        {"a": math.nan},
        {"a": [1, math.inf]},
        [{"a": {"b": -math.inf}}],
    ],
)
def test_json_value_rejects_nested_non_finite(value: object) -> None:
    """Reject non-finite floats nested in dicts and lists."""
    with pytest.raises(ValidationError, match="non-finite"):
        JsonModel(value=value)


def test_session_create_request_rejects_non_finite_inputs() -> None:
    """Reject non-finite floats in session create inputs and metadata."""
    with pytest.raises(ValidationError):
        SessionCreateRequest(
            agent_id=uuid.uuid4(),
            origin=SessionOrigin.RECORDED,
            inputs={"question": math.nan},
        )
    with pytest.raises(ValidationError):
        SessionCreateRequest(
            agent_id=uuid.uuid4(),
            origin=SessionOrigin.RECORDED,
            metadata={"score": math.inf},
        )


def test_session_scores_request_rejects_non_finite_scores() -> None:
    """Reject non-finite score values."""
    with pytest.raises(ValidationError):
        SessionScoresRequest(scores={"accuracy": math.nan})


def test_session_node_create_request_rejects_non_finite_outputs() -> None:
    """Reject non-finite floats in node outputs."""
    with pytest.raises(ValidationError):
        SessionNodeCreateRequest(
            id=uuid.uuid4(),
            sequence=0,
            node_type=NodeType.SPAN,
            name="step",
            status=NodeStatus.COMPLETED,
            outputs={"result": [math.inf]},
        )


def test_job_update_request_rejects_non_finite_score() -> None:
    """Reject a non-finite job score."""
    with pytest.raises(ValidationError):
        JobUpdateRequest(
            status=JobStatus.COMPLETED,
            passed=True,
            score=math.nan,
            scores={"accuracy": 1.0},
        )
