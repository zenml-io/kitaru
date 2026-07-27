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
"""Tests for scoring."""

import uuid
from datetime import UTC, datetime
from typing import Any

import pytest

from kitaru.api_models.v1.sessions import (
    SessionOrigin,
    SessionResponse,
    SessionStatus,
)
from kitaru.scoring import (
    ScoringError,
    SessionView,
    call_scorer,
    load_scorer,
)


def constant_scorer(session: SessionView, value: float = 1.0) -> float:
    """Return a configured constant score."""
    return value


def raising_scorer(session: SessionView) -> float:
    """Raise an error."""
    raise RuntimeError("boom")


def string_scorer(session: SessionView) -> Any:
    """Return a non-numeric score."""
    return "high"


def boolean_scorer(session: SessionView) -> Any:
    """Return a boolean score."""
    return True


NOT_CALLABLE = "not callable"


def make_view() -> SessionView:
    """Build a session view around a minimal completed session."""
    now = datetime.now(UTC)
    session = SessionResponse(
        id=uuid.uuid4(),
        owner_id=uuid.uuid4(),
        agent_id=uuid.uuid4(),
        agent_version_id=None,
        origin=SessionOrigin.RECORDED,
        status=SessionStatus.COMPLETED,
        name=None,
        inputs=None,
        outputs=None,
        expected=None,
        error=None,
        started_at=None,
        ended_at=None,
        external_id=None,
        metadata={},
        provider=None,
        framework=None,
        adapter_version=None,
        log_uri=None,
        scores={},
        cost=None,
        tokens=None,
        llm_call_count=0,
        tool_call_count=0,
        created=now,
        updated=now,
    )
    return SessionView(session=session, nodes=[])


def test_load_scorer() -> None:
    """Import the referenced function."""
    assert load_scorer("test_scoring:constant_scorer") is constant_scorer


@pytest.mark.parametrize("source", ["noseparator", ":attribute", "module:"])
def test_load_scorer_malformed_source(source: str) -> None:
    """Reject sources that are not 'module:attribute'."""
    with pytest.raises(ScoringError, match="expected 'module:attribute'"):
        load_scorer(source)


def test_load_scorer_missing_module() -> None:
    """Reject a module that does not import."""
    with pytest.raises(ScoringError, match="Failed to import scorer module"):
        load_scorer("kitaru_missing_module:scorer")


def test_load_scorer_missing_attribute() -> None:
    """Reject a missing attribute."""
    with pytest.raises(ScoringError, match="has no attribute"):
        load_scorer("test_scoring:missing_scorer")


def test_load_scorer_not_callable() -> None:
    """Reject a non-callable attribute."""
    with pytest.raises(ScoringError, match="is not callable"):
        load_scorer("test_scoring:NOT_CALLABLE")


def test_call_scorer_passes_params() -> None:
    """Call the scorer with the session view and configured params."""
    assert call_scorer("quality", constant_scorer, make_view(), {"value": 0.3}) == 0.3


@pytest.mark.parametrize("value", [-0.1, 1.5])
def test_call_scorer_out_of_range(value: float) -> None:
    """Reject scores outside 0..1."""
    with pytest.raises(ScoringError, match=r"expected a value in 0\.\.1"):
        call_scorer("quality", constant_scorer, make_view(), {"value": value})


def test_call_scorer_non_numeric() -> None:
    """Reject a non-numeric score."""
    with pytest.raises(ScoringError, match=r"expected a float in 0\.\.1"):
        call_scorer("quality", string_scorer, make_view(), {})


def test_call_scorer_boolean() -> None:
    """Reject a boolean score."""
    with pytest.raises(ScoringError, match=r"expected a float in 0\.\.1"):
        call_scorer("quality", boolean_scorer, make_view(), {})


def test_call_scorer_exception_propagates() -> None:
    """Wrap a raising scorer in a ScoringError naming the scorer."""
    with pytest.raises(ScoringError, match="'quality' raised RuntimeError: boom"):
        call_scorer("quality", raising_scorer, make_view(), {})
