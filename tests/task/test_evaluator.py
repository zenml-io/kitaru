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
"""Tests for evaluator task behavior."""

import asyncio
import uuid
from types import SimpleNamespace
from typing import Any

import pytest

from kitaru.api_models.v1.evaluation import EvaluationResult
from kitaru.api_models.v1.session import SessionResponse
from kitaru.api_models.v1.session_node import SessionNodeResponse
from kitaru.api_models.v1.task import (
    EvaluationTaskDetails,
    PackagePluginSpec,
)
from kitaru.task import evaluator as evaluator_module
from kitaru.task.evaluator import (
    EvaluationError,
    SessionView,
    call_evaluator,
)


def session_view() -> SessionView:
    """Build the smallest evaluator session view."""
    return SessionView(
        session=SessionResponse.model_construct(id=uuid.uuid4()),
        nodes=[],
    )


def test_call_evaluator_normalizes_single_result() -> None:
    """Normalize a single result to the list result contract."""
    result = EvaluationResult(name="quality", score=0.8)
    assert call_evaluator(
        "judge", lambda session, **params: result, session_view(), {}
    ) == [result]


def test_call_evaluator_passes_session_and_params() -> None:
    """Invoke the plugin with the session followed by keyword params."""
    view = session_view()
    received: dict[str, Any] = {}

    def evaluate(session: SessionView, *, threshold: float):
        received.update(session=session, threshold=threshold)
        return [
            EvaluationResult(name="first", score=True),
            EvaluationResult(name="second", value="good"),
        ]

    results = call_evaluator("judge", evaluate, view, {"threshold": 0.5})
    assert received == {"session": view, "threshold": 0.5}
    assert [result.name for result in results] == ["first", "second"]


@pytest.mark.parametrize(
    ("evaluate", "message"),
    [
        (lambda session: [], "returned no results"),
        (
            lambda session: [
                EvaluationResult(name="same", score=1.0),
                EvaluationResult(name="same", value="yes"),
            ],
            "duplicate result name",
        ),
        (lambda session: {"name": "wrong"}, "not an EvaluationResult"),
    ],
)
def test_call_evaluator_rejects_invalid_results(evaluate, message: str) -> None:
    """Reject empty, duplicate, and incorrectly typed results."""
    with pytest.raises(EvaluationError, match=message):
        call_evaluator("judge", evaluate, session_view(), {})


def test_call_evaluator_wraps_plugin_exception() -> None:
    """Name the evaluator when its callable raises."""

    def evaluate(session: SessionView) -> EvaluationResult:
        raise ValueError("model unavailable")

    with pytest.raises(
        EvaluationError, match="Evaluator 'judge' failed: model unavailable"
    ):
        call_evaluator("judge", evaluate, session_view(), {})


async def test_run_fetches_session_and_nodes_concurrently(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Start session and full-node reads before waiting for either."""
    session_id = uuid.uuid4()
    task_id = uuid.uuid4()
    details = EvaluationTaskDetails(
        kind="evaluator",
        evaluator_name="judge",
        params={"threshold": 0.5},
        plugin=PackagePluginSpec(
            type="package",
            entrypoint="package:evaluate",
            requirement="package==1.0",
        ),
        input_session_id=session_id,
    )
    session = SessionResponse.model_construct(id=session_id)
    node = SessionNodeResponse.model_construct(
        id=uuid.uuid4(), session_id=session_id, index=0
    )
    both_started = asyncio.Event()
    release = asyncio.Event()
    starts: set[str] = set()

    async def mark_started(name: str) -> None:
        starts.add(name)
        if len(starts) == 2:
            both_started.set()
        await release.wait()

    class Tasks:
        async def get_spec(self, requested_task_id: uuid.UUID):
            assert requested_task_id == task_id
            return SimpleNamespace(details=details)

    class Sessions:
        async def get(self, requested_id: uuid.UUID):
            assert requested_id == session_id
            await mark_started("session")
            return session

        async def iter_nodes(self, requested_id: uuid.UUID, *, include_payloads: bool):
            assert requested_id == session_id
            assert include_payloads is True
            await mark_started("nodes")
            yield node

    client: Any = SimpleNamespace(tasks=Tasks(), sessions=Sessions())
    captured: list[object] = []

    def evaluate(view: SessionView, **params: Any) -> EvaluationResult:
        assert view.session is session
        assert view.nodes == [node]
        assert params == {"threshold": 0.5}
        return EvaluationResult(name="quality", score=1.0)

    monkeypatch.setattr(
        evaluator_module, "load_source_ref", lambda ref, label: evaluate
    )
    monkeypatch.setattr(evaluator_module, "write_task_result", captured.append)

    task = asyncio.create_task(evaluator_module.run(client, str(task_id)))
    await asyncio.wait_for(both_started.wait(), timeout=1)
    assert starts == {"session", "nodes"}
    release.set()
    await task
    assert captured == [[EvaluationResult(name="quality", score=1.0)]]


async def test_run_requires_evaluator_details() -> None:
    """Reject a task spec for another kind."""

    task_id = uuid.uuid4()

    class Tasks:
        async def get_spec(self, requested_task_id: uuid.UUID):
            assert requested_task_id == task_id
            return SimpleNamespace(details=SimpleNamespace(kind="agent"))

    client: Any = SimpleNamespace(tasks=Tasks())
    with pytest.raises(EvaluationError, match="not an evaluator task"):
        await evaluator_module.run(client, str(task_id))
