"""Replay scoring retry contracts."""

from __future__ import annotations

from contextlib import nullcontext
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock, Mock, call

import pytest
from pydantic_ai import Agent
from pydantic_ai.models.test import TestModel

from kitaru.adapters.pydantic_ai import KitaruAgent
from kitaru.adapters.pydantic_ai import _agent as agent_module


@dataclass(frozen=True)
class _ReplayResult:
    record: Any
    submission: Any
    runs: Any


def _replay_with_scoring(
    monkeypatch: pytest.MonkeyPatch,
    *,
    result: _ReplayResult,
    evaluation_service: Any,
) -> _ReplayResult:
    agent = KitaruAgent(Agent(TestModel(), name="scored-agent", output_type=str))
    binding = SimpleNamespace(project_id="project-id")
    agent._registered_state = cast(Any, SimpleNamespace(binding=binding))
    flow_definition = MagicMock()
    monkeypatch.setattr(agent, "_registered_flow", lambda: flow_definition)
    monkeypatch.setattr(agent, "_preflight_registered_identity", Mock())
    monkeypatch.setattr(agent_module, "Client", lambda: object())
    monkeypatch.setattr(
        agent_module,
        "_temporary_active_project",
        lambda _project: nullcontext(),
    )
    monkeypatch.setattr(
        agent_module,
        "preplan_replay_attempt",
        lambda *_a, **_k: object(),
    )
    monkeypatch.setattr(
        agent_module,
        "freeze_replay_attempt",
        lambda *_a, **_k: SimpleNamespace(spec=SimpleNamespace(experiment_id="exp-1")),
    )
    monkeypatch.setattr(
        agent_module,
        "execute_replay_attempt",
        lambda *_a, **_k: result,
    )
    monkeypatch.setattr(agent_module, "scorer_snapshot", lambda _item: object())
    monkeypatch.setattr(agent_module, "ScoreEvaluationService", evaluation_service)

    return cast(
        _ReplayResult,
        agent.replay(
            "parent-run",
            at="checkpoint",
            on_error="fail",
            uncovered_policy="fail",
            idempotency_key="same-request",
            scorers=[object()],
        ),
    )


def test_scored_replay_retry_does_not_replace_existing_aggregate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    aggregate_reference = object()
    runs = Mock()
    result = _ReplayResult(
        record=SimpleNamespace(score_aggregate=aggregate_reference),
        submission=SimpleNamespace(results=[]),
        runs=runs,
    )
    evaluation_service = Mock(side_effect=AssertionError("retry rescored the attempt"))

    actual = _replay_with_scoring(
        monkeypatch,
        result=result,
        evaluation_service=evaluation_service,
    )

    assert actual is result
    evaluation_service.assert_not_called()
    runs.list.assert_not_called()


def test_scored_replay_retry_recovers_all_verified_children(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    children = [SimpleNamespace(id=f"child-{index}") for index in range(51)]
    runs = Mock()
    runs.list.side_effect = [
        SimpleNamespace(items=children[:50]),
        SimpleNamespace(items=children[50:]),
    ]
    result = _ReplayResult(
        record=SimpleNamespace(
            score_aggregate=None,
            counts=SimpleNamespace(verified=51),
            unverified_children=[],
        ),
        submission=SimpleNamespace(results=[]),
        runs=runs,
    )
    scored_record = SimpleNamespace(score_aggregate=object())
    service = Mock()
    service.evaluate_existing_attempt.return_value = SimpleNamespace(
        record=scored_record
    )
    evaluation_service = Mock(return_value=service)

    actual = _replay_with_scoring(
        monkeypatch,
        result=result,
        evaluation_service=evaluation_service,
    )

    assert actual.record is scored_record
    assert service.evaluate_existing_attempt.call_args.kwargs["executions"] == [
        f"child-{index}" for index in range(51)
    ]
    assert runs.list.call_args_list == [
        call(page=1, size=50),
        call(page=2, size=50),
    ]
