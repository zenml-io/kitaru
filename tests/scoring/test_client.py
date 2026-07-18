"""Client-facing scoring API tests."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast

import kitaru.scoring as scoring_pkg
from kitaru._client._models import Execution, ExecutionStatus, ScoreFilter
from kitaru.client import _ExecutionsAPI
from kitaru.scoring import (
    Score,
    ScoreObservation,
    ScoreObservationOutcome,
    ScoreObservationStatus,
    ScorerSnapshot,
)


def _score(_: object) -> Score:
    return Score(value=0.9)


SNAPSHOT = ScorerSnapshot.from_callable(_score, capability="pure")
MANIFEST_HASH = f"sha256:{'3' * 64}"


class _ClientRef:
    def __init__(self) -> None:
        self._project = "project-id"
        self.zen = SimpleNamespace(active_project=SimpleNamespace(id="project-id"))

    def _client(self) -> Any:
        return self.zen


def _execution(client: Any) -> Execution:
    return Execution(
        exec_id="run-1",
        flow_id=None,
        flow_name="flow",
        status=ExecutionStatus.COMPLETED,
        started_at=datetime.now(UTC),
        ended_at=datetime.now(UTC),
        stack_name=None,
        metadata={},
        status_reason=None,
        failure=None,
        pending_wait=None,
        frozen_execution_spec=None,
        original_exec_id=None,
        checkpoints=[],
        artifacts=[],
        _client=client,
        project_id="project-id",
        project_name="project",
    )


def _observation(execution_id: str, value: float) -> ScoreObservation:
    return ScoreObservation(
        observation_id=f"obs-{execution_id}",
        project_id="project-id",
        execution_id=execution_id,
        experiment_id="exp-score",
        scorer=SNAPSHOT,
        outcome=ScoreObservationOutcome(
            status=ScoreObservationStatus.SCORED, score=Score(value=value)
        ),
        completed_at="2026-07-18T10:00:00Z",
        evidence_manifest_sha256=MANIFEST_HASH,
    )


def test_collection_evaluate_uses_shared_service(monkeypatch: Any) -> None:
    calls: list[dict[str, Any]] = []

    class FakeService:
        def __init__(self, **kwargs: Any) -> None:
            calls.append({"init": kwargs})

        def evaluate(self, executions: Any, scorers: Any, **kwargs: Any) -> str:
            calls.append(
                {"executions": executions, "scorers": scorers, "kwargs": kwargs}
            )
            return "attempt"

    import kitaru.client as client_module

    monkeypatch.setattr(client_module, "ScoreEvaluationService", FakeService)
    api = _ExecutionsAPI(cast(Any, _ClientRef()))

    assert api.evaluate("run-1", _score, name="Check") == "attempt"
    assert calls[0]["init"]["project_id"] == "project-id"
    assert calls[1]["executions"] == ["run-1"]
    assert calls[1]["scorers"] == [_score]
    assert calls[1]["kwargs"]["name"] == "Check"


def test_execution_evaluate_delegates_to_collection_api() -> None:
    calls: list[tuple[Any, Any, dict[str, Any]]] = []

    class FakeExecutions:
        def evaluate(self, executions: Any, scorers: Any, **kwargs: Any) -> str:
            calls.append((executions, scorers, kwargs))
            return "attempt"

    client = SimpleNamespace(executions=FakeExecutions())
    execution = _execution(client)

    assert execution.evaluate(_score, idempotency_key="key") == "attempt"
    assert calls == [
        (
            ["run-1"],
            _score,
            {
                "name": None,
                "suite_key": None,
                "idempotency_key": "key",
                "comparative": None,
                "metadata": None,
                "grounded_policy": None,
                "grounded_capabilities": None,
            },
        )
    ]


def test_score_candidate_filter_uses_observation_repository(monkeypatch: Any) -> None:
    class FakeRepo:
        def __init__(self, **_: Any) -> None:
            pass

        def matching_execution_ids(
            self,
            query: Any,
            *,
            minimum: float | None,
            maximum: float | None,
            cap: int,
        ) -> set[str]:
            assert query.experiment_id == "exp-score"
            assert query.valid is True
            assert minimum == 0.5
            assert maximum is None
            assert cap == 2
            return {"run-2"}

    monkeypatch.setattr(scoring_pkg, "ScoreObservationRepository", FakeRepo)
    api = _ExecutionsAPI(cast(Any, _ClientRef()))

    assert api._score_candidate_ids(
        ScoreFilter(
            experiment_id="exp-score",
            valid=True,
            minimum=0.5,
            candidate_cap=2,
        )
    ) == {"run-2"}
