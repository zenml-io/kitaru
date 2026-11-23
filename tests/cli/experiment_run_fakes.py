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
"""Recording experiment-run client fake for CLI tests."""

import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest
from tests.cli.task_diagnostic_fakes import EmptyTaskDiagnostics

from kitaru.api_models.v1.experiment_run import (
    EvaluationAggregateResponse,
    ExperimentRunCreateRequest,
    ExperimentRunJobsListParams,
    ExperimentRunListParams,
    ExperimentRunProgress,
    ExperimentRunResponse,
    ExperimentRunStatus,
)
from kitaru.api_models.v1.replay import BaselineEvaluationMode
from kitaru.cli import invocation_context


def build_run(
    run_id: uuid.UUID,
    status: ExperimentRunStatus,
    *,
    experiment_id: uuid.UUID,
    cohort_version_id: uuid.UUID,
    agent_version_id: uuid.UUID,
    updated_offset: int = 0,
    completed: int = 0,
) -> ExperimentRunResponse:
    now = datetime(2026, 8, 3, tzinfo=UTC)
    return ExperimentRunResponse(
        id=run_id,
        owner_id=uuid.uuid5(uuid.NAMESPACE_URL, str(run_id)),
        created=now,
        updated=now + timedelta(seconds=updated_offset),
        experiment_id=experiment_id,
        number=3,
        status=status,
        cohort_version_id=cohort_version_id,
        agent_version_id=agent_version_id,
        evaluate_baselines=True,
        baseline_evaluation_mode=BaselineEvaluationMode.IF_MISSING,
        started_at=now,
        ended_at=now
        if status
        in {
            ExperimentRunStatus.COMPLETED,
            ExperimentRunStatus.FAILED,
            ExperimentRunStatus.CANCELED,
        }
        else None,
        error="remote failure" if status is ExperimentRunStatus.FAILED else None,
        progress=ExperimentRunProgress(
            pending=0 if completed else 1,
            evaluating=0,
            completed=completed,
            failed=1 if status is ExperimentRunStatus.FAILED else 0,
            canceled=1 if status is ExperimentRunStatus.CANCELED else 0,
            total=1,
        ),
    )


class StubRunsClient:
    """Protocol-shaped client recording all experiment-run SDK calls."""

    def __init__(self) -> None:
        self.experiment = SimpleNamespace(id=uuid.uuid4(), name="regression")
        self.cohort_version = SimpleNamespace(
            id=uuid.uuid4(), cohort_id=uuid.uuid4(), version=4
        )
        self.tasks = EmptyTaskDiagnostics()
        self.agent = SimpleNamespace(
            id=uuid.uuid4(), name="candidate", latest_version=2
        )
        self.agent_version = SimpleNamespace(
            id=uuid.uuid4(), agent_id=self.agent.id, version=2
        )
        self.run_id = uuid.uuid4()
        self.created_run = build_run(
            self.run_id,
            ExperimentRunStatus.RUNNING,
            experiment_id=self.experiment.id,
            cohort_version_id=self.cohort_version.id,
            agent_version_id=self.agent_version.id,
        )
        self.get_responses = [self.created_run]
        self.start_calls: list[tuple[uuid.UUID, ExperimentRunCreateRequest]] = []
        self.start_idempotency_keys: list[str | None] = []
        self.list_calls: list[ExperimentRunListParams] = []
        self.jobs_calls: list[tuple[uuid.UUID, ExperimentRunJobsListParams]] = []
        self.evaluation_aggregates: list[EvaluationAggregateResponse] = []
        self.aggregate_calls: list[uuid.UUID] = []
        self.get_calls = 0
        self.cancel_calls = 0
        self.delete_calls = 0
        self.experiment_lookups = 0
        self.experiments = self._Experiments(self)
        self.experiment_runs = self._Runs(self)
        self.cohort_versions = self._CohortVersions(self)
        self.agents = self._Agents(self)
        self.agent_versions = self._AgentVersions(self)

    class _Experiments:
        def __init__(self, owner: "StubRunsClient") -> None:
            self.owner = owner

        async def iter(self):
            self.owner.experiment_lookups += 1
            yield self.owner.experiment

        async def list(self, params: Any) -> Any:
            assert params.size == 2
            self.owner.experiment_lookups += 1
            return SimpleNamespace(items=[self.owner.experiment], next_cursor=None)

        async def get(self, experiment_id: uuid.UUID) -> Any:
            self.owner.experiment_lookups += 1
            assert experiment_id == self.owner.experiment.id
            return self.owner.experiment

        async def start_run(
            self,
            experiment_id: uuid.UUID,
            request: ExperimentRunCreateRequest,
            idempotency_key: str | None = None,
        ) -> ExperimentRunResponse:
            self.owner.start_calls.append((experiment_id, request))
            self.owner.start_idempotency_keys.append(idempotency_key)
            return self.owner.created_run

    class _Runs:
        def __init__(self, owner: "StubRunsClient") -> None:
            self.owner = owner

        async def get(self, run_id: uuid.UUID) -> ExperimentRunResponse:
            assert run_id == self.owner.run_id
            index = min(self.owner.get_calls, len(self.owner.get_responses) - 1)
            self.owner.get_calls += 1
            return self.owner.get_responses[index]

        async def get_evaluation_aggregates(
            self, run_id: uuid.UUID
        ) -> list[EvaluationAggregateResponse]:
            self.owner.aggregate_calls.append(run_id)
            return self.owner.evaluation_aggregates

        async def list(self, params: ExperimentRunListParams) -> Any:
            self.owner.list_calls.append(params)
            return SimpleNamespace(items=[self.owner.created_run], next_cursor="next")

        async def list_jobs(
            self, run_id: uuid.UUID, params: ExperimentRunJobsListParams
        ) -> Any:
            self.owner.jobs_calls.append((run_id, params))
            job = SimpleNamespace(model_dump=lambda **_: {"id": "job-1"})
            return SimpleNamespace(items=[job], next_cursor=None)

        async def cancel(self, run_id: uuid.UUID) -> ExperimentRunResponse:
            assert run_id == self.owner.run_id
            self.owner.cancel_calls += 1
            return self.owner.get_responses[-1]

        async def delete(self, run_id: uuid.UUID) -> None:
            assert run_id == self.owner.run_id
            self.owner.delete_calls += 1

    class _CohortVersions:
        def __init__(self, owner: "StubRunsClient") -> None:
            self.owner = owner

        async def get(self, version_id: uuid.UUID) -> Any:
            assert version_id == self.owner.cohort_version.id
            return self.owner.cohort_version

    class _Agents:
        def __init__(self, owner: "StubRunsClient") -> None:
            self.owner = owner

        async def iter(self):
            yield self.owner.agent

        async def list(self, params: Any) -> Any:
            assert params.size == 2
            return SimpleNamespace(items=[self.owner.agent], next_cursor=None)

        async def get(self, agent_id: uuid.UUID) -> Any:
            assert agent_id == self.owner.agent.id
            return self.owner.agent

        async def iter_versions(self, agent_id: uuid.UUID):
            assert agent_id == self.owner.agent.id
            yield self.owner.agent_version

    class _AgentVersions:
        def __init__(self, owner: "StubRunsClient") -> None:
            self.owner = owner

        async def get(self, version_id: uuid.UUID) -> Any:
            assert version_id == self.owner.agent_version.id
            return self.owner.agent_version


def install_runs_client(monkeypatch: pytest.MonkeyPatch) -> StubRunsClient:
    """Route public run commands through one recording client."""
    client = StubRunsClient()

    @asynccontextmanager
    async def fake_open_client():
        yield client

    monkeypatch.setattr(invocation_context, "open_asset_client", fake_open_client)
    return client
