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
"""Round-trip tests for the experiment runs SDK resource."""

import uuid
from collections.abc import AsyncGenerator

import pytest
from test_experiments import SCORING_POLICY, create_cohort, create_experiment

from conftest import asgi_api_client, experiment_app
from kitaru.api_models.v1.experiment_runs import (
    ExperimentRunCreateRequest,
    ExperimentRunStatus,
)
from kitaru.api_models.v1.jobs import JobClaimRequest, JobStatus
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError, NotFoundError


@pytest.fixture
async def api_client() -> AsyncGenerator[KitaruAPIClient, None]:
    """Provide an API client routed to the app with fake-backed services."""
    async with asgi_api_client(experiment_app()) as client:
        yield client


async def test_get_and_list_round_trip(api_client: KitaruAPIClient) -> None:
    """Round-trip experiment runs through get and list."""
    cohort_id, _ = await create_cohort(api_client)
    experiment = await create_experiment(api_client, cohort_id)
    created = await api_client.experiments.create_run(
        experiment.id, ExperimentRunCreateRequest()
    )

    loaded = await api_client.experiment_runs.get(created.id)
    assert loaded == created
    assert loaded.progress.pending == 1

    page = await api_client.experiment_runs.list()
    assert page.total == 1
    assert page.items[0].id == created.id

    page = await api_client.experiment_runs.list(
        experiment_id=experiment.id, status=ExperimentRunStatus.PENDING
    )
    assert page.total == 1
    assert page.items[0].id == created.id

    page = await api_client.experiment_runs.list(status=ExperimentRunStatus.RUNNING)
    assert page.total == 0

    with pytest.raises(NotFoundError):
        await api_client.experiment_runs.list(experiment_id=uuid.uuid4())


async def test_get_not_found(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 404 as a NotFoundError."""
    with pytest.raises(NotFoundError):
        await api_client.experiment_runs.get(uuid.uuid4())


async def test_list_jobs(api_client: KitaruAPIClient) -> None:
    """List the jobs of a run with their inlined config."""
    cohort_id, version_id = await create_cohort(api_client)
    experiment = await create_experiment(api_client, cohort_id)
    created = await api_client.experiments.create_run(
        experiment.id, ExperimentRunCreateRequest()
    )
    page = await api_client.experiment_runs.list_jobs(created.id)
    assert page.total == 1
    job = page.items[0]
    assert job.experiment_run_id == created.id
    assert job.agent_version_id == version_id
    assert job.status is JobStatus.PENDING
    assert job.scoring_policy == SCORING_POLICY

    page = await api_client.experiment_runs.list_jobs(
        created.id, status=JobStatus.PENDING
    )
    assert page.total == 1
    assert page.items[0].id == job.id

    page = await api_client.experiment_runs.list_jobs(
        created.id, status=JobStatus.CLAIMED
    )
    assert page.total == 0


async def test_claim_and_cancel_round_trip(api_client: KitaruAPIClient) -> None:
    """Round-trip a run through claim and cancel."""
    cohort_id, _ = await create_cohort(api_client)
    experiment = await create_experiment(api_client, cohort_id)
    created = await api_client.experiments.create_run(
        experiment.id, ExperimentRunCreateRequest()
    )
    claimed = await api_client.experiment_runs.claim(
        created.id, JobClaimRequest(worker_id="worker-1", max_jobs=5)
    )
    assert len(claimed.jobs) == 1
    assert claimed.jobs[0].status is JobStatus.CLAIMED
    assert claimed.jobs[0].worker_id == "worker-1"

    canceled = await api_client.experiment_runs.cancel(created.id)
    assert canceled.status is ExperimentRunStatus.CANCELED
    assert canceled.summary is not None

    empty = await api_client.experiment_runs.claim(
        created.id, JobClaimRequest(worker_id="worker-1", max_jobs=5)
    )
    assert empty.jobs == []

    with pytest.raises(APIError) as exc_info:
        await api_client.experiment_runs.cancel(created.id)
    assert exc_info.value.status_code == 409


async def test_delete_round_trip(api_client: KitaruAPIClient) -> None:
    """Round-trip a run through cancel and delete."""
    cohort_id, _ = await create_cohort(api_client)
    experiment = await create_experiment(api_client, cohort_id)
    created = await api_client.experiments.create_run(
        experiment.id, ExperimentRunCreateRequest()
    )
    with pytest.raises(APIError) as exc_info:
        await api_client.experiment_runs.delete(created.id)
    assert exc_info.value.status_code == 409

    await api_client.experiment_runs.cancel(created.id)
    await api_client.experiment_runs.delete(created.id)
    with pytest.raises(NotFoundError):
        await api_client.experiment_runs.get(created.id)
