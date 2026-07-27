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
"""Round-trip tests for the session runs SDK resource."""

from collections.abc import AsyncGenerator

import pytest
from pydantic import ValidationError
from test_jobs import create_session

from conftest import asgi_api_client, experiment_app
from kitaru.api_models.v1.jobs import JobClaimRequest, JobKind, JobStatus
from kitaru.api_models.v1.session_runs import SessionRunCreateRequest
from kitaru.api_models.v1.workers import WorkerCreateRequest
from kitaru.client.api_client import KitaruAPIClient


@pytest.fixture
async def api_client() -> AsyncGenerator[KitaruAPIClient, None]:
    """Provide an API client routed to the app with fake-backed services."""
    async with asgi_api_client(experiment_app()) as client:
        yield client


async def test_create_and_claim_round_trip(api_client: KitaruAPIClient) -> None:
    """Round-trip a session run through create and the global claim."""
    _, version_id = await create_session(api_client)
    created = await api_client.session_runs.create(
        SessionRunCreateRequest(
            agent_version_id=version_id, inputs={"prompt": "hi"}, name="smoke"
        )
    )
    assert created.kind is JobKind.SESSION_RUN
    assert created.agent_version_id == version_id
    assert created.inputs == {"prompt": "hi"}
    assert created.name == "smoke"
    assert created.status is JobStatus.PENDING

    loaded = await api_client.jobs.get(created.id)
    assert loaded == created

    page = await api_client.jobs.list(kind=JobKind.SESSION_RUN)
    assert page.total == 1
    assert page.items[0].id == created.id

    worker = await api_client.workers.create(WorkerCreateRequest(name="worker-1"))
    claim = await api_client.jobs.claim(
        JobClaimRequest(worker_id=worker.id, max_jobs=5)
    )
    assert [claimed.job.id for claimed in claim.jobs] == [created.id]
    assert claim.jobs[0].job.worker_id == worker.id
    assert claim.jobs[0].spec.inputs == {"prompt": "hi"}
    assert claim.jobs[0].spec.name == "smoke"


def test_create_request_requires_agent_reference() -> None:
    """Reject a request without an agent or agent version id."""
    with pytest.raises(ValidationError):
        SessionRunCreateRequest()
