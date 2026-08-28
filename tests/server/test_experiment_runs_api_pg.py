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
"""End-to-end experiment run tests against PostgreSQL."""

import json
from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import db_settings, lifespan_client


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app running its full lifespan."""
    async with lifespan_client(db_settings()) as client:
        yield client


async def _setup_run(client: httpx.AsyncClient) -> dict[str, str]:
    """Create an agent version, a two-session cohort version, and an experiment.

    Returns:
        Ids for the experiment, cohort version, and agent version.
    """
    agent = (await client.post("/api/v1/agents", json={"name": "assistant"})).json()
    version = (
        await client.post(
            f"/api/v1/agents/{agent['id']}/versions",
            json={"run_spec": {"command": "run.sh", "timeout_seconds": 60}},
        )
    ).json()
    blob = (
        await client.post(
            "/api/v1/blobs",
            files={"file": ("score.py", b"def score(): pass", "text/plain")},
        )
    ).json()
    evaluator = (
        await client.post(
            "/api/v1/evaluators", json={"name": "accuracy", "metadata": {}}
        )
    ).json()
    await client.post(
        f"/api/v1/evaluators/{evaluator['id']}/versions",
        json={
            "source": {"type": "script", "blob_id": blob["id"], "entrypoint": "score"}
        },
    )
    session_ids = []
    for _ in range(2):
        session = (
            await client.post(
                "/api/v1/sessions",
                json={
                    "agent_id": agent["id"],
                    "agent_version_id": version["id"],
                    "origin": "recorded",
                    "inputs": {"q": "hi"},
                    "outputs": None,
                },
            )
        ).json()
        session_ids.append(session["id"])
    cohort = (
        await client.post(
            "/api/v1/cohorts", json={"name": "cohort-1", "agent_id": agent["id"]}
        )
    ).json()
    cohort_version = (
        await client.post(
            f"/api/v1/cohorts/{cohort['id']}/versions",
            json={"add_session_ids": session_ids},
        )
    ).json()
    experiment = (
        await client.post(
            "/api/v1/experiments",
            json={
                "name": "exp1",
                "agent_id": agent["id"],
                "evaluators": [{"evaluator": "accuracy"}],
            },
        )
    ).json()
    return {
        "experiment_id": experiment["id"],
        "cohort_version_id": cohort_version["id"],
        "agent_version_id": version["id"],
    }


async def test_start_run_fans_out_one_replay_per_session(
    client: httpx.AsyncClient,
) -> None:
    """A run creates one replay per cohort session, reflected in progress."""
    setup = await _setup_run(client)
    response = await client.post(
        f"/api/v1/experiments/{setup['experiment_id']}/runs",
        json={
            "cohort_version_id": setup["cohort_version_id"],
            "agent_version_id": setup["agent_version_id"],
        },
    )
    assert response.status_code == 201
    run = response.json()
    assert run["number"] == 1
    assert run["progress"]["total"] == 2
    assert run["progress"]["pending"] == 2

    replays = (
        await client.get(
            "/api/v1/replays",
            params={
                "filter": json.dumps(
                    {"field": "experiment_run_id", "op": "eq", "value": run["id"]}
                )
            },
        )
    ).json()["items"]
    assert len(replays) == 2

    jobs = (await client.get(f"/api/v1/experiment-runs/{run['id']}/jobs")).json()[
        "items"
    ]
    assert len(jobs) == 2
    assert all(job["status"] == "pending" for job in jobs)


async def test_list_run_jobs_scoped_to_the_run(
    client: httpx.AsyncClient,
) -> None:
    """Listing a run's jobs excludes the jobs of other runs."""
    setup = await _setup_run(client)
    job_ids = []
    for _ in range(2):
        run = (
            await client.post(
                f"/api/v1/experiments/{setup['experiment_id']}/runs",
                json={
                    "cohort_version_id": setup["cohort_version_id"],
                    "agent_version_id": setup["agent_version_id"],
                },
            )
        ).json()
        replays = (
            await client.get(
                "/api/v1/replays",
                params={
                    "filter": json.dumps(
                        {"field": "experiment_run_id", "op": "eq", "value": run["id"]}
                    )
                },
            )
        ).json()["items"]
        jobs = (await client.get(f"/api/v1/experiment-runs/{run['id']}/jobs")).json()[
            "items"
        ]
        assert len(jobs) == 2
        assert {job["id"] for job in jobs} == {replay["job_id"] for replay in replays}
        job_ids.append({job["id"] for job in jobs})
    assert job_ids[0].isdisjoint(job_ids[1])


async def test_cancel_run_drains_pending_replicas_immediately(
    client: httpx.AsyncClient,
) -> None:
    """Canceling a run whose tasks are all pending settles it canceled."""
    setup = await _setup_run(client)
    run = (
        await client.post(
            f"/api/v1/experiments/{setup['experiment_id']}/runs",
            json={
                "cohort_version_id": setup["cohort_version_id"],
                "agent_version_id": setup["agent_version_id"],
            },
        )
    ).json()

    response = await client.post(f"/api/v1/experiment-runs/{run['id']}/cancel")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "canceled"
    assert body["progress"]["canceled"] == 2

    reloaded = (await client.get(f"/api/v1/experiment-runs/{run['id']}")).json()
    assert reloaded["status"] == "canceled"


async def test_delete_run_cascades_its_replays_and_cancels_its_jobs(
    client: httpx.AsyncClient,
) -> None:
    """Deleting a run removes its replay rows and cancels the jobs that ran them."""
    setup = await _setup_run(client)
    run = (
        await client.post(
            f"/api/v1/experiments/{setup['experiment_id']}/runs",
            json={
                "cohort_version_id": setup["cohort_version_id"],
                "agent_version_id": setup["agent_version_id"],
            },
        )
    ).json()
    jobs = (await client.get(f"/api/v1/experiment-runs/{run['id']}/jobs")).json()[
        "items"
    ]
    replays = (
        await client.get(
            "/api/v1/replays",
            params={
                "filter": json.dumps(
                    {"field": "experiment_run_id", "op": "eq", "value": run["id"]}
                )
            },
        )
    ).json()["items"]

    response = await client.delete(f"/api/v1/experiment-runs/{run['id']}")
    assert response.status_code == 204

    assert (await client.get(f"/api/v1/experiment-runs/{run['id']}")).status_code == 404
    for job in jobs:
        kept = await client.get(f"/api/v1/jobs/{job['id']}")
        assert kept.status_code == 200
        assert kept.json()["cancel_requested_at"] is not None
    for replay in replays:
        assert (await client.get(f"/api/v1/replays/{replay['id']}")).status_code == 404


async def test_experiment_config_update_conflicts_once_it_has_runs(
    client: httpx.AsyncClient,
) -> None:
    """Updating an experiment's replay config conflicts once it has a run."""
    setup = await _setup_run(client)
    await client.post(
        f"/api/v1/experiments/{setup['experiment_id']}/runs",
        json={
            "cohort_version_id": setup["cohort_version_id"],
            "agent_version_id": setup["agent_version_id"],
        },
    )
    response = await client.patch(
        f"/api/v1/experiments/{setup['experiment_id']}",
        json={"evaluators": [{"evaluator": "accuracy"}]},
    )
    assert response.status_code == 409


async def test_agent_version_update_allowed_once_tasks_reference_it(
    client: httpx.AsyncClient,
) -> None:
    """Updating an agent version's run spec stays legal once a task references it."""
    setup = await _setup_run(client)
    await client.post(
        f"/api/v1/experiments/{setup['experiment_id']}/runs",
        json={
            "cohort_version_id": setup["cohort_version_id"],
            "agent_version_id": setup["agent_version_id"],
        },
    )
    response = await client.patch(
        f"/api/v1/agent-versions/{setup['agent_version_id']}",
        json={"run_spec": {"command": "new.sh"}},
    )
    assert response.status_code == 200
    assert response.json()["run_spec"]["command"] == "new.sh"
