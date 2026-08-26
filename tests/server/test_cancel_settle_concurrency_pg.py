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
"""Adversarial concurrency tests for request_jobs_cancel settling jobs inline."""

import asyncio
import uuid
from typing import Any

import httpx

from conftest import db_settings, lifespan_client

AGENT_SCOPE = {"claims": [{"kind": "agent"}]}
EVALUATOR_SCOPE = {"claims": [{"kind": "evaluator"}]}
RUNTIME = {"platform": "bare"}


def assert_no_server_error(responses: list[httpx.Response]) -> None:
    """Fail with the response bodies when any racer got a server error.

    Args:
        responses: Responses collected from a race.
    """
    failures = [
        (
            response.request.method,
            str(response.request.url),
            response.status_code,
            response.text,
        )
        for response in responses
        if response.status_code >= 500
    ]
    assert not failures, failures


async def _agent_version(client: httpx.AsyncClient) -> tuple[str, str]:
    """Create an agent and a runnable version.

    Returns:
        Agent id and agent version id.
    """
    agent = (
        await client.post(
            "/api/v1/agents", json={"name": f"assistant-{uuid.uuid4().hex[:8]}"}
        )
    ).json()
    version = (
        await client.post(
            f"/api/v1/agents/{agent['id']}/versions",
            json={"run_spec": {"command": "run.sh", "timeout_seconds": 60}},
        )
    ).json()
    return agent["id"], version["id"]


async def _baseline_session(
    client: httpx.AsyncClient, agent_id: str, version_id: str, status: str = "completed"
) -> str:
    """Create a recorded session to replay.

    Returns:
        Session id.
    """
    session = (
        await client.post(
            "/api/v1/sessions",
            json={
                "agent_id": agent_id,
                "agent_version_id": version_id,
                "origin": "recorded",
                "status": status,
                "inputs": {"q": "hi"},
                "outputs": None,
            },
        )
    ).json()
    return session["id"]


async def _register_evaluator(client: httpx.AsyncClient) -> str:
    """Register an evaluator with a runnable version.

    Returns:
        Evaluator name.
    """
    name = f"accuracy-{uuid.uuid4().hex[:8]}"
    blob = (
        await client.post(
            "/api/v1/blobs",
            files={"file": ("score.py", b"def score(): pass", "text/plain")},
        )
    ).json()
    evaluator = (
        await client.post("/api/v1/evaluators", json={"name": name, "metadata": {}})
    ).json()
    await client.post(
        f"/api/v1/evaluators/{evaluator['id']}/versions",
        json={
            "source": {"type": "script", "blob_id": blob["id"], "entrypoint": "score"}
        },
    )
    return name


async def _standalone_replay(
    client: httpx.AsyncClient, baseline_id: str, evaluator_name: str
) -> dict[str, Any]:
    """Create a standalone replay of a baseline session.

    Returns:
        Created replay.
    """
    response = await client.post(
        "/api/v1/replays",
        json={
            "baseline_session_id": baseline_id,
            "evaluators": [{"evaluator": evaluator_name}],
        },
    )
    assert response.status_code == 201, response.text
    return response.json()


async def _register_worker(
    client: httpx.AsyncClient, scope: dict[str, Any]
) -> dict[str, str]:
    """Register a worker able to claim tasks of the given scope.

    Returns:
        Authorization header for the worker.
    """
    registration = (
        await client.post(
            "/api/v1/workers",
            json={
                "name": f"worker-{uuid.uuid4().hex[:8]}",
                "scope": scope,
                "runtime": RUNTIME,
                "metadata": {},
            },
        )
    ).json()
    return {"Authorization": f"Bearer {registration['token']}"}


async def _claim_one(
    client: httpx.AsyncClient, worker_headers: dict[str, str]
) -> tuple[dict[str, Any], dict[str, str]] | None:
    """Claim one pending task matching the worker's scope.

    Returns:
        Claimed task and its task token header, or None when nothing is
        pending yet.
    """
    claimed = (
        await client.post(
            "/api/v1/tasks/claim", json={"max_tasks": 1}, headers=worker_headers
        )
    ).json()
    tasks = claimed["tasks"]
    if not tasks:
        return None
    entry = tasks[0]
    return entry["task"], {"Authorization": f"Bearer {entry['token']}"}


async def _claim_one_retrying(
    client: httpx.AsyncClient, worker_headers: dict[str, str], attempts: int = 50
) -> tuple[dict[str, Any], dict[str, str]]:
    """Claim one pending task, retrying briefly while it has not been created yet.

    Returns:
        Claimed task and its task token header.
    """
    for _ in range(attempts):
        claimed = await _claim_one(client, worker_headers)
        if claimed is not None:
            return claimed
        await asyncio.sleep(0.05)
    raise AssertionError("No task became claimable in time")


async def _experiment_with_evaluator(
    client: httpx.AsyncClient, agent_id: str, evaluator_name: str
) -> dict[str, Any]:
    """Create an experiment scoring with one evaluator.

    Returns:
        Created experiment.
    """
    response = await client.post(
        "/api/v1/experiments",
        json={
            "name": f"exp-{uuid.uuid4().hex[:8]}",
            "agent_id": agent_id,
            "evaluators": [{"evaluator": evaluator_name}],
        },
    )
    assert response.status_code == 201, response.text
    return response.json()


async def _cohort_version(
    client: httpx.AsyncClient, agent_id: str, session_ids: list[str]
) -> str:
    """Create a cohort version holding the given sessions.

    Returns:
        Cohort version id.
    """
    cohort = (
        await client.post(
            "/api/v1/cohorts",
            json={"name": f"cohort-{uuid.uuid4().hex[:8]}", "agent_id": agent_id},
        )
    ).json()
    cohort_version = (
        await client.post(
            f"/api/v1/cohorts/{cohort['id']}/versions",
            json={"add_session_ids": session_ids},
        )
    ).json()
    return cohort_version["id"]


async def _start_run(
    client: httpx.AsyncClient,
    experiment_id: str,
    cohort_version_id: str,
    agent_version_id: str,
    evaluate_baselines: bool = False,
) -> dict[str, Any]:
    """Start an experiment run.

    Returns:
        Created run.
    """
    response = await client.post(
        f"/api/v1/experiments/{experiment_id}/runs",
        json={
            "cohort_version_id": cohort_version_id,
            "agent_version_id": agent_version_id,
            "evaluate_baselines": evaluate_baselines,
        },
    )
    assert response.status_code == 201, response.text
    return response.json()


async def _run_job_ids(client: httpx.AsyncClient, run_id: str) -> list[str]:
    """List the job ids backing an experiment run's replays.

    Returns:
        Job ids of the run.
    """
    response = await client.get(f"/api/v1/experiment-runs/{run_id}/jobs")
    assert response.status_code == 200, response.text
    return [job["id"] for job in response.json()["items"]]


async def _poll_job_settled(
    client: httpx.AsyncClient, job_id: str, timeout: float = 20.0
) -> dict[str, Any]:
    """Poll a job until it settles, failing if it never does.

    Returns:
        Settled job.
    """
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        response = await client.get(f"/api/v1/jobs/{job_id}")
        assert response.status_code == 200, response.text
        job = response.json()
        if job["status"] in ("completed", "failed", "canceled"):
            return job
        await asyncio.sleep(0.1)
    raise AssertionError(f"Job {job_id} never settled: last status unknown")


def _fast_sweep_settings(**overrides: Any) -> Any:
    """Build settings with a fast sweeper, tuned for a wide connection pool.

    Returns:
        API settings for the test database.
    """
    values: dict[str, Any] = {
        "DB_POOL_SIZE": 15,
        "DB_MAX_OVERFLOW": 10,
        "TASK_SWEEP_INTERVAL_SECONDS": 1,
        "TASK_HEARTBEAT_TIMEOUT_SECONDS": 1,
        **overrides,
    }
    return db_settings(**values)


# --- 1. Many racers cancelling the same job, sweeper active ---------------


async def test_many_racers_cancel_same_job_with_sweeper() -> None:
    """20+ concurrent cancels of one pending-task job settle it exactly once."""
    settings = _fast_sweep_settings()
    async with lifespan_client(settings) as client:
        agent_id, version_id = await _agent_version(client)
        baseline_id = await _baseline_session(client, agent_id, version_id)
        evaluator_name = await _register_evaluator(client)
        replay = await _standalone_replay(client, baseline_id, evaluator_name)
        job_id = replay["job_id"]

        # Let the sweeper tick at least once before the racers pile on.
        await asyncio.sleep(1.2)

        responses = await asyncio.gather(
            *(client.post(f"/api/v1/jobs/{job_id}/cancel") for _ in range(24))
        )
        assert_no_server_error(list(responses))
        statuses = {response.status_code for response in responses}
        assert statuses <= {200, 404, 409}, statuses

        job = await _poll_job_settled(client, job_id)
        assert job["status"] == "canceled", job
        assert job["ended_at"] is not None, job
        ended_at = job["ended_at"]

        # Idempotency: further racing cancels and reads must not perturb the
        # already-settled job, and must not 5xx.
        await asyncio.sleep(1.2)
        more_responses = await asyncio.gather(
            *(client.post(f"/api/v1/jobs/{job_id}/cancel") for _ in range(10))
        )
        assert_no_server_error(list(more_responses))
        final = (await client.get(f"/api/v1/jobs/{job_id}")).json()
        assert final["status"] == "canceled"
        assert final["ended_at"] == ended_at, (
            "job settled a second time",
            ended_at,
            final["ended_at"],
        )


# --- 2. Many jobs, many racers each, all at once ---------------------------


async def test_many_jobs_concurrent_cancel_racers() -> None:
    """20+ jobs, each raced by several cancel calls at once, all settle cleanly."""
    settings = _fast_sweep_settings()
    async with lifespan_client(settings) as client:
        agent_id, version_id = await _agent_version(client)
        evaluator_name = await _register_evaluator(client)
        job_ids: list[str] = []
        for _ in range(24):
            baseline_id = await _baseline_session(client, agent_id, version_id)
            replay = await _standalone_replay(client, baseline_id, evaluator_name)
            job_ids.append(replay["job_id"])

        requests = [
            client.post(f"/api/v1/jobs/{job_id}/cancel")
            for job_id in job_ids
            for _ in range(3)
        ]
        responses = await asyncio.gather(*requests)
        assert_no_server_error(list(responses))

        jobs = await asyncio.gather(
            *(_poll_job_settled(client, job_id) for job_id in job_ids)
        )
        for job in jobs:
            assert job["status"] == "canceled", job
            assert job["ended_at"] is not None, job


# --- 3. Experiment/run delete racing job cancel racing the sweeper --------


async def test_delete_experiment_races_run_delete_job_cancel_and_sweeper() -> None:
    """Deleting an experiment and its run race job cancels and the sweeper.

    Exactly one of the two deletes should win (the other observes 404), no
    request observes a 5xx, and every job that belonged to the run ends up
    settled canceled with no task left non-terminal.
    """
    settings = _fast_sweep_settings()
    async with lifespan_client(settings) as client:
        agent_id, version_id = await _agent_version(client)
        evaluator_name = await _register_evaluator(client)
        experiment = await _experiment_with_evaluator(client, agent_id, evaluator_name)
        session_ids = [
            await _baseline_session(client, agent_id, version_id) for _ in range(6)
        ]
        cohort_version_id = await _cohort_version(client, agent_id, session_ids)
        run = await _start_run(client, experiment["id"], cohort_version_id, version_id)
        job_ids = await _run_job_ids(client, run["id"])
        assert len(job_ids) == 6

        await asyncio.sleep(1.2)

        racers = [
            client.delete(f"/api/v1/experiment-runs/{run['id']}"),
            client.delete(f"/api/v1/experiments/{experiment['id']}"),
            client.post(f"/api/v1/experiment-runs/{run['id']}/cancel"),
        ]
        for job_id in job_ids:
            racers.append(client.post(f"/api/v1/jobs/{job_id}/cancel"))
            racers.append(client.post(f"/api/v1/jobs/{job_id}/cancel"))

        responses = await asyncio.gather(*racers, return_exceptions=False)
        assert_no_server_error(list(responses))
        statuses = {response.status_code for response in responses}
        assert statuses <= {200, 204, 404, 409}, [
            (str(r.request.url), r.status_code, r.text)
            for r in responses
            if r.status_code not in (200, 204, 404, 409)
        ]

        jobs = await asyncio.gather(
            *(_poll_job_settled(client, job_id) for job_id in job_ids)
        )
        for job_id, job in zip(job_ids, jobs, strict=True):
            assert job["status"] == "canceled", job
            assert job["ended_at"] is not None, job
            tasks = (await client.get(f"/api/v1/jobs/{job_id}/tasks")).json()["items"]
            live = [
                t
                for t in tasks
                if t["status"] not in ("completed", "failed", "canceled")
            ]
            assert not live, (job_id, live)


# --- 4/5. Mixed pending+running task cancel races a worker completion -----


async def test_mixed_pending_running_cancel_races_worker_completion() -> None:
    """Cancel a mixed pending and running job while the worker completes a task.

    The job must settle exactly once, to a status consistent with its
    tasks' final statuses, and must never be observed settled while one of
    its tasks is still live.
    """
    settings = _fast_sweep_settings()
    async with lifespan_client(settings) as client:
        agent_id, version_id = await _agent_version(client)
        baseline_id = await _baseline_session(client, agent_id, version_id)
        evaluator_name = await _register_evaluator(client)
        experiment = await _experiment_with_evaluator(client, agent_id, evaluator_name)
        cohort_version_id = await _cohort_version(client, agent_id, [baseline_id])

        for _ in range(10):
            run = await _start_run(
                client,
                experiment["id"],
                cohort_version_id,
                version_id,
                evaluate_baselines=True,
            )
            job_ids = await _run_job_ids(client, run["id"])
            assert len(job_ids) == 1
            job_id = job_ids[0]

            tasks = (await client.get(f"/api/v1/jobs/{job_id}/tasks")).json()["items"]
            assert len(tasks) == 2, tasks
            assert {t["kind"] for t in tasks} == {"agent", "evaluator"}

            agent_worker = await _register_worker(client, AGENT_SCOPE)
            agent_task, agent_headers = await _claim_one_retrying(client, agent_worker)
            assert agent_task["kind"] == "agent"
            assert agent_task["job_id"] == job_id
            response = await client.patch(
                f"/api/v1/tasks/{agent_task['id']}",
                json={"status": "running"},
                headers=agent_headers,
            )
            assert response.status_code == 200, response.text

            result_session = (
                await client.post(
                    "/api/v1/sessions",
                    json={"origin": "replay", "inputs": None, "outputs": None},
                    headers=agent_headers,
                )
            ).json()
            await client.patch(
                f"/api/v1/sessions/{result_session['id']}",
                json={"status": "completed", "outputs": {}},
            )

            async def complete_agent_task(
                task_id: str = agent_task["id"],
                headers: dict[str, str] = agent_headers,
            ) -> httpx.Response:
                return await client.patch(
                    f"/api/v1/tasks/{task_id}",
                    json={"status": "completed"},
                    headers=headers,
                )

            racers = [complete_agent_task()]
            for _ in range(16):
                racers.append(client.post(f"/api/v1/jobs/{job_id}/cancel"))

            responses = await asyncio.gather(*racers)
            assert_no_server_error(list(responses))
            for response in responses[1:]:
                assert response.status_code in (200, 404, 409), (
                    response.status_code,
                    response.text,
                )

            job = await _poll_job_settled(client, job_id)
            assert job["ended_at"] is not None, job

            final_tasks = (await client.get(f"/api/v1/jobs/{job_id}/tasks")).json()[
                "items"
            ]
            live = [
                t
                for t in final_tasks
                if t["status"] not in ("completed", "failed", "canceled")
            ]
            assert not live, (
                "job settled with a live task remaining",
                job,
                final_tasks,
            )

            statuses = {t["status"] for t in final_tasks}
            if statuses == {"completed"}:
                assert job["status"] == "completed", (job, final_tasks)
            elif "canceled" in statuses and "failed" not in statuses:
                assert job["status"] == "canceled", (job, final_tasks)

            ended_at = job["ended_at"]
            await asyncio.sleep(0.3)
            recheck = (await client.get(f"/api/v1/jobs/{job_id}")).json()
            assert recheck["ended_at"] == ended_at, (
                "job settled a second time",
                ended_at,
                recheck["ended_at"],
            )


# --- 6. Maximize sweeper/request overlap: spam across several ticks -------


async def test_cancel_racers_spam_overlaps_several_sweep_ticks() -> None:
    """Cancel racers hammer many jobs continuously across several sweep ticks.

    The background nowait propagation is very likely to collide with an
    in-flight HTTP cancel on the same job's task rows while both hold
    live locks.
    """
    settings = _fast_sweep_settings()
    async with lifespan_client(settings) as client:
        agent_id, version_id = await _agent_version(client)
        evaluator_name = await _register_evaluator(client)
        job_ids: list[str] = []
        for _ in range(20):
            baseline_id = await _baseline_session(client, agent_id, version_id)
            replay = await _standalone_replay(client, baseline_id, evaluator_name)
            job_ids.append(replay["job_id"])

        stop = asyncio.Event()
        all_responses: list[httpx.Response] = []

        async def hammer(job_id: str) -> None:
            while not stop.is_set():
                response = await client.post(f"/api/v1/jobs/{job_id}/cancel")
                all_responses.append(response)
                await asyncio.sleep(0.02)

        hammer_tasks = [asyncio.create_task(hammer(job_id)) for job_id in job_ids]
        # Span multiple sweep ticks (interval is 1s) so the background
        # nowait propagation and the racers are almost certainly in flight
        # at the same moment on the same job's task rows at some point.
        await asyncio.sleep(4.5)
        stop.set()
        await asyncio.gather(*hammer_tasks)

        assert_no_server_error(all_responses)
        statuses = {response.status_code for response in all_responses}
        assert statuses <= {200, 404, 409}, statuses

        jobs = await asyncio.gather(
            *(_poll_job_settled(client, job_id) for job_id in job_ids)
        )
        for job_id, job in zip(job_ids, jobs, strict=True):
            assert job["status"] == "canceled", job
            assert job["ended_at"] is not None, job
            tasks = (await client.get(f"/api/v1/jobs/{job_id}/tasks")).json()["items"]
            live = [
                t
                for t in tasks
                if t["status"] not in ("completed", "failed", "canceled")
            ]
            assert not live, (job_id, live)
