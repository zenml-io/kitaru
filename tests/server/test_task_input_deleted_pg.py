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
"""A task outliving the input row it names, and cancelling on claim."""

import asyncio
import uuid
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import httpx
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from conftest import db_settings, lifespan_client
from kitaru.api_models.v1.job import JobKind, JobStatus
from kitaru.server.adapters.db.repositories.job_repository import SQLJobRepository
from kitaru.server.adapters.db.repositories.task_repository import SQLTaskRepository
from kitaru.server.api.config import APISettings
from kitaru.server.database.service import DatabaseService
from kitaru.server.domain.job import Job
from kitaru.server.domain.task import EvaluationTask

RUNTIME = {"platform": "bare"}
RACERS = 10


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


def _bearer(token: str) -> dict[str, str]:
    """Build an Authorization header carrying a bearer token.

    Args:
        token: Bearer token.

    Returns:
        Header mapping.
    """
    return {"Authorization": f"Bearer {token}"}


async def _agent(client: httpx.AsyncClient, name: str) -> dict[str, Any]:
    response = await client.post("/api/v1/agents", json={"name": name})
    assert response.status_code == 201, response.text
    return response.json()


async def _agent_version(client: httpx.AsyncClient, agent_id: str) -> dict[str, Any]:
    """Create an agent version carrying a run spec, so it can back an agent task."""
    response = await client.post(
        f"/api/v1/agents/{agent_id}/versions",
        json={"run_spec": {"command": "run.sh", "timeout_seconds": 60}},
    )
    assert response.status_code == 201, response.text
    return response.json()


async def _session(client: httpx.AsyncClient, agent_id: str) -> dict[str, Any]:
    response = await client.post(
        "/api/v1/sessions",
        json={
            "agent_id": agent_id,
            "origin": "recorded",
            "inputs": {"q": "hi"},
            "outputs": None,
        },
    )
    assert response.status_code == 201, response.text
    return response.json()


async def _upload_blob(client: httpx.AsyncClient, name: str, content: bytes) -> str:
    """Upload a blob, deduped by content.

    Args:
        client: HTTP client.
        name: Uploaded file name.
        content: Blob content, content-addressed so identical bytes across
            calls resolve to the same blob id.

    Returns:
        Id of the stored blob.
    """
    response = await client.post(
        "/api/v1/blobs", files={"file": (name, content, "text/plain")}
    )
    assert response.status_code in (200, 201), response.text
    return response.json()["id"]


async def _importer_plugin(client: httpx.AsyncClient, name: str) -> dict[str, Any]:
    """Register an importer plugin with one script version."""
    script_blob_id = await _upload_blob(client, "importer.py", b"def run(): pass")
    importer = (await client.post("/api/v1/importers", json={"name": name})).json()
    version = (
        await client.post(
            f"/api/v1/importers/{importer['id']}/versions",
            json={
                "source": {
                    "type": "script",
                    "blob_id": script_blob_id,
                    "entrypoint": "run",
                }
            },
        )
    ).json()
    return {"importer": importer, "version": version}


async def _evaluator_plugin(client: httpx.AsyncClient, name: str) -> dict[str, Any]:
    """Register an evaluator plugin with one script version."""
    blob_id = await _upload_blob(client, "score.py", b"def score(): pass")
    evaluator = (
        await client.post("/api/v1/evaluators", json={"name": name, "metadata": {}})
    ).json()
    version = (
        await client.post(
            f"/api/v1/evaluators/{evaluator['id']}/versions",
            json={
                "source": {"type": "script", "blob_id": blob_id, "entrypoint": "score"}
            },
        )
    ).json()
    return {"evaluator": evaluator, "version": version}


async def _import_job(
    client: httpx.AsyncClient, agent_id: str, importer_name: str, payload: bytes = b"[]"
) -> tuple[str, str]:
    """Create an import job holding one import task.

    Args:
        client: HTTP client.
        agent_id: Agent the import targets.
        importer_name: Name of the registered importer.
        payload: Payload blob content. Distinct content across calls yields
            distinct blobs, since blobs are content-addressed.

    Returns:
        Job id and the payload blob id the task takes as input.
    """
    payload_blob_id = await _upload_blob(client, "payload.json", payload)
    response = await client.post(
        "/api/v1/imports",
        json={
            "importer": importer_name,
            "agent_id": agent_id,
            "payload_blob_id": payload_blob_id,
        },
    )
    assert response.status_code == 201, response.text
    return response.json()["id"], payload_blob_id


async def _agent_run_job(client: httpx.AsyncClient, agent_id: str) -> tuple[str, str]:
    """Create a session-run job holding one agent task.

    Returns:
        Job id and the agent version id the task takes as input.
    """
    version = await _agent_version(client, agent_id)
    response = await client.post(
        "/api/v1/session-runs",
        json={"agent_version_id": version["id"], "inputs": {"q": "hi"}},
    )
    assert response.status_code == 201, response.text
    return response.json()["id"], version["id"]


async def _job_task_id(client: httpx.AsyncClient, job_id: str) -> str:
    items = (await client.get(f"/api/v1/jobs/{job_id}/tasks")).json()["items"]
    assert len(items) == 1, items
    return items[0]["id"]


async def _register_worker(
    client: httpx.AsyncClient, scope: dict[str, Any]
) -> tuple[str, str]:
    response = await client.post(
        "/api/v1/workers",
        json={"name": "worker-1", "scope": scope, "runtime": RUNTIME, "metadata": {}},
    )
    assert response.status_code == 200, response.text
    body = response.json()
    return body["worker"]["id"], body["token"]


async def _claim_all(
    client: httpx.AsyncClient, worker_token: str, max_tasks: int = 20
) -> list[dict[str, Any]]:
    response = await client.post(
        "/api/v1/tasks/claim",
        json={"max_tasks": max_tasks},
        headers=_bearer(worker_token),
    )
    assert response.status_code == 200, response.text
    return response.json()["tasks"]


@asynccontextmanager
async def _raw_session(
    settings: APISettings,
) -> AsyncGenerator[AsyncSession, None]:
    """Open a session bound to the same database a lifespan_client migrated.

    Args:
        settings: Settings naming the database, matching the ones passed to
            lifespan_client.

    Yields:
        Session on the shared database.
    """
    engine = create_async_engine(DatabaseService.generate_database_uri(settings))
    try:
        session_factory = async_sessionmaker(
            bind=engine, class_=AsyncSession, expire_on_commit=False
        )
        async with session_factory() as session:
            yield session
    finally:
        await engine.dispose()


async def _raw_evaluator_job(
    settings: APISettings,
    owner_id: uuid.UUID,
    plugin_version_id: uuid.UUID,
    input_session_id: uuid.UUID,
) -> tuple[str, str]:
    """Insert a job and one evaluator task directly against the shared database.

    Every route that creates an evaluator task routes it through the replay
    pipeline, which also links the task's input session as a replay baseline
    and blocks deleting that session outright. Inserting the row directly
    isolates the input-session cascade from that unrelated restriction, since
    the foreign key itself does not require a replay to exist.

    Returns:
        Job id and task id.
    """
    async with _raw_session(settings) as session:
        job = await SQLJobRepository(session).create(
            Job(owner_id=owner_id, kind=JobKind.REPLAY, status=JobStatus.PENDING)
        )
        task = await SQLTaskRepository(session).create(
            EvaluationTask(
                job_id=job.id,
                plugin_version_id=plugin_version_id,
                input_session_id=input_session_id,
            )
        )
        await session.commit()
        return str(job.id), str(task.id)


async def _account_id(client: httpx.AsyncClient) -> uuid.UUID:
    response = await client.get("/api/v1/accounts/me")
    assert response.status_code == 200, response.text
    return uuid.UUID(response.json()["id"])


# ---------------------------------------------------------------------------
# 1. One task per input kind: delete the input, check the task and the job.
# ---------------------------------------------------------------------------


async def _claim_cancels_everything(
    client: httpx.AsyncClient, kind: str, task_id: str, job_id: str
) -> None:
    """Claim as a worker and expect the unresolvable task to cancel instead.

    Args:
        client: HTTP client.
        kind: Task kind the worker claims.
        task_id: Id of the task whose input is gone.
        job_id: Id of the job owning the task.
    """
    _worker_id, token = await _register_worker(client, {"claims": [{"kind": kind}]})
    assert await _claim_all(client, token) == []

    task = (await client.get(f"/api/v1/tasks/{task_id}")).json()
    assert task["status"] == "canceled", task
    job = (await client.get(f"/api/v1/jobs/{job_id}")).json()
    assert job["status"] == "canceled", job
    assert job["ended_at"] is not None, job
    assert (await client.delete(f"/api/v1/jobs/{job_id}")).status_code == 204


async def test_deleting_agent_version_leaves_its_task_for_the_claim() -> None:
    """Deleting an agent version keeps the task, which cancels when claimed."""
    settings = db_settings(DB_POOL_SIZE=20, DB_MAX_OVERFLOW=10)
    async with lifespan_client(settings) as client:
        agent = await _agent(client, "assistant")
        job_id, version_id = await _agent_run_job(client, agent["id"])
        task_id = await _job_task_id(client, job_id)

        response = await client.delete(f"/api/v1/agent-versions/{version_id}")
        assert response.status_code == 204, response.text

        assert (await client.get(f"/api/v1/tasks/{task_id}")).status_code == 200
        assert await _job_task_id(client, job_id) == task_id
        await _claim_cancels_everything(client, "agent", task_id, job_id)


async def test_deleting_payload_blob_leaves_its_task_for_the_claim() -> None:
    """Deleting a payload blob keeps the task, which cancels when claimed."""
    settings = db_settings(DB_POOL_SIZE=20, DB_MAX_OVERFLOW=10)
    async with lifespan_client(settings) as client:
        agent = await _agent(client, "assistant")
        await _importer_plugin(client, "importer-1")
        job_id, blob_id = await _import_job(client, agent["id"], "importer-1")
        task_id = await _job_task_id(client, job_id)

        response = await client.delete(f"/api/v1/blobs/{blob_id}")
        assert response.status_code == 204, response.text

        assert (await client.get(f"/api/v1/tasks/{task_id}")).status_code == 200
        await _claim_cancels_everything(client, "importer", task_id, job_id)


async def test_deleting_plugin_leaves_its_task_for_the_claim() -> None:
    """Deleting a plugin keeps the task, which cancels when claimed."""
    settings = db_settings(DB_POOL_SIZE=20, DB_MAX_OVERFLOW=10)
    async with lifespan_client(settings) as client:
        agent = await _agent(client, "assistant")
        plugin = await _importer_plugin(client, "importer-1")
        job_id, _blob_id = await _import_job(client, agent["id"], "importer-1")
        task_id = await _job_task_id(client, job_id)

        response = await client.delete(f"/api/v1/importers/{plugin['importer']['id']}")
        assert response.status_code == 204, response.text

        assert (await client.get(f"/api/v1/tasks/{task_id}")).status_code == 200
        await _claim_cancels_everything(client, "importer", task_id, job_id)


async def test_deleting_input_session_leaves_its_evaluator_task() -> None:
    """Deleting a scored session keeps the evaluator task that names it."""
    settings = db_settings(DB_POOL_SIZE=20, DB_MAX_OVERFLOW=10)
    async with lifespan_client(settings) as client:
        owner_id = await _account_id(client)
        agent = await _agent(client, "assistant")
        session = await _session(client, agent["id"])
        plugin = await _evaluator_plugin(client, "accuracy")

        job_id, task_id = await _raw_evaluator_job(
            settings,
            owner_id,
            uuid.UUID(plugin["version"]["id"]),
            uuid.UUID(session["id"]),
        )
        assert (await client.get(f"/api/v1/tasks/{task_id}")).status_code == 200

        response = await client.delete(f"/api/v1/sessions/{session['id']}")
        assert response.status_code == 204, response.text

        # The evaluator spec does not resolve the input session, so this task
        # is still handed to a worker and fails there rather than cancelling
        # on the claim. It reaches a terminal status either way, which is what
        # keeps its job settleable.
        assert (await client.get(f"/api/v1/tasks/{task_id}")).status_code == 200
        assert await _job_task_id(client, job_id) == task_id
        job = (await client.get(f"/api/v1/jobs/{job_id}")).json()
        assert job["status"] == "pending", job


# ---------------------------------------------------------------------------
# 2. The worker hazard: input deleted while a claimed task is running.
# ---------------------------------------------------------------------------


async def test_worker_calls_after_input_deleted_mid_flight_never_5xx() -> None:
    """A worker keeps reporting on a running task whose input was deleted."""
    settings = db_settings(DB_POOL_SIZE=20, DB_MAX_OVERFLOW=10)
    async with lifespan_client(settings) as client:
        agent = await _agent(client, "assistant")
        await _importer_plugin(client, "importer-1")
        _job_id, blob_id = await _import_job(client, agent["id"], "importer-1")

        worker_id, worker_token = await _register_worker(
            client, {"claims": [{"kind": "importer"}]}
        )
        entries = await _claim_all(client, worker_token)
        assert len(entries) == 1, entries
        task_id = entries[0]["task"]["id"]
        task_token = entries[0]["token"]

        running_response = await client.patch(
            f"/api/v1/tasks/{task_id}",
            json={"status": "running"},
            headers=_bearer(task_token),
        )
        assert running_response.status_code == 200, running_response.text
        assert running_response.json()["status"] == "running"

        # Delete the task's input while the worker believes it still owns a
        # running task.
        delete_response = await client.delete(f"/api/v1/blobs/{blob_id}")
        assert delete_response.status_code == 204, delete_response.text
        # The worker keeps the task it is running. Only the input is gone.
        assert (await client.get(f"/api/v1/tasks/{task_id}")).status_code == 200

        calls: dict[str, httpx.Response] = {}
        calls["heartbeat"] = await client.post(
            f"/api/v1/workers/{worker_id}/heartbeat",
            json={"task_ids": [task_id]},
            headers=_bearer(worker_token),
        )
        for status in ("running", "completed", "failed", "timed_out", "canceled"):
            calls[f"patch_{status}"] = await client.patch(
                f"/api/v1/tasks/{task_id}",
                json={"status": status, "result": {"ok": True}},
                headers=_bearer(task_token),
            )
        calls["get_task"] = await client.get(
            f"/api/v1/tasks/{task_id}", headers=_bearer(task_token)
        )
        calls["get_spec"] = await client.get(
            f"/api/v1/tasks/{task_id}/spec", headers=_bearer(task_token)
        )

        assert_no_server_error(list(calls.values()))
        for label, response in calls.items():
            assert response.status_code < 500, (
                label,
                response.status_code,
                response.text,
            )

        # Recorded, exact status per call. The task outlives its input, so the
        # worker still owns it and reports on it as usual. Only the spec, which
        # has to resolve the deleted blob, cannot be served.
        assert calls["heartbeat"].status_code == 200, calls["heartbeat"].text
        assert calls["heartbeat"].json()["cancel_task_ids"] == []
        # The task was already moved to running above, so that repeat is the
        # usual illegal transition rather than anything to do with the input.
        assert calls["patch_running"].status_code == 409, calls["patch_running"].text
        assert calls["patch_completed"].status_code == 200, calls[
            "patch_completed"
        ].text
        assert calls["get_task"].status_code == 200
        assert calls["get_spec"].status_code == 404


# ---------------------------------------------------------------------------
# 3. Highest-value hypothesis: a job left permanently unsettleable.
# ---------------------------------------------------------------------------


async def test_job_whose_task_input_vanished_settles_on_the_claim() -> None:
    """A job whose only task lost its input settles and becomes deletable.

    The background sweeper runs on a fast interval, so the job gets several
    ticks before a worker claims. Settling is the claim's job, not the
    sweeper's, and the job must not settle before then.
    """
    settings = db_settings(
        DB_POOL_SIZE=20,
        DB_MAX_OVERFLOW=10,
        TASK_SWEEP_INTERVAL_SECONDS=1,
        TASK_HEARTBEAT_TIMEOUT_SECONDS=1,
        TASK_RETRY_LIMIT=1,
    )
    async with lifespan_client(settings) as client:
        agent = await _agent(client, "assistant")
        job_id, version_id = await _agent_run_job(client, agent["id"])
        task_id = await _job_task_id(client, job_id)
        assert (await client.get(f"/api/v1/jobs/{job_id}")).json()[
            "status"
        ] == "pending"

        response = await client.delete(f"/api/v1/agent-versions/{version_id}")
        assert response.status_code == 204, response.text

        # The task is still queued, so the job is still legitimately pending.
        await asyncio.sleep(2.5)
        waiting = (await client.get(f"/api/v1/jobs/{job_id}")).json()
        assert waiting["status"] == "pending", waiting
        assert (await client.delete(f"/api/v1/jobs/{job_id}")).status_code == 409

        await _claim_cancels_everything(client, "agent", task_id, job_id)


# ---------------------------------------------------------------------------
# 4. Massive parallelism.
# ---------------------------------------------------------------------------


async def test_parallel_deletes_race_worker_claims_and_transitions_without_5xx() -> (
    None
):
    """Race double-deletes of many task inputs against live worker traffic."""
    settings = db_settings(DB_POOL_SIZE=80, DB_MAX_OVERFLOW=40)
    async with lifespan_client(settings) as client:
        agent = await _agent(client, "assistant")
        await _importer_plugin(client, "importer-1")

        blob_ids: list[str] = []
        for index in range(RACERS):
            _job_id, blob_id = await _import_job(
                client, agent["id"], "importer-1", payload=f"[{index}]".encode()
            )
            blob_ids.append(blob_id)

        worker_id, worker_token = await _register_worker(
            client, {"claims": [{"kind": "importer"}]}
        )
        entries = await _claim_all(client, worker_token, max_tasks=RACERS * 2)
        assert len(entries) == RACERS, entries
        task_ids = [entry["task"]["id"] for entry in entries]
        task_tokens = [entry["token"] for entry in entries]

        calls = []
        for blob_id in blob_ids:
            calls.append(client.delete(f"/api/v1/blobs/{blob_id}"))
            calls.append(client.delete(f"/api/v1/blobs/{blob_id}"))
        calls.append(
            client.post(
                f"/api/v1/workers/{worker_id}/heartbeat",
                json={"task_ids": task_ids},
                headers=_bearer(worker_token),
            )
        )
        for task_id, task_token in zip(task_ids, task_tokens, strict=True):
            calls.append(
                client.patch(
                    f"/api/v1/tasks/{task_id}",
                    json={"status": "running"},
                    headers=_bearer(task_token),
                )
            )

        responses = list(await asyncio.gather(*calls))
        assert_no_server_error(responses)

        delete_responses = responses[: 2 * RACERS]
        for index in range(RACERS):
            pair = [
                delete_responses[2 * index].status_code,
                delete_responses[2 * index + 1].status_code,
            ]
            # Whether a genuinely concurrent double-delete leaves exactly one
            # winner is a separate, pre-existing question unrelated to task
            # cascading, so only the absence of a server error is asserted
            # here.
            assert set(pair) <= {204, 404}, pair

        heartbeat_response = responses[2 * RACERS]
        assert heartbeat_response.status_code == 200, heartbeat_response.text

        patch_responses = responses[2 * RACERS + 1 :]
        for response in patch_responses:
            assert response.status_code in (200, 404), (
                response.status_code,
                response.text,
            )

        for task_id in task_ids:
            assert (await client.get(f"/api/v1/tasks/{task_id}")).status_code == 200


# ---------------------------------------------------------------------------
# 5. The task sweeper against a task deleted out from under it.
# ---------------------------------------------------------------------------


async def test_sweeper_tolerates_a_stale_tasks_input_deleted_concurrently() -> None:
    """The background sweep loop must not error, or die, over a vanished task."""
    settings = db_settings(
        DB_POOL_SIZE=20,
        DB_MAX_OVERFLOW=10,
        TASK_SWEEP_INTERVAL_SECONDS=1,
        TASK_HEARTBEAT_TIMEOUT_SECONDS=1,
        TASK_RETRY_LIMIT=1,
    )
    async with lifespan_client(settings) as client:
        agent = await _agent(client, "assistant")
        await _importer_plugin(client, "importer-1")
        _job_id, blob_id = await _import_job(client, agent["id"], "importer-1")

        _worker_id, worker_token = await _register_worker(
            client, {"claims": [{"kind": "importer"}]}
        )
        entries = await _claim_all(client, worker_token)
        task_id = entries[0]["task"]["id"]

        # Never heartbeat again, so the task goes stale on the sweeper's own
        # clock. Delete its input right as the heartbeat timeout elapses, to
        # race the sweep's unlocked read of the row against the cascade.
        await asyncio.sleep(1.2)
        delete_response = await client.delete(f"/api/v1/blobs/{blob_id}")
        assert delete_response.status_code == 204, delete_response.text

        # Give the sweeper several more ticks to run into the vanished row.
        await asyncio.sleep(2.5)

        assert (await client.get(f"/api/v1/tasks/{task_id}")).status_code == 200
        # The sweep loop, and the server hosting it, must still be alive.
        health = await client.get("/api/v1/accounts/me")
        assert health.status_code == 200, health.text
