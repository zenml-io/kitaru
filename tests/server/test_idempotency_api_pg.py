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
"""End-to-end scoped idempotency contract against PostgreSQL."""

import asyncio
import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import (
    db_settings,
    drop_test_database,
    lifespan_client,
    postgres_available,
)
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.idempotency_repository import (
    SQLIdempotencyRepository,
)
from kitaru.server.api.app import create_app
from kitaru.server.database.service import DatabaseService
from kitaru.server.domain.idempotency import IdempotencyRecord


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide a real app whose lifespan migrates a fresh PostgreSQL database."""
    async with lifespan_client(db_settings(TASK_SWEEP_INTERVAL_SECONDS=0)) as client:
        yield client


async def _setup_protected_routes(client: httpx.AsyncClient) -> dict[str, str]:
    """Create shared resources required by all four protected mutations."""
    agent = (await client.post("/v1/agents", json={"name": "assistant"})).json()
    version = (
        await client.post(
            f"/v1/agents/{agent['id']}/versions",
            json={"run_spec": {"command": "run.sh", "timeout_seconds": 60}},
        )
    ).json()
    session = (
        await client.post(
            "/v1/sessions",
            json={
                "agent_id": agent["id"],
                "agent_version_id": version["id"],
                "origin": "recorded",
                "inputs": {"q": "hi"},
                "outputs": None,
                "expected": None,
            },
        )
    ).json()
    blob = (
        await client.post(
            "/v1/blobs",
            files={"file": ("score.py", b"def score(): pass", "text/plain")},
        )
    ).json()
    evaluator = (
        await client.post("/v1/evaluators", json={"name": "accuracy", "metadata": {}})
    ).json()
    await client.post(
        f"/v1/evaluators/{evaluator['id']}/versions",
        json={
            "source": {
                "type": "script",
                "blob_id": blob["id"],
                "entrypoint": "score",
            }
        },
    )
    cohort = (
        await client.post(
            "/v1/cohorts", json={"name": "cohort-1", "agent_id": agent["id"]}
        )
    ).json()
    cohort_version = (
        await client.post(
            f"/v1/cohorts/{cohort['id']}/versions",
            json={"add_session_ids": [session["id"]]},
        )
    ).json()
    experiment = (
        await client.post(
            "/v1/experiments",
            json={"name": "experiment-1", "evaluators": [{"evaluator": "accuracy"}]},
        )
    ).json()
    return {
        "session_id": session["id"],
        "agent_version_id": version["id"],
        "cohort_version_id": cohort_version["id"],
        "experiment_id": experiment["id"],
    }


async def _assert_stored_then_replayed(
    client: httpx.AsyncClient,
    path: str,
    body: dict[str, object],
    key: str,
) -> httpx.Response:
    """Submit one mutation twice and assert its authoritative raw replay."""
    first = await client.post(path, json=body, headers={"Idempotency-Key": key})
    second = await client.post(path, json=body, headers={"Idempotency-Key": key})
    assert first.status_code == 201
    assert second.status_code == 201
    assert first.content == second.content
    assert first.headers["Idempotency-Status"] == "stored"
    assert second.headers["Idempotency-Status"] == "replayed"
    assert first.headers["content-type"] == second.headers["content-type"]
    return first


async def test_all_four_protected_routes_replay_one_committed_result(
    client: httpx.AsyncClient,
) -> None:
    """Replay exact committed receipts for every route in the endpoint matrix."""
    setup = await _setup_protected_routes(client)
    await _assert_stored_then_replayed(
        client,
        "/v1/replays",
        {
            "baseline_session_id": setup["session_id"],
            "evaluators": [{"evaluator": "accuracy"}],
        },
        "shared-request",
    )
    await _assert_stored_then_replayed(
        client,
        "/v1/evaluations",
        {
            "input_session_ids": [setup["session_id"]],
            "evaluators": [{"evaluator": "accuracy"}],
        },
        "shared-request",
    )
    await _assert_stored_then_replayed(
        client,
        "/v1/session-runs",
        {
            "agent_version_id": setup["agent_version_id"],
            "inputs": {"q": "hi"},
        },
        "shared-request",
    )
    await _assert_stored_then_replayed(
        client,
        f"/v1/experiments/{setup['experiment_id']}/runs",
        {
            "cohort_version_id": setup["cohort_version_id"],
            "agent_version_id": setup["agent_version_id"],
        },
        "shared-request",
    )


async def test_changed_body_mismatches_and_failed_mutation_rolls_back_reservation(
    client: httpx.AsyncClient,
) -> None:
    """Separate permanent mismatch from a failed mutation's reusable key."""
    setup = await _setup_protected_routes(client)
    original = {
        "agent_version_id": setup["agent_version_id"],
        "inputs": {"q": "hi"},
    }
    first = await client.post(
        "/v1/session-runs",
        json=original,
        headers={"Idempotency-Key": "mismatch-request"},
    )
    assert first.status_code == 201
    mismatch = await client.post(
        "/v1/session-runs",
        json={**original, "inputs": {"q": "changed"}},
        headers={"Idempotency-Key": "mismatch-request"},
    )
    assert mismatch.status_code == 409
    assert mismatch.json() == {
        "detail": "The idempotency key was already used for a different request.",
        "code": "idempotency_mismatch",
        "retryable": False,
    }

    rollback_key = "failed-request"
    failed = await client.post(
        "/v1/session-runs",
        json={"agent_version_id": str(uuid.uuid4()), "inputs": {}},
        headers={"Idempotency-Key": rollback_key},
    )
    assert failed.status_code == 404
    succeeded = await client.post(
        "/v1/session-runs",
        json=original,
        headers={"Idempotency-Key": rollback_key},
    )
    assert succeeded.status_code == 201
    assert succeeded.headers["Idempotency-Status"] == "stored"


async def test_identical_concurrent_requests_create_one_result(
    client: httpx.AsyncClient,
) -> None:
    """Let PostgreSQL arbitrate concurrent ownership of one key."""
    setup = await _setup_protected_routes(client)
    path = "/v1/session-runs"
    body = {
        "agent_version_id": setup["agent_version_id"],
        "inputs": {"q": "concurrent"},
    }

    first, second = await asyncio.gather(
        client.post(path, json=body, headers={"Idempotency-Key": "concurrent"}),
        client.post(path, json=body, headers={"Idempotency-Key": "concurrent"}),
    )
    assert first.status_code == second.status_code == 201
    assert first.content == second.content
    assert {
        first.headers["Idempotency-Status"],
        second.headers["Idempotency-Status"],
    } == {
        "stored",
        "replayed",
    }


async def test_missing_key_preserves_existing_non_idempotent_behavior(
    client: httpx.AsyncClient,
) -> None:
    """Do not enroll legacy requests unless they supply a key."""
    setup = await _setup_protected_routes(client)
    body = {"agent_version_id": setup["agent_version_id"], "inputs": {}}
    first = await client.post("/v1/session-runs", json=body)
    second = await client.post("/v1/session-runs", json=body)
    assert first.status_code == second.status_code == 201
    assert first.json()["id"] != second.json()["id"]
    assert "Idempotency-Status" not in first.headers


async def test_invalid_keys_are_rejected_before_mutation(
    client: httpx.AsyncClient,
) -> None:
    """Reject empty, duplicated, whitespace, and oversized request keys."""
    setup = await _setup_protected_routes(client)
    body = {"agent_version_id": setup["agent_version_id"], "inputs": {}}
    invalid_headers: list[list[tuple[str, str]]] = [
        [("Idempotency-Key", "")],
        [("Idempotency-Key", "contains space")],
        [("Idempotency-Key", "x" * 256)],
        [("Idempotency-Key", "first"), ("Idempotency-Key", "second")],
    ]
    for headers in invalid_headers:
        response = await client.post("/v1/session-runs", json=body, headers=headers)
        assert response.status_code == 422


async def test_in_flight_predecessor_returns_bounded_http_425() -> None:
    """Return retryable in-progress while another transaction owns the key."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    settings = db_settings(
        IDEMPOTENCY_WAIT_TIMEOUT_SECONDS=0.05,
        TASK_SWEEP_INTERVAL_SECONDS=0,
    )
    await DatabaseService.create_db(settings)
    app = create_app(settings)
    try:
        async with app.router.lifespan_context(app):
            database: DatabaseService = app.state.database
            session_generator = database.get_async_session()
            owner_session = await anext(session_generator)
            try:
                account = await SQLAccountRepository(owner_session).get_by_name(
                    settings.DEFAULT_ACCOUNT_NAME
                )
                await SQLIdempotencyRepository(owner_session).reserve(
                    IdempotencyRecord(
                        actor_account_id=account.id,
                        actor_principal_kind="account",
                        actor_principal_identity=str(account.id),
                        method="POST",
                        route="/v1/session-runs",
                        caller_key="held-request",
                        fingerprint="a" * 64,
                    ),
                    wait_timeout_seconds=1,
                )
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app),
                    base_url="http://test",
                ) as request_client:
                    response = await request_client.post(
                        "/v1/session-runs",
                        json={"agent_version_id": str(uuid.uuid4()), "inputs": {}},
                        headers={"Idempotency-Key": "held-request"},
                    )
                assert response.status_code == 425
                assert response.headers["Retry-After"] == "1"
                assert response.json() == {
                    "detail": (
                        "A request with this idempotency key is still in progress."
                    ),
                    "code": "request_in_progress",
                    "retryable": True,
                }
            finally:
                await owner_session.rollback()
                await session_generator.aclose()
    finally:
        await drop_test_database(settings)
