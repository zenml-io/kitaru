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
"""Tests for idempotency key enforcement."""

import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest
from fastapi.routing import iter_route_contexts

from conftest import FakeIdempotencyKeyRepository, FakeTagRepository
from kitaru.server.adapters.rest.commit_route import is_idempotent
from kitaru.server.adapters.rest.dependencies import (
    _resolve_auth_context,
    authorize,
    get_idempotency_key_repository,
    get_tag_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.tag_service import TagService
from kitaru.server.domain.account import Account
from kitaru.server.domain.idempotency_key import MAX_IDEMPOTENCY_KEY_LENGTH

ACCOUNT = Account(id=uuid.uuid4(), name="ann")
OTHER_ACCOUNT = Account(id=uuid.uuid4(), name="bob")

EXPECTED_IDEMPOTENT_ROUTES = {
    ("POST", "/api/v1/sessions"),
    ("POST", "/api/v1/replays"),
    ("POST", "/api/v1/evaluations"),
    ("POST", "/api/v1/session-runs"),
    ("POST", "/api/v1/imports"),
    ("POST", "/api/v1/agents"),
    ("POST", "/api/v1/agents/{agent_id}/versions"),
    ("POST", "/api/v1/cohorts"),
    ("POST", "/api/v1/cohorts/{cohort_id}/versions"),
    ("POST", "/api/v1/experiments"),
    ("POST", "/api/v1/experiments/{experiment_id}/runs"),
    ("POST", "/api/v1/annotations"),
    ("POST", "/api/v1/investigations"),
    ("POST", "/api/v1/api-keys"),
    ("POST", "/api/v1/api-keys/{api_key_id}/rotate"),
    ("POST", "/api/v1/evaluators"),
    ("POST", "/api/v1/importers"),
    ("POST", "/api/v1/tags"),
    ("POST", "/api/v1/secrets"),
    ("POST", "/api/v1/service-accounts"),
    ("POST", "/api/v1/users"),
}


def _settings() -> APISettings:
    return APISettings(
        DB_HOST="localhost",
        SECRET_ENCRYPTION_KEY="test-encryption-key",
        JWT_SIGNING_KEY="test-signing-key-0123456789abcdef",
    )


@pytest.fixture
def tag_repository() -> FakeTagRepository:
    """Provide the fake tag repository backing the app."""
    return FakeTagRepository()


@pytest.fixture
def idempotency_key_repository() -> FakeIdempotencyKeyRepository:
    """Provide the fake idempotency key repository backing the app."""
    return FakeIdempotencyKeyRepository()


def _build_client(
    tag_repository: FakeTagRepository,
    idempotency_key_repository: FakeIdempotencyKeyRepository,
    account: Account,
) -> httpx.AsyncClient:
    """Build an HTTP client for the app authorized as the given account.

    Args:
        tag_repository: Fake tag repository backing the app.
        idempotency_key_repository: Fake idempotency key repository backing
            the app.
        account: Account the client authenticates as.

    Returns:
        HTTP client routed to the app.
    """
    app = create_app(_settings())
    app.dependency_overrides[get_tag_service] = lambda: TagService(
        repository=tag_repository
    )
    app.dependency_overrides[get_idempotency_key_repository] = lambda: (
        idempotency_key_repository
    )
    app.dependency_overrides[authorize] = lambda: AuthContext(account=account)
    app.dependency_overrides[_resolve_auth_context] = lambda: AuthContext(
        account=account
    )
    transport = httpx.ASGITransport(app=app)
    return httpx.AsyncClient(transport=transport, base_url="http://test")


@pytest.fixture
async def client(
    tag_repository: FakeTagRepository,
    idempotency_key_repository: FakeIdempotencyKeyRepository,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app with a fake-backed tag service."""
    async with _build_client(
        tag_repository, idempotency_key_repository, ACCOUNT
    ) as client:
        yield client


async def test_create_tag_without_header_runs_normally(
    client: httpx.AsyncClient,
    idempotency_key_repository: FakeIdempotencyKeyRepository,
) -> None:
    """Run a request without the header normally, storing no key row."""
    response = await client.post("/api/v1/tags", json={"name": "prod"})
    assert response.status_code == 201
    assert "Idempotent-Replayed" not in response.headers
    assert await idempotency_key_repository.get(ACCOUNT.id, "any-key") is None


async def test_replay_returns_the_stored_response(
    client: httpx.AsyncClient,
) -> None:
    """Replay a stored response for a repeated key and identical body."""
    headers = {"Idempotency-Key": "create-prod"}
    body = {"name": "prod"}

    first = await client.post("/api/v1/tags", json=body, headers=headers)
    assert first.status_code == 201
    assert "Idempotent-Replayed" not in first.headers

    second = await client.post("/api/v1/tags", json=body, headers=headers)
    assert second.status_code == 201
    assert second.headers["Idempotent-Replayed"] == "true"
    assert second.json() == first.json()

    listing = await client.get("/api/v1/tags")
    assert len(listing.json()["items"]) == 1


async def test_replay_mismatched_body_returns_422(client: httpx.AsyncClient) -> None:
    """Reject a reused key whose body no longer matches the stored fingerprint."""
    headers = {"Idempotency-Key": "create-prod"}
    first = await client.post("/api/v1/tags", json={"name": "prod"}, headers=headers)
    assert first.status_code == 201

    second = await client.post(
        "/api/v1/tags", json={"name": "staging"}, headers=headers
    )
    assert second.status_code == 422
    assert second.json() == {
        "detail": "Idempotency-Key was already used with a different request"
    }


async def test_different_accounts_do_not_collide(
    tag_repository: FakeTagRepository,
    idempotency_key_repository: FakeIdempotencyKeyRepository,
) -> None:
    """Scope the key to the account, so two accounts may reuse the same key."""
    headers = {"Idempotency-Key": "create-prod"}
    async with _build_client(
        tag_repository, idempotency_key_repository, ACCOUNT
    ) as ann_client:
        first = await ann_client.post(
            "/api/v1/tags", json={"name": "ann-tag"}, headers=headers
        )
    assert first.status_code == 201

    async with _build_client(
        tag_repository, idempotency_key_repository, OTHER_ACCOUNT
    ) as bob_client:
        second = await bob_client.post(
            "/api/v1/tags", json={"name": "bob-tag"}, headers=headers
        )
    assert second.status_code == 201
    assert "Idempotent-Replayed" not in second.headers
    assert second.json()["id"] != first.json()["id"]


async def test_empty_header_returns_400(client: httpx.AsyncClient) -> None:
    """Reject an empty Idempotency-Key header."""
    response = await client.post(
        "/api/v1/tags", json={"name": "prod"}, headers={"Idempotency-Key": "   "}
    )
    assert response.status_code == 400
    assert response.json() == {"detail": "Invalid Idempotency-Key header."}


async def test_overlong_header_returns_400(client: httpx.AsyncClient) -> None:
    """Reject an Idempotency-Key header longer than the maximum length."""
    response = await client.post(
        "/api/v1/tags",
        json={"name": "prod"},
        headers={"Idempotency-Key": "a" * (MAX_IDEMPOTENCY_KEY_LENGTH + 1)},
    )
    assert response.status_code == 400
    assert response.json() == {"detail": "Invalid Idempotency-Key header."}


async def test_unmarked_route_ignores_the_header(
    client: httpx.AsyncClient,
    idempotency_key_repository: FakeIdempotencyKeyRepository,
) -> None:
    """Ignore the header on a route that carries no idempotent marker."""
    tag = (await client.post("/api/v1/tags", json={"name": "prod"})).json()
    headers = {"Idempotency-Key": "link-it"}
    body = {"resource_type": "session", "resource_id": str(uuid.uuid4())}

    first = await client.post(
        f"/api/v1/tags/{tag['id']}/links", json=body, headers=headers
    )
    assert first.status_code == 201
    assert "Idempotent-Replayed" not in first.headers
    assert await idempotency_key_repository.get(ACCOUNT.id, "link-it") is None

    second = await client.post(
        f"/api/v1/tags/{tag['id']}/links", json=body, headers=headers
    )
    assert second.status_code == 409
    assert await idempotency_key_repository.get(ACCOUNT.id, "link-it") is None


async def test_idempotent_routes_match_the_expected_set() -> None:
    """Assert exactly the specified routes carry the idempotent marker."""
    app = create_app(_settings())
    marked = {
        (method, context.path)
        for context in iter_route_contexts(app.routes)
        if context.endpoint is not None and is_idempotent(context.endpoint)
        for method in context.methods or ()
    }
    assert marked == EXPECTED_IDEMPOTENT_ROUTES
    assert ("POST", "/api/v1/tasks/claim") not in marked
