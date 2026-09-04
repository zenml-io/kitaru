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

from conftest import (
    FakeAgentRepository,
    FakeAgentVersionRepository,
    FakeApiKeyRepository,
    FakeIdempotencyKeyRepository,
    FakeTagRepository,
    marked_idempotent_routes,
)
from kitaru.server.adapters.rest.dependencies import (
    _resolve_auth_context,
    get_agent_version_service,
    get_api_key_service,
    get_idempotency_key_repository,
    get_tag_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import (
    AuthContext,
    WorkerAuthContext,
    WorkerPrincipal,
)
from kitaru.server.application.services.agent_version_service import (
    AgentVersionService,
)
from kitaru.server.application.services.api_key_service import ApiKeyService
from kitaru.server.application.services.tag_service import TagService
from kitaru.server.domain.account import Account
from kitaru.server.domain.idempotency_key import (
    MAX_IDEMPOTENCY_KEY_LENGTH,
    MAX_IDEMPOTENCY_PATH_LENGTH,
)

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
    ("POST", "/api/v1/insights"),
    ("POST", "/api/v1/investigations"),
    ("POST", "/api/v1/api-keys"),
    ("POST", "/api/v1/api-keys/{api_key_id}/rotate"),
    ("POST", "/api/v1/evaluators"),
    ("POST", "/api/v1/evaluators/{evaluator_id}/versions"),
    ("POST", "/api/v1/importers"),
    ("POST", "/api/v1/importers/{importer_id}/versions"),
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
    context: AuthContext | None = None,
) -> httpx.AsyncClient:
    """Build an HTTP client for the app authorized as the given account.

    Args:
        tag_repository: Fake tag repository backing the app.
        idempotency_key_repository: Fake idempotency key repository backing
            the app.
        account: Account the client authenticates as.
        context: Resolved auth context, defaulting to an account context.

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
    app.dependency_overrides[get_api_key_service] = lambda: ApiKeyService(
        repository=FakeApiKeyRepository()
    )
    app.dependency_overrides[get_agent_version_service] = lambda: AgentVersionService(
        repository=FakeAgentVersionRepository(FakeAgentRepository())
    )
    resolved = context if context is not None else AuthContext(account=account)
    app.dependency_overrides[_resolve_auth_context] = lambda: resolved
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
    marked = marked_idempotent_routes(app)
    assert marked == EXPECTED_IDEMPOTENT_ROUTES
    assert ("POST", "/api/v1/tasks/claim") not in marked


async def test_openapi_schema_documents_the_header_on_the_expected_routes() -> None:
    """Assert the OpenAPI schema declares the header on exactly the expected routes."""
    app = create_app(_settings())
    schema = app.openapi()
    documented = {
        (method.upper(), path)
        for path, operations in schema["paths"].items()
        for method, operation in operations.items()
        for parameter in operation.get("parameters", [])
        if parameter.get("name") == "Idempotency-Key"
        and parameter.get("in") == "header"
    }
    assert documented == EXPECTED_IDEMPOTENT_ROUTES


async def test_non_printable_header_returns_400(client: httpx.AsyncClient) -> None:
    """Reject an Idempotency-Key header carrying control characters."""
    response = await client.post(
        "/api/v1/tags",
        json={"name": "prod"},
        headers={b"Idempotency-Key": b"abc\x00def"},
    )
    assert response.status_code == 400
    assert response.json() == {"detail": "Invalid Idempotency-Key header."}


async def test_overlong_path_returns_400(client: httpx.AsyncClient) -> None:
    """Reject a keyed request whose path exceeds the stored path length."""
    garbage = "a" * (MAX_IDEMPOTENCY_PATH_LENGTH + 1)
    response = await client.post(
        f"/api/v1/agents/{garbage}/versions",
        json={},
        headers={"Idempotency-Key": "long-path"},
    )
    assert response.status_code == 400
    assert response.json() == {"detail": "Request path too long for Idempotency-Key."}


async def test_query_string_is_part_of_the_fingerprint(
    client: httpx.AsyncClient,
) -> None:
    """Reject a reused key whose query string differs from the stored request."""
    headers = {"Idempotency-Key": "create-prod"}
    first = await client.post(
        "/api/v1/tags?source=a", json={"name": "prod"}, headers=headers
    )
    assert first.status_code == 201

    second = await client.post(
        "/api/v1/tags?source=b", json={"name": "prod"}, headers=headers
    )
    assert second.status_code == 422


async def test_replay_runs_after_authorization(
    tag_repository: FakeTagRepository,
    idempotency_key_repository: FakeIdempotencyKeyRepository,
) -> None:
    """Reject a disallowed principal before replaying a stored response."""
    headers = {"Idempotency-Key": "create-prod"}
    body = {"name": "prod"}
    async with _build_client(
        tag_repository, idempotency_key_repository, ACCOUNT
    ) as account_client:
        first = await account_client.post("/api/v1/tags", json=body, headers=headers)
    assert first.status_code == 201

    worker = WorkerAuthContext(
        account=ACCOUNT, principal=WorkerPrincipal(worker_id=uuid.uuid4())
    )
    async with _build_client(
        tag_repository, idempotency_key_repository, ACCOUNT, context=worker
    ) as worker_client:
        replay = await worker_client.post("/api/v1/tags", json=body, headers=headers)
    assert replay.status_code == 403
    assert "Idempotent-Replayed" not in replay.headers


async def test_api_key_responses_are_stored_encrypted(
    client: httpx.AsyncClient,
    idempotency_key_repository: FakeIdempotencyKeyRepository,
) -> None:
    """Store the issued API key response through the encrypting path."""
    response = await client.post(
        "/api/v1/api-keys",
        json={"name": "ci"},
        headers={"Idempotency-Key": "create-key"},
    )
    assert response.status_code == 201

    stored = await idempotency_key_repository.get(ACCOUNT.id, "create-key")
    assert stored is not None
    assert stored.id in idempotency_key_repository.encrypted_ids

    tag = await client.post(
        "/api/v1/tags", json={"name": "prod"}, headers={"Idempotency-Key": "tag"}
    )
    assert tag.status_code == 201
    stored_tag = await idempotency_key_repository.get(ACCOUNT.id, "tag")
    assert stored_tag is not None
    assert stored_tag.id not in idempotency_key_repository.encrypted_ids


async def test_api_key_replay_decrypts_the_stored_response(
    client: httpx.AsyncClient,
) -> None:
    """Replay the decrypted stored response on an encrypting route."""
    headers = {"Idempotency-Key": "create-key"}
    body = {"name": "ci"}
    first = await client.post("/api/v1/api-keys", json=body, headers=headers)
    assert first.status_code == 201

    second = await client.post("/api/v1/api-keys", json=body, headers=headers)
    assert second.status_code == 201
    assert second.headers["Idempotent-Replayed"] == "true"
    assert second.json() == first.json()


async def test_plaintext_key_reused_on_encrypting_route_returns_422(
    client: httpx.AsyncClient,
) -> None:
    """Reject a key registered on a plaintext route before decrypting anything."""
    headers = {"Idempotency-Key": "shared-key"}
    first = await client.post("/api/v1/tags", json={"name": "prod"}, headers=headers)
    assert first.status_code == 201

    second = await client.post("/api/v1/api-keys", json={"name": "ci"}, headers=headers)
    assert second.status_code == 422
    assert second.json() == {
        "detail": "Idempotency-Key was already used with a different request"
    }

    rotate = await client.post(
        f"/api/v1/api-keys/{uuid.uuid4()}/rotate", headers=headers
    )
    assert rotate.status_code == 422


async def test_encrypted_key_reused_on_plaintext_route_returns_422(
    client: httpx.AsyncClient,
) -> None:
    """Reject a key registered on an encrypting route and reused on a plaintext one."""
    headers = {"Idempotency-Key": "shared-key"}
    first = await client.post("/api/v1/api-keys", json={"name": "ci"}, headers=headers)
    assert first.status_code == 201

    second = await client.post("/api/v1/tags", json={"name": "prod"}, headers=headers)
    assert second.status_code == 422


async def test_undecryptable_stored_response_returns_409(
    client: httpx.AsyncClient,
    idempotency_key_repository: FakeIdempotencyKeyRepository,
) -> None:
    """Reject a replay whose stored response cannot be decrypted."""
    headers = {"Idempotency-Key": "create-key"}
    body = {"name": "ci"}
    first = await client.post("/api/v1/api-keys", json=body, headers=headers)
    assert first.status_code == 201

    stored = await idempotency_key_repository.get(ACCOUNT.id, "create-key")
    assert stored is not None
    await idempotency_key_repository.store_response(
        stored.id,
        response_status=201,
        response_body=b"not-encrypted",
        response_content_type="application/json",
    )

    second = await client.post("/api/v1/api-keys", json=body, headers=headers)
    assert second.status_code == 409
    assert second.json() == {
        "detail": "Idempotency-Key stored response cannot be decrypted"
    }
