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
"""Round-trip tests for the cohorts and cohort versions SDK resources."""

import uuid
from collections.abc import AsyncGenerator

import pytest

from conftest import (
    FakeAgentRepository,
    FakeAgentVersionRepository,
    FakeCohortRepository,
    FakeCohortVersionRepository,
    FakeReplayRepository,
    FakeSessionRepository,
    FakeTagRepository,
    FakeTaskRepository,
    asgi_api_client,
    build_payload_offload_service,
    override_idempotency,
)
from kitaru.api_models.v1.agent import AgentCreateRequest
from kitaru.api_models.v1.cohort import (
    CohortCreateRequest,
    CohortListParams,
    CohortResponse,
    CohortUpdateRequest,
)
from kitaru.api_models.v1.cohort_version import (
    CohortVersionCreateRequest,
    CohortVersionListParams,
    CohortVersionResponse,
    CohortVersionUpdateRequest,
)
from kitaru.api_models.v1.filter import FilterCondition, FilterOp
from kitaru.api_models.v1.session import SessionCreateRequest, SessionOrigin
from kitaru.api_models.v1.tag import (
    TagCreateRequest,
    TagLinkCreateRequest,
    TagResourceType,
)
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError, NotFoundError
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    authorize_with_task,
    get_agent_service,
    get_cohort_service,
    get_cohort_version_service,
    get_session_service,
    get_tag_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.agent_service import AgentService
from kitaru.server.application.services.cohort_service import CohortService
from kitaru.server.application.services.cohort_version_service import (
    CohortVersionService,
)
from kitaru.server.application.services.session_service import SessionService
from kitaru.server.application.services.tag_service import TagService
from kitaru.server.domain.account import Account

ACCOUNT = Account(id=uuid.uuid4(), name="ann")


@pytest.fixture
async def api_client() -> AsyncGenerator[KitaruAPIClient, None]:
    """Provide an API client routed to the app with fake-backed services."""
    app = create_app(
        APISettings(
            DB_HOST="localhost",
            SECRET_ENCRYPTION_KEY="test-encryption-key",
            JWT_SIGNING_KEY="test-signing-key-0123456789abcdef",
        )
    )
    agent_repository = FakeAgentRepository()
    session_repository = FakeSessionRepository()
    cohort_repository = FakeCohortRepository()
    tag_repository = FakeTagRepository()
    cohort_version_repository = FakeCohortVersionRepository(
        cohorts=cohort_repository, sessions=session_repository, tags=tag_repository
    )
    app.dependency_overrides[get_agent_service] = lambda: AgentService(
        repository=agent_repository
    )
    app.dependency_overrides[get_session_service] = lambda: SessionService(
        repository=session_repository,
        task_repository=FakeTaskRepository(),
        agent_version_repository=FakeAgentVersionRepository(agent_repository),
        replay_repository=FakeReplayRepository(),
        payload_offload=build_payload_offload_service(),
    )
    app.dependency_overrides[get_cohort_service] = lambda: CohortService(
        repository=cohort_repository, agent_repository=agent_repository
    )
    app.dependency_overrides[get_cohort_version_service] = lambda: CohortVersionService(
        repository=cohort_version_repository,
        cohort_repository=cohort_repository,
        session_repository=session_repository,
    )
    app.dependency_overrides[get_tag_service] = lambda: TagService(
        repository=tag_repository
    )
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    app.dependency_overrides[authorize_with_task] = lambda: AuthContext(account=ACCOUNT)
    override_idempotency(app, ACCOUNT)
    async with asgi_api_client(app) as client:
        yield client


async def _make_agent(api_client: KitaruAPIClient) -> uuid.UUID:
    """Create an agent through the SDK."""
    agent = await api_client.agents.create(
        AgentCreateRequest(name=f"assistant-{uuid.uuid4().hex[:8]}")
    )
    return agent.id


async def _make_session(api_client: KitaruAPIClient, agent_id: uuid.UUID) -> uuid.UUID:
    """Create a session on the given agent through the SDK."""
    session = await api_client.sessions.create(
        SessionCreateRequest(
            agent_id=agent_id,
            origin=SessionOrigin.RECORDED,
            inputs=None,
            outputs=None,
        )
    )
    return session.id


async def test_create(api_client: KitaruAPIClient) -> None:
    """Create a cohort through the SDK."""
    agent_id = await _make_agent(api_client)
    cohort = await api_client.cohorts.create(
        CohortCreateRequest(
            name="smoke-test", description="A cohort", agent_id=agent_id
        )
    )
    assert isinstance(cohort, CohortResponse)
    assert cohort.name == "smoke-test"
    assert cohort.owner_id == ACCOUNT.id
    assert cohort.agent_id == agent_id
    assert cohort.latest_version == 0


async def test_create_duplicate_name(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 409 as a typed error."""
    agent_id = await _make_agent(api_client)
    await api_client.cohorts.create(
        CohortCreateRequest(name="smoke-test", agent_id=agent_id)
    )
    with pytest.raises(APIError) as exc_info:
        await api_client.cohorts.create(
            CohortCreateRequest(name="smoke-test", agent_id=agent_id)
        )
    assert exc_info.value.status_code == 409
    assert exc_info.value.detail == "Cohort name 'smoke-test' is already registered"


async def test_get(api_client: KitaruAPIClient) -> None:
    """Get a cohort by id through the SDK."""
    agent_id = await _make_agent(api_client)
    created = await api_client.cohorts.create(
        CohortCreateRequest(name="cohort", agent_id=agent_id)
    )
    loaded = await api_client.cohorts.get(created.id)
    assert loaded == created


async def test_get_not_found(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 404 as a typed error."""
    with pytest.raises(NotFoundError):
        await api_client.cohorts.get(uuid.uuid4())


async def test_list(api_client: KitaruAPIClient) -> None:
    """List cohorts newest-first with filters through the SDK."""
    agent_id = await _make_agent(api_client)
    for name in ["alpha", "beta"]:
        await api_client.cohorts.create(
            CohortCreateRequest(name=name, agent_id=agent_id)
        )

    page = await api_client.cohorts.list()
    assert page.next_cursor is None
    assert [item.name for item in page.items] == ["beta", "alpha"]

    page = await api_client.cohorts.list(
        CohortListParams(
            filter=FilterCondition(field="name", op=FilterOp.EQ, value="alpha")
        )
    )
    assert page.items[0].name == "alpha"


async def test_iter(api_client: KitaruAPIClient) -> None:
    """Iterate every cohort across pages through the SDK."""
    agent_id = await _make_agent(api_client)
    for name in ["alpha", "beta", "gamma"]:
        await api_client.cohorts.create(
            CohortCreateRequest(name=name, agent_id=agent_id)
        )

    collected = [
        item.name async for item in api_client.cohorts.iter(CohortListParams(size=1))
    ]
    assert collected == ["gamma", "beta", "alpha"]


async def test_update(api_client: KitaruAPIClient) -> None:
    """Update a cohort through the SDK."""
    agent_id = await _make_agent(api_client)
    created = await api_client.cohorts.create(
        CohortCreateRequest(name="cohort", description="old", agent_id=agent_id)
    )
    updated = await api_client.cohorts.update(
        created.id, CohortUpdateRequest(description="new")
    )
    assert updated.description == "new"


async def test_delete(api_client: KitaruAPIClient) -> None:
    """Delete a cohort through the SDK."""
    agent_id = await _make_agent(api_client)
    created = await api_client.cohorts.create(
        CohortCreateRequest(name="cohort", agent_id=agent_id)
    )
    await api_client.cohorts.delete(created.id)
    with pytest.raises(NotFoundError):
        await api_client.cohorts.get(created.id)


async def test_create_version(api_client: KitaruAPIClient) -> None:
    """Create a version of a cohort through the SDK."""
    agent_id = await _make_agent(api_client)
    session_ids = [await _make_session(api_client, agent_id) for _ in range(2)]
    cohort = await api_client.cohorts.create(
        CohortCreateRequest(name="cohort", agent_id=agent_id)
    )
    version = await api_client.cohorts.create_version(
        cohort.id,
        CohortVersionCreateRequest(add_session_ids=session_ids, display_version="v1"),
    )
    assert isinstance(version, CohortVersionResponse)
    assert version.cohort_id == cohort.id
    assert version.version == 1
    assert version.display_version == "v1"
    assert version.session_count == 2


async def test_create_version_from_baseline(api_client: KitaruAPIClient) -> None:
    """Create a version from an older baseline through the SDK."""
    agent_id = await _make_agent(api_client)
    session_ids = [await _make_session(api_client, agent_id) for _ in range(2)]
    cohort = await api_client.cohorts.create(
        CohortCreateRequest(name="cohort", agent_id=agent_id)
    )
    baseline = await api_client.cohorts.create_version(
        cohort.id, CohortVersionCreateRequest(add_session_ids=session_ids)
    )
    await api_client.cohorts.create_version(
        cohort.id, CohortVersionCreateRequest(remove_session_ids=[session_ids[0]])
    )
    restored = await api_client.cohorts.create_version(
        cohort.id, CohortVersionCreateRequest(baseline_id=baseline.id)
    )
    assert restored.version == 3
    assert restored.session_count == 2


async def test_list_versions(api_client: KitaruAPIClient) -> None:
    """List the versions of a cohort through the SDK."""
    agent_id = await _make_agent(api_client)
    other_agent_id = await _make_agent(api_client)
    cohort = await api_client.cohorts.create(
        CohortCreateRequest(name="cohort", agent_id=agent_id)
    )
    other_cohort = await api_client.cohorts.create(
        CohortCreateRequest(name="other", agent_id=other_agent_id)
    )
    v1 = await api_client.cohorts.create_version(
        cohort.id, CohortVersionCreateRequest()
    )
    v2 = await api_client.cohorts.create_version(
        cohort.id, CohortVersionCreateRequest()
    )
    await api_client.cohorts.create_version(
        other_cohort.id, CohortVersionCreateRequest()
    )

    page = await api_client.cohorts.list_versions(cohort.id)
    assert page.next_cursor is None
    assert [item.id for item in page.items] == [v2.id, v1.id]


async def test_list_versions_filters_by_tag(api_client: KitaruAPIClient) -> None:
    """List versions filtered by tag through the SDK."""
    agent_id = await _make_agent(api_client)
    cohort = await api_client.cohorts.create(
        CohortCreateRequest(name="cohort", agent_id=agent_id)
    )
    tagged = await api_client.cohorts.create_version(
        cohort.id, CohortVersionCreateRequest()
    )
    await api_client.cohorts.create_version(cohort.id, CohortVersionCreateRequest())
    tag = await api_client.tags.create(TagCreateRequest(name="smoke-test"))
    await api_client.tags.create_link(
        tag.id,
        TagLinkCreateRequest(
            resource_type=TagResourceType.COHORT_VERSION, resource_id=tagged.id
        ),
    )

    page = await api_client.cohorts.list_versions(
        cohort.id,
        CohortVersionListParams(
            filter=FilterCondition(field="tag", op=FilterOp.EQ, value="smoke-test")
        ),
    )
    assert [item.id for item in page.items] == [tagged.id]


async def test_iter_versions(api_client: KitaruAPIClient) -> None:
    """Iterate every version of a cohort across pages through the SDK."""
    agent_id = await _make_agent(api_client)
    cohort = await api_client.cohorts.create(
        CohortCreateRequest(name="cohort", agent_id=agent_id)
    )
    created = [
        await api_client.cohorts.create_version(cohort.id, CohortVersionCreateRequest())
        for _ in range(3)
    ]

    collected = [
        item.id
        async for item in api_client.cohorts.iter_versions(
            cohort.id, CohortVersionListParams(size=1)
        )
    ]

    assert collected == list(reversed([version.id for version in created]))


async def test_get_version(api_client: KitaruAPIClient) -> None:
    """Get a cohort version by id through the SDK."""
    agent_id = await _make_agent(api_client)
    cohort = await api_client.cohorts.create(
        CohortCreateRequest(name="cohort", agent_id=agent_id)
    )
    created = await api_client.cohorts.create_version(
        cohort.id, CohortVersionCreateRequest(display_version="v1")
    )
    loaded = await api_client.cohort_versions.get(created.id)
    assert loaded == created


async def test_get_version_not_found(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 404 as a typed error."""
    with pytest.raises(NotFoundError):
        await api_client.cohort_versions.get(uuid.uuid4())


async def test_update_version(api_client: KitaruAPIClient) -> None:
    """Update a cohort version through the SDK."""
    agent_id = await _make_agent(api_client)
    cohort = await api_client.cohorts.create(
        CohortCreateRequest(name="cohort", agent_id=agent_id)
    )
    created = await api_client.cohorts.create_version(
        cohort.id, CohortVersionCreateRequest(display_version="v1")
    )
    updated = await api_client.cohort_versions.update(
        created.id, CohortVersionUpdateRequest(display_version="v1.1")
    )
    assert updated.display_version == "v1.1"


async def test_update_version_clears_display_version(
    api_client: KitaruAPIClient,
) -> None:
    """Clear the display version with an explicit null through the SDK."""
    agent_id = await _make_agent(api_client)
    cohort = await api_client.cohorts.create(
        CohortCreateRequest(name="cohort", agent_id=agent_id)
    )
    created = await api_client.cohorts.create_version(
        cohort.id, CohortVersionCreateRequest(display_version="v1")
    )
    updated = await api_client.cohort_versions.update(
        created.id, CohortVersionUpdateRequest(display_version=None)
    )
    assert updated.display_version is None


async def test_delete_version(api_client: KitaruAPIClient) -> None:
    """Delete a cohort version through the SDK."""
    agent_id = await _make_agent(api_client)
    cohort = await api_client.cohorts.create(
        CohortCreateRequest(name="cohort", agent_id=agent_id)
    )
    created = await api_client.cohorts.create_version(
        cohort.id, CohortVersionCreateRequest()
    )
    await api_client.cohort_versions.delete(created.id)
    with pytest.raises(NotFoundError):
        await api_client.cohort_versions.get(created.id)
