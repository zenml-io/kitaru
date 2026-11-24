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
"""Round-trip tests for the sessions SDK resource."""

import uuid
from collections.abc import AsyncGenerator
from decimal import Decimal

import pytest

from conftest import asgi_api_client
from kitaru.api_models.v1.filter import (
    AndFilter,
    Filter,
    FilterCondition,
    FilterOp,
)
from kitaru.api_models.v1.session import (
    SessionCreateRequest,
    SessionDetailResponse,
    SessionListParams,
    SessionOrigin,
    SessionResponse,
    SessionStatus,
    SessionUpdateRequest,
)
from kitaru.api_models.v1.session_node import (
    NodeStatus,
    NodeType,
    SessionNodeBatchRequest,
    SessionNodeCreateRequest,
    SessionNodeListParams,
)
from kitaru.api_models.v1.tag import (
    TagCreateRequest,
    TagLinkCreateRequest,
    TagResourceType,
    TagUpdateRequest,
)
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError, NotFoundError
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    authorize_with_task,
    get_session_node_service,
    get_session_service,
    get_tag_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.session_node_service import (
    SessionNodeService,
)
from kitaru.server.application.services.session_service import SessionService
from kitaru.server.application.services.tag_service import TagService
from kitaru.server.domain.account import Account
from kitaru_test_support import (
    FakeAgentRepository,
    FakeAgentVersionRepository,
    FakeImportRepository,
    FakeReplayRepository,
    FakeSessionNodeRepository,
    FakeSessionRepository,
    FakeTagRepository,
    FakeTaskRepository,
    build_payload_store,
    override_idempotency,
)

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
    tag_repository = FakeTagRepository()
    session_repository = FakeSessionRepository(tags=tag_repository)
    node_repository = FakeSessionNodeRepository()
    payload_store = build_payload_store().store
    session_service = SessionService(
        repository=session_repository,
        task_repository=FakeTaskRepository(),
        agent_version_repository=FakeAgentVersionRepository(FakeAgentRepository()),
        replay_repository=FakeReplayRepository(),
        import_repository=FakeImportRepository(),
        payload_store=payload_store,
    )
    node_service = SessionNodeService(
        repository=node_repository,
        session_repository=session_repository,
        task_repository=FakeTaskRepository(),
        payload_store=payload_store,
    )
    app.dependency_overrides[get_session_service] = lambda: session_service
    app.dependency_overrides[get_tag_service] = lambda: TagService(tag_repository)
    app.dependency_overrides[get_session_node_service] = lambda: node_service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    app.dependency_overrides[authorize_with_task] = lambda: AuthContext(account=ACCOUNT)
    override_idempotency(app, ACCOUNT)
    async with asgi_api_client(app) as client:
        yield client


async def test_create(api_client: KitaruAPIClient) -> None:
    """Create a session through the SDK."""
    agent_id = uuid.uuid4()
    session = await api_client.sessions.create(
        SessionCreateRequest(
            agent_id=agent_id,
            origin=SessionOrigin.RECORDED,
            inputs={"prompt": "hi"},
            outputs=None,
            metadata={},
        )
    )
    assert isinstance(session, SessionResponse)
    assert session.agent_id == agent_id
    assert session.owner_id == ACCOUNT.id
    assert session.status == SessionStatus.IN_PROGRESS


async def test_create_duplicate_external_id(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 409 as a typed error."""
    request = SessionCreateRequest(
        agent_id=uuid.uuid4(),
        origin=SessionOrigin.IMPORTED,
        inputs=None,
        outputs=None,
        metadata={},
        imported_from="langsmith",
        external_id="run-1",
    )
    await api_client.sessions.create(request)
    with pytest.raises(APIError) as exc_info:
        await api_client.sessions.create(request)
    assert exc_info.value.status_code == 409


async def test_get(api_client: KitaruAPIClient) -> None:
    """Get a session by id through the SDK."""
    created = await api_client.sessions.create(
        SessionCreateRequest(
            agent_id=uuid.uuid4(),
            origin=SessionOrigin.RECORDED,
            inputs=None,
            outputs=None,
            metadata={},
        )
    )
    loaded = await api_client.sessions.get(created.id)
    assert (
        loaded.model_dump(
            exclude={
                "inputs",
                "outputs",
                "input_text_selector",
                "output_text_selector",
            }
        )
        == created.model_dump()
    )


async def test_get_not_found(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 404 as a typed error."""
    with pytest.raises(NotFoundError):
        await api_client.sessions.get(uuid.uuid4())


async def test_list_and_iter(api_client: KitaruAPIClient) -> None:
    """List and iterate sessions through the SDK."""
    agent_id = uuid.uuid4()
    for _ in range(3):
        await api_client.sessions.create(
            SessionCreateRequest(
                agent_id=agent_id,
                origin=SessionOrigin.RECORDED,
                inputs=None,
                outputs=None,
                metadata={},
            )
        )

    agent_filter = FilterCondition(field="agent_id", op=FilterOp.EQ, value=agent_id)
    page = await api_client.sessions.list(
        SessionListParams(filter=agent_filter, size=2)
    )
    assert len(page.items) == 2
    assert page.next_cursor is not None

    collected = [
        item.id
        async for item in api_client.sessions.iter(
            SessionListParams(filter=agent_filter, size=2)
        )
    ]
    assert len(collected) == 3


async def test_list_include_payloads(api_client: KitaruAPIClient) -> None:
    """List sessions with their payloads through the SDK."""
    agent_id = uuid.uuid4()
    await api_client.sessions.create(
        SessionCreateRequest(
            agent_id=agent_id,
            origin=SessionOrigin.RECORDED,
            inputs={"q": "hi"},
            outputs=None,
            metadata={},
        )
    )
    agent_filter = FilterCondition(field="agent_id", op=FilterOp.EQ, value=agent_id)
    page = await api_client.sessions.list(
        SessionListParams(filter=agent_filter, include_payloads=True)
    )
    assert isinstance(page.items[0], SessionDetailResponse)
    assert page.items[0].inputs == {"q": "hi"}
    assert page.items[0].outputs is None


async def test_list_with_filter_expression(api_client: KitaruAPIClient) -> None:
    """List sessions filtered by a filter expression built from the DTO classes."""
    agent_id = uuid.uuid4()
    matching = await api_client.sessions.create(
        SessionCreateRequest(
            agent_id=agent_id,
            origin=SessionOrigin.RECORDED,
            status=SessionStatus.COMPLETED,
            inputs=None,
            outputs=None,
            metadata={},
        )
    )
    await api_client.sessions.create(
        SessionCreateRequest(
            agent_id=agent_id,
            origin=SessionOrigin.RECORDED,
            inputs=None,
            outputs=None,
            metadata={},
        )
    )

    params = SessionListParams(
        filter=AndFilter(
            **{
                "and": [
                    FilterCondition(field="agent_id", op=FilterOp.EQ, value=agent_id),
                    FilterCondition(field="status", op=FilterOp.EQ, value="completed"),
                ]
            }
        ),
    )
    page = await api_client.sessions.list(params)
    assert [item.id for item in page.items] == [matching.id]


async def test_update(api_client: KitaruAPIClient) -> None:
    """Update a session through the SDK."""
    created = await api_client.sessions.create(
        SessionCreateRequest(
            agent_id=uuid.uuid4(),
            origin=SessionOrigin.RECORDED,
            inputs=None,
            outputs=None,
            metadata={},
        )
    )
    updated = await api_client.sessions.update(
        created.id,
        SessionUpdateRequest(status=SessionStatus.COMPLETED, outputs={"a": 1}),
    )
    assert updated.status == SessionStatus.COMPLETED
    fetched = await api_client.sessions.get(created.id)
    assert fetched.outputs == {"a": 1}


async def test_update_status_conflict(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 409 for an illegal status transition."""
    created = await api_client.sessions.create(
        SessionCreateRequest(
            agent_id=uuid.uuid4(),
            origin=SessionOrigin.RECORDED,
            inputs=None,
            outputs=None,
            metadata={},
        )
    )
    await api_client.sessions.update(
        created.id, SessionUpdateRequest(status=SessionStatus.FAILED)
    )
    with pytest.raises(APIError) as exc_info:
        await api_client.sessions.update(
            created.id, SessionUpdateRequest(status=SessionStatus.IN_PROGRESS)
        )
    assert exc_info.value.status_code == 409


async def test_delete(api_client: KitaruAPIClient) -> None:
    """Delete a session through the SDK."""
    created = await api_client.sessions.create(
        SessionCreateRequest(
            agent_id=uuid.uuid4(),
            origin=SessionOrigin.RECORDED,
            inputs=None,
            outputs=None,
            metadata={},
        )
    )
    await api_client.sessions.delete(created.id)
    with pytest.raises(NotFoundError):
        await api_client.sessions.get(created.id)


async def test_ingest_nodes_and_list_nodes(api_client: KitaruAPIClient) -> None:
    """Ingest and list session nodes through the SDK."""
    created = await api_client.sessions.create(
        SessionCreateRequest(
            agent_id=uuid.uuid4(),
            origin=SessionOrigin.RECORDED,
            inputs=None,
            outputs=None,
            metadata={},
        )
    )
    batch = SessionNodeBatchRequest(
        nodes=[
            SessionNodeCreateRequest(
                external_id="call",
                node_type=NodeType.LLM_CALL,
                name="call",
                status=NodeStatus.COMPLETED,
                cost=Decimal("1.50"),
                inputs={"q": "hi"},
                outputs=None,
                attributes=None,
            ),
            SessionNodeCreateRequest(
                external_id="search",
                parent_external_id="call",
                node_type=NodeType.TOOL_CALL,
                name="search",
                status=NodeStatus.COMPLETED,
                tool_name="search",
                inputs={"q": "hi"},
                outputs=None,
                attributes=None,
            ),
        ]
    )
    stored = await api_client.sessions.ingest_nodes(created.id, batch)
    assert len(stored) == 2
    assert stored[1].parent_external_id == "call"
    assert stored[1].cache_key is not None

    page = await api_client.sessions.list_nodes(created.id)
    assert [item.external_id for item in page.items] == ["call", "search"]
    assert page.items[0].inputs is None

    page = await api_client.sessions.list_nodes(
        created.id, SessionNodeListParams(include_payloads=True)
    )
    assert page.items[0].inputs == {"q": "hi"}


@pytest.mark.parametrize(
    ("filter_", "expected"),
    [
        (None, ["node-0", "node-1", "node-2", "node-3", "node-4"]),
        (
            FilterCondition(field="node_type", op=FilterOp.EQ, value="llm_call"),
            ["node-1", "node-4"],
        ),
        (
            FilterCondition(
                field="node_type", op=FilterOp.IN, value=["llm_call", "tool_call"]
            ),
            ["node-1", "node-3", "node-4"],
        ),
    ],
)
async def test_iter_nodes(
    api_client: KitaruAPIClient, filter_: Filter | None, expected: list[str]
) -> None:
    """Iterate every node of a session across pages through the SDK."""
    created = await api_client.sessions.create(
        SessionCreateRequest(
            agent_id=uuid.uuid4(),
            origin=SessionOrigin.RECORDED,
            inputs=None,
            outputs=None,
            metadata={},
        )
    )
    batch = SessionNodeBatchRequest(
        nodes=[
            SessionNodeCreateRequest(
                external_id=f"node-{index}",
                node_type=[
                    NodeType.SPAN,
                    NodeType.LLM_CALL,
                    NodeType.SPAN,
                    NodeType.TOOL_CALL,
                    NodeType.LLM_CALL,
                ][index],
                name="span",
                status=NodeStatus.COMPLETED,
                inputs=None,
                outputs=None,
                attributes=None,
            )
            for index in range(5)
        ]
    )
    await api_client.sessions.ingest_nodes(created.id, batch)

    collected = [
        item.external_id
        async for item in api_client.sessions.iter_nodes(
            created.id, SessionNodeListParams(size=2, filter=filter_)
        )
    ]
    assert collected == expected


async def test_tag_alias_pagination_and_current_tag_responses(
    api_client: KitaruAPIClient,
) -> None:
    """Return linked tag data through SDK list, detail and update responses."""
    sessions = [
        await api_client.sessions.create(
            SessionCreateRequest(
                agent_id=uuid.uuid4(),
                origin=SessionOrigin.RECORDED,
                inputs={"secret": "payload"},
                outputs=None,
                metadata={},
            )
        )
        for _ in range(4)
    ]
    assert all(session.tags == [] for session in sessions)
    prod = await api_client.tags.create(TagCreateRequest(name="prod"))
    excluded = await api_client.tags.create(TagCreateRequest(name="excluded"))
    unrelated = await api_client.tags.create(
        TagCreateRequest(name="other-session-only")
    )
    for session in sessions[:3]:
        await api_client.tags.create_link(
            prod.id,
            TagLinkCreateRequest(
                resource_type=TagResourceType.SESSION, resource_id=session.id
            ),
        )
    for tag_id, session_id in (
        (excluded.id, sessions[2].id),
        (unrelated.id, sessions[3].id),
    ):
        await api_client.tags.create_link(
            tag_id,
            TagLinkCreateRequest(
                resource_type=TagResourceType.SESSION, resource_id=session_id
            ),
        )

    def expression(field: str) -> AndFilter:
        return AndFilter.model_validate(
            {
                "and": [
                    {
                        "or": [
                            {"field": field, "op": "in", "value": ["prod", "missing"]},
                            {"field": "name", "op": "eq", "value": "missing"},
                        ]
                    },
                    {"not": {"field": field, "op": "eq", "value": "excluded"}},
                ]
            }
        )

    first = await api_client.sessions.list(
        SessionListParams(filter=expression("tags"), size=1, sort="created:asc")
    )
    assert [session.id for session in first.items] == [sessions[0].id]
    assert first.items[0].tags == [prod]
    assert "inputs" not in first.items[0].model_dump()
    assert first.next_cursor is not None
    second = await api_client.sessions.list(
        SessionListParams(
            filter=expression("tag"),
            size=1,
            sort="created:asc",
            cursor=first.next_cursor,
        )
    )
    assert [session.id for session in second.items] == [sessions[1].id]
    assert second.items[0].tags == [prod]
    assert second.next_cursor is None
    detail = await api_client.sessions.get(sessions[0].id)
    assert detail.tags == [prod]
    assert detail.inputs == {"secret": "payload"}
    renamed = await api_client.tags.update(prod.id, TagUpdateRequest(name="production"))
    updated = await api_client.sessions.update(
        sessions[0].id, SessionUpdateRequest(name="updated")
    )
    assert updated.tags == [renamed]
    await api_client.tags.delete_link(prod.id, TagResourceType.SESSION, sessions[0].id)
    assert (await api_client.sessions.get(sessions[0].id)).tags == []


async def test_old_server_responses_without_tags_still_parse(
    api_client: KitaruAPIClient,
) -> None:
    """Accept old list and detail wire responses that omitted tags."""
    created = await api_client.sessions.create(
        SessionCreateRequest(
            agent_id=uuid.uuid4(),
            origin=SessionOrigin.RECORDED,
            inputs=None,
            outputs=None,
            metadata={},
        )
    )
    assert (
        SessionResponse.model_validate(created.model_dump(exclude={"tags"})).tags == []
    )
    detail = await api_client.sessions.get(created.id)
    assert (
        SessionDetailResponse.model_validate(detail.model_dump(exclude={"tags"})).tags
        == []
    )
