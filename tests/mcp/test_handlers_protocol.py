#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
"""Focused handler, pagination, protocol, and destructive contracts."""

import json
import uuid
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast

import pytest
from mcp.server import MCPServer, ServerRequestContext
from mcp.server.mcpserver import Context
from mcp.types import CallToolResult, TextContent
from pydantic import ValidationError

from kitaru.api_models.v1.agent import AgentResponse
from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.investigation import InvestigationSessionResponse
from kitaru.api_models.v1.session import SessionResponse
from kitaru.mcp.lifecycle import MCPServerState
from kitaru.mcp.models.activity import ActivityListRequest
from kitaru.mcp.models.common import PageData
from kitaru.mcp.models.management import (
    CohortUpdate,
    CohortVersionCreate,
    ExperimentUpdate,
)
from kitaru.mcp.server import create_server
from kitaru.mcp.settings import CapabilityMode, MCPSettings
from kitaru.mcp.tools.activity import handle_activity_read
from kitaru.mcp.tools.cohorts import handle_cohorts_manage
from kitaru.mcp.tools.experiments import handle_experiments_manage


class UpdateClient:
    """Capture sparse update DTOs passed to typed SDK resources."""

    def __init__(self) -> None:
        self.cohort_updates: list[object] = []
        self.experiment_updates: list[object] = []
        self.cohort_version_creates: list[object] = []
        self.cohorts = SimpleNamespace(
            update=self._update_cohort, create_version=self._create_cohort_version
        )
        self.experiments = SimpleNamespace(update=self._update_experiment)

    async def _update_cohort(self, _item_id: uuid.UUID, request: object) -> object:
        self.cohort_updates.append(request)
        return SimpleNamespace()

    async def _update_experiment(self, _item_id: uuid.UUID, request: object) -> object:
        self.experiment_updates.append(request)
        return SimpleNamespace()

    async def _create_cohort_version(
        self, _item_id: uuid.UUID, request: object
    ) -> object:
        self.cohort_version_creates.append(request)
        return SimpleNamespace()


class InvalidResponseClient:
    """Simulate typed SDK response validation failing after a remote call."""

    def __init__(self) -> None:
        self.closed = 0
        self.list_calls: list[object] = []
        self.get_calls: list[uuid.UUID] = []
        self.sessions = SimpleNamespace(list=self._list_sessions, get=self._get)

    async def _list_sessions(self, params: object) -> Page[SessionResponse]:
        self.list_calls.append(params)
        return Page(items=[], next_cursor=None)

    async def _get(self, item_id: uuid.UUID) -> SessionResponse:
        self.get_calls.append(item_id)
        return SessionResponse.model_validate({"id": str(item_id)})

    async def close(self) -> None:
        self.closed += 1


class FakeClient:
    """Typed resource-shaped fake for protocol and workflow calls."""

    def __init__(self) -> None:
        self.closed = 0
        self.list_calls: list[object] = []
        self.get_calls: list[uuid.UUID] = []
        self.sessions = SimpleNamespace(list=self._list_sessions, get=self._get)
        self.agents = SimpleNamespace(list=self._list_agents)
        self.investigations = SimpleNamespace(
            list_sessions=self._list_investigation_sessions
        )

    async def _list_sessions(self, params: object) -> Page[SessionResponse]:
        self.list_calls.append(params)
        return Page(items=[_get_session()], next_cursor="opaque")

    async def _get(self, _id: uuid.UUID) -> SessionResponse:
        self.get_calls.append(_id)
        return _get_session(_id)

    async def _list_agents(self, params: object) -> Page[AgentResponse]:
        self.list_calls.append(params)
        return Page(items=[_get_agent()], next_cursor="opaque")

    async def _list_investigation_sessions(
        self, _investigation_id: uuid.UUID, params: object
    ) -> Page[InvestigationSessionResponse]:
        self.list_calls.append(params)
        return Page(items=[_get_investigation_session()], next_cursor="opaque")

    async def close(self) -> None:
        self.closed += 1


def _get_session(session_id: uuid.UUID | None = None) -> SessionResponse:
    now = datetime.now(UTC)
    return SessionResponse(
        id=session_id or uuid.uuid4(),
        owner_id=uuid.uuid4(),
        agent_id=uuid.uuid4(),
        number=1,
        origin="recorded",
        status="completed",
        inputs={},
        outputs={},
        metadata={},
        llm_call_count=0,
        tool_call_count=0,
        created=now,
        updated=now,
    )


def _get_agent() -> AgentResponse:
    now = datetime.now(UTC)
    return AgentResponse(
        id=uuid.uuid4(),
        owner_id=uuid.uuid4(),
        name="returns-resolver",
        description=None,
        latest_version=1,
        created=now,
        updated=now,
    )


def _get_investigation_session() -> InvestigationSessionResponse:
    now = datetime.now(UTC)
    return InvestigationSessionResponse(
        id=uuid.uuid4(),
        investigation_id=uuid.uuid4(),
        session_id=uuid.uuid4(),
        position=0,
        questions=[],
        verdict=None,
        created=now,
        updated=now,
    )


def _get_context(
    client: FakeClient, *, mode: CapabilityMode = CapabilityMode.READ_ONLY
) -> tuple[MCPServer[MCPServerState], Context[MCPServerState, Any]]:
    state = _get_state(client)
    server = create_server(MCPSettings(mode=mode))
    request_context = ServerRequestContext(
        session=cast(Any, None),
        lifespan_context=state,
        protocol_version="2026-07-28",
        method="tools/call",
    )
    return server, Context(request_context=request_context, mcp_server=server)


def _get_state(client: FakeClient) -> MCPServerState:
    return MCPServerState(MCPSettings(), cast(Any, client))


async def test_activity_returns_exactly_one_page_and_preserves_cursor() -> None:
    client = FakeClient()
    result = cast(
        PageData,
        await handle_activity_read(
            _get_state(client),
            ActivityListRequest(
                operation="list", kind="session", cursor="before", size=3
            ),
        ),
    )
    assert len(client.list_calls) == 1
    assert result.page.size == 3
    assert result.page.next_cursor == "opaque"
    assert result.page.has_more is True


async def test_public_sdk_serializes_non_empty_list_pages() -> None:
    """Return populated activity, registry, and review pages through MCP."""
    client = FakeClient()
    server, context = _get_context(client, mode=CapabilityMode.STANDARD)
    calls = (
        (
            "kitaru_activity_read",
            {"request": {"operation": "list", "kind": "session", "size": 3}},
        ),
        (
            "kitaru_registry_read",
            {"request": {"operation": "list", "kind": "agent", "size": 3}},
        ),
        (
            "kitaru_review_read",
            {
                "request": {
                    "operation": "list_sessions",
                    "investigation_id": str(uuid.uuid4()),
                    "size": 3,
                }
            },
        ),
    )

    for tool_name, arguments in calls:
        result = await server.call_tool(tool_name, arguments, context)

        assert isinstance(result, CallToolResult)
        assert result.is_error is False
        assert result.structured_content is not None
        assert len(result.structured_content["data"]["items"]) == 1
        assert result.structured_content["data"]["page"] == {
            "size": 3,
            "next_cursor": "opaque",
            "has_more": True,
        }


async def test_public_sdk_call_has_canonical_structured_text_parity() -> None:
    client = FakeClient()
    server, context = _get_context(client)
    item_id = uuid.uuid4()
    result = await server.call_tool(
        "kitaru_activity_read",
        {"request": {"operation": "get", "kind": "session", "id": str(item_id)}},
        context,
    )
    assert isinstance(result, CallToolResult)
    assert isinstance(result.content[0], TextContent)
    assert result.is_error is False
    assert result.structured_content is not None
    assert json.loads(result.content[0].text) == result.structured_content
    assert result.structured_content["data"]["id"] == str(item_id)


async def test_remote_response_validation_is_not_reported_as_bad_arguments() -> None:
    client = InvalidResponseClient()
    state = MCPServerState(MCPSettings(), cast(Any, client))
    server = create_server(MCPSettings())
    request_context = ServerRequestContext(
        session=cast(Any, None),
        lifespan_context=state,
        protocol_version="2026-07-28",
        method="tools/call",
    )
    context = Context(request_context=request_context, mcp_server=server)
    result = await server.call_tool(
        "kitaru_activity_read",
        {
            "request": {
                "operation": "get",
                "kind": "session",
                "id": str(uuid.uuid4()),
            }
        },
        context,
    )
    assert isinstance(result, CallToolResult)
    assert result.is_error is True
    assert result.structured_content is not None
    assert result.structured_content["error"]["code"] == "internal_error"
    assert "output validation" in result.structured_content["error"]["message"]


async def test_public_sdk_rejects_malformed_arguments_before_handler() -> None:
    client = FakeClient()
    server, context = _get_context(client)
    with pytest.raises(Exception, match="validation error"):
        await server.call_tool(
            "kitaru_activity_read",
            {"request": {"operation": "get", "kind": "session", "id": "bad"}},
            context,
        )
    assert client.get_calls == []
    assert client.list_calls == []


@pytest.mark.parametrize(
    "payload",
    [
        {
            "operation": "list_children",
            "kind": "session_nodes",
            "parent_id": str(uuid.uuid4()),
            "sort": "created:asc",
        },
        {
            "operation": "list_children",
            "kind": "experiment_run_jobs",
            "parent_id": str(uuid.uuid4()),
            "include_payloads": True,
        },
        {
            "operation": "list_children",
            "kind": "job_tasks",
            "parent_id": str(uuid.uuid4()),
            "include_payloads": True,
        },
    ],
)
async def test_public_sdk_rejects_fields_invalid_for_child_kind(
    payload: dict[str, object],
) -> None:
    client = FakeClient()
    server, context = _get_context(client)
    with pytest.raises(Exception, match="validation error"):
        await server.call_tool("kitaru_activity_read", {"request": payload}, context)
    assert client.get_calls == []
    assert client.list_calls == []


def test_management_preflight_validation() -> None:
    with pytest.raises(ValidationError, match="change at least one"):
        CohortUpdate(operation="update", cohort_id=uuid.uuid4())
    session_id = uuid.uuid4()
    with pytest.raises(ValidationError, match="unique"):
        CohortVersionCreate(
            operation="create_version",
            cohort_id=uuid.uuid4(),
            add_session_ids=[session_id, session_id],
        )
    with pytest.raises(
        ValidationError, match="cannot be null without clear_description"
    ):
        CohortUpdate(operation="update", cohort_id=uuid.uuid4(), description=None)
    CohortUpdate(
        operation="update",
        cohort_id=uuid.uuid4(),
        description=None,
        clear_description=True,
    )
    experiment_id = uuid.uuid4()
    for field in ("description", "override", "tool_policy"):
        with pytest.raises(ValidationError, match="cannot be null without clear"):
            ExperimentUpdate.model_validate(
                {"operation": "update", "experiment_id": experiment_id, field: None}
            )
        ExperimentUpdate.model_validate(
            {
                "operation": "update",
                "experiment_id": experiment_id,
                field: None,
                f"clear_{field}": True,
            }
        )


async def test_clear_flags_forward_explicit_null_update_fields() -> None:
    client = UpdateClient()
    state = MCPServerState(MCPSettings(), cast(Any, client))
    await handle_cohorts_manage(
        state,
        CohortUpdate(
            operation="update",
            cohort_id=uuid.uuid4(),
            description=None,
            clear_description=True,
        ),
    )
    await handle_experiments_manage(
        state,
        ExperimentUpdate(
            operation="update",
            experiment_id=uuid.uuid4(),
            description=None,
            clear_description=True,
            override=None,
            clear_override=True,
            tool_policy=None,
            clear_tool_policy=True,
        ),
    )
    cohort_dto = cast(Any, client.cohort_updates[0])
    experiment_dto = cast(Any, client.experiment_updates[0])
    assert cohort_dto.model_dump(exclude_unset=True) == {"description": None}
    assert experiment_dto.model_dump(exclude_unset=True) == {
        "description": None,
        "override": None,
        "tool_policy": None,
    }


async def test_cohort_version_create_forwards_optional_baseline() -> None:
    client = UpdateClient()
    state = MCPServerState(MCPSettings(), cast(Any, client))
    baseline_id = uuid.uuid4()

    await handle_cohorts_manage(
        state,
        CohortVersionCreate(
            operation="create_version",
            cohort_id=uuid.uuid4(),
            baseline_id=baseline_id,
        ),
    )
    await handle_cohorts_manage(
        state,
        CohortVersionCreate(
            operation="create_version",
            cohort_id=uuid.uuid4(),
        ),
    )

    assert cast(Any, client.cohort_version_creates[0]).baseline_id == baseline_id
    assert cast(Any, client.cohort_version_creates[1]).baseline_id is None
