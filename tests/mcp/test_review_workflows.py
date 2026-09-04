#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
"""Review, workflow-start, evaluator-management, and deletion contracts."""

import asyncio
import json
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast

import httpx
import pytest
from mcp.server import MCPServer, ServerRequestContext
from mcp.server.mcpserver import Context
from mcp.types import CallToolResult, TextContent
from pydantic import TypeAdapter, ValidationError

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.evaluator import (
    EvaluatorResponse,
    EvaluatorVersionResponse,
)
from kitaru.api_models.v1.info import AuthScheme, ServerInfoResponse
from kitaru.api_models.v1.insight import InsightResponse
from kitaru.api_models.v1.investigation import (
    InvestigationResponse,
    InvestigationSessionVerdict,
)
from kitaru.api_models.v1.job import JobResponse
from kitaru.api_models.v1.plugin import PackagePluginSource
from kitaru.api_models.v1.tag import (
    TagCreateRequest,
    TagLinkCreateRequest,
    TagLinkResponse,
    TagResourceType,
    TagResponse,
    TagUpdateRequest,
)
from kitaru.client.exceptions import APIError
from kitaru.mcp.errors import MCPToolError
from kitaru.mcp.lifecycle import MCPServerState
from kitaru.mcp.models.common import PageData
from kitaru.mcp.models.evaluators import (
    EvaluatorCreate,
    EvaluatorsManageRequest,
    EvaluatorUpdate,
    EvaluatorVersionCreate,
    EvaluatorVersionUpdate,
)
from kitaru.mcp.models.review import (
    AnnotationUpdate,
    InsightsCreate,
    InsightUpdate,
    InvestigationAnswerCreate,
    InvestigationCreate,
    InvestigationUpdate,
    ManualAnnotationCreate,
    ReviewGet,
    ReviewList,
    ReviewListSessions,
    ReviewManageRequest,
    SetInvestigationSessionVerdict,
    TagCreate,
    TagLink,
    TagUpdate,
)
from kitaru.mcp.models.workflows import (
    DeleteRequest,
    EvaluationStart,
    ExperimentRunStart,
    ResourceDelete,
    TagLinkDelete,
)
from kitaru.mcp.server import create_server
from kitaru.mcp.settings import CapabilityMode, MCPSettings
from kitaru.mcp.tools.destructive import handle_delete
from kitaru.mcp.tools.evaluators import handle_evaluators_manage
from kitaru.mcp.tools.review import handle_review_manage, handle_review_read
from kitaru.mcp.tools.workflow_start import handle_workflow_start


def _get_state(client: object, **settings: Any) -> MCPServerState:
    return MCPServerState(MCPSettings.model_validate(settings), cast(Any, client))


def _get_context(
    client: object,
    mode: CapabilityMode,
    *,
    handler_timeout: float = 120.0,
) -> tuple[MCPServer[MCPServerState], Context[MCPServerState, Any]]:
    settings = MCPSettings(mode=mode, handler_timeout=handler_timeout)
    state = MCPServerState(settings, cast(Any, client))
    server = create_server(settings)
    request_context = ServerRequestContext(
        session=cast(Any, None),
        lifespan_context=state,
        protocol_version="2026-07-28",
        method="tools/call",
    )
    return server, Context(request_context=request_context, mcp_server=server)


async def test_review_read_protocol_has_typed_structured_text_parity() -> None:
    async def list_investigations(_params: object) -> Page[Any]:
        return Page(items=[], next_cursor=None)

    client = SimpleNamespace(investigations=SimpleNamespace(list=list_investigations))
    server, context = _get_context(client, CapabilityMode.READ_ONLY)
    result = await server.call_tool(
        "kitaru_review_read",
        {"request": {"operation": "list", "kind": "investigation", "size": 4}},
        context,
    )
    assert isinstance(result, CallToolResult)
    assert result.is_error is False
    assert isinstance(result.content[0], TextContent)
    assert json.loads(result.content[0].text) == result.structured_content
    assert result.structured_content is not None
    assert result.structured_content["data"] == {
        "items": [],
        "page": {"size": 4, "next_cursor": None, "has_more": False},
    }


async def test_review_manage_protocol_rejects_unknown_verdict_before_handler() -> None:
    calls: list[object] = []

    async def update_session(*args: object) -> object:
        calls.append(args)
        return SimpleNamespace()

    client = SimpleNamespace(
        investigations=SimpleNamespace(update_session=update_session)
    )
    server, context = _get_context(client, CapabilityMode.STANDARD)
    with pytest.raises(Exception, match="validation error"):
        await server.call_tool(
            "kitaru_review_manage",
            {
                "request": {
                    "operation": "set_session_verdict",
                    "investigation_id": str(uuid.uuid4()),
                    "session_id": str(uuid.uuid4()),
                    "verdict": "pending",
                }
            },
            context,
        )
    assert calls == []


async def test_review_sessions_read_is_one_ordered_sdk_page() -> None:
    calls: list[tuple[uuid.UUID, object]] = []

    async def list_sessions(investigation_id: uuid.UUID, params: object) -> Page[Any]:
        calls.append((investigation_id, params))
        return Page(items=[], next_cursor="next")

    investigation_id = uuid.uuid4()
    client = SimpleNamespace(
        investigations=SimpleNamespace(list_sessions=list_sessions)
    )
    result = cast(
        PageData,
        await handle_review_read(
            _get_state(client),
            ReviewListSessions(
                operation="list_sessions",
                investigation_id=investigation_id,
                cursor="before",
                size=7,
            ),
        ),
    )
    assert len(calls) == 1
    assert calls[0][0] == investigation_id
    assert cast(Any, calls[0][1]).model_dump(exclude_unset=True) == {
        "cursor": "before",
        "size": 7,
    }
    assert result.page.next_cursor == "next"


@pytest.mark.parametrize("kind", ["investigation", "annotation", "insight"])
async def test_review_get_routes_to_the_selected_resource(kind: str) -> None:
    item_id = uuid.uuid4()
    calls: list[str] = []
    investigation = SimpleNamespace(id=item_id, kind="investigation")
    annotation = SimpleNamespace(id=item_id, kind="annotation")
    insight = SimpleNamespace(id=item_id, kind="insight")

    async def get_investigation(received_id: uuid.UUID) -> object:
        assert received_id == item_id
        calls.append("investigation")
        return investigation

    async def get_annotation(received_id: uuid.UUID) -> object:
        assert received_id == item_id
        calls.append("annotation")
        return annotation

    async def get_insight(received_id: uuid.UUID) -> object:
        assert received_id == item_id
        calls.append("insight")
        return insight

    client = SimpleNamespace(
        investigations=SimpleNamespace(get=get_investigation),
        annotations=SimpleNamespace(get=get_annotation),
        insights=SimpleNamespace(get=get_insight),
    )
    result = await handle_review_read(
        _get_state(client),
        ReviewGet(operation="get", kind=cast(Any, kind), id=item_id),
    )
    expected = {
        "investigation": investigation,
        "annotation": annotation,
        "insight": insight,
    }[kind]
    assert result is expected
    assert calls == [kind]


async def test_review_annotation_list_routes_to_annotations() -> None:
    calls: list[object] = []

    async def list_annotations(params: object) -> Page[Any]:
        calls.append(params)
        return Page(items=[], next_cursor=None)

    client = SimpleNamespace(
        annotations=SimpleNamespace(list=list_annotations),
    )
    result = cast(
        PageData,
        await handle_review_read(
            _get_state(client),
            ReviewList(operation="list", kind="annotation", size=3),
        ),
    )
    assert len(calls) == 1
    assert result.page.size == 3


async def test_review_insight_list_routes_to_insights() -> None:
    calls: list[object] = []

    async def list_insights(params: object) -> Page[Any]:
        calls.append(params)
        return Page(items=[], next_cursor=None)

    client = SimpleNamespace(
        insights=SimpleNamespace(list=list_insights),
    )
    result = cast(
        PageData,
        await handle_review_read(
            _get_state(client),
            ReviewList(operation="list", kind="insight", size=3),
        ),
    )
    assert len(calls) == 1
    assert result.page.size == 3


@pytest.mark.parametrize("status", ["pending", "in_progress", "completed"])
def test_review_management_accepts_every_investigation_status(status: str) -> None:
    request = InvestigationUpdate.model_validate(
        {
            "operation": "update_investigation",
            "investigation_id": uuid.uuid4(),
            "status": status,
        }
    )
    assert request.status == status


def test_review_management_rejects_noop_null_status_and_unknown_verdict() -> None:
    with pytest.raises(ValidationError, match="change at least one"):
        InvestigationUpdate(
            operation="update_investigation", investigation_id=uuid.uuid4()
        )
    with pytest.raises(ValidationError, match="status cannot be null"):
        InvestigationUpdate(
            operation="update_investigation",
            investigation_id=uuid.uuid4(),
            status=None,
        )
    adapter = TypeAdapter(ReviewManageRequest)
    with pytest.raises(ValidationError):
        adapter.validate_python(
            {
                "operation": "set_session_verdict",
                "investigation_id": uuid.uuid4(),
                "session_id": uuid.uuid4(),
                "verdict": "pending",
            }
        )


def test_insight_management_rejects_noop_null_title_and_conflicting_clear() -> None:
    insight_id = uuid.uuid4()
    with pytest.raises(ValidationError, match="change at least one"):
        InsightUpdate(operation="update_insight", insight_id=insight_id)
    with pytest.raises(ValidationError, match="title cannot be null"):
        InsightUpdate(operation="update_insight", insight_id=insight_id, title=None)
    with pytest.raises(
        ValidationError, match="cannot be null without clear_description"
    ):
        InsightUpdate(
            operation="update_insight", insight_id=insight_id, description=None
        )
    with pytest.raises(ValidationError, match="description and clear_description"):
        InsightUpdate(
            operation="update_insight",
            insight_id=insight_id,
            description="set",
            clear_description=True,
        )
    InsightUpdate(
        operation="update_insight",
        insight_id=insight_id,
        description=None,
        clear_description=True,
    )


def test_insights_create_caps_batch_size() -> None:
    with pytest.raises(ValidationError, match="at most 100"):
        InsightsCreate(
            operation="create_insights",
            agent_id=uuid.uuid4(),
            insights=[
                {"title": f"insight-{index}", "data": {"type": "text", "content": "x"}}
                for index in range(101)
            ],
        )


async def test_review_insight_creates_and_updates_forward_typed_sdk_dtos() -> None:
    create_calls: list[tuple[object, object, str | None]] = []
    update_calls: list[tuple[uuid.UUID, object]] = []
    agent_id = uuid.uuid4()
    insight_id = uuid.uuid4()

    async def create_insights(
        received_agent_id: uuid.UUID,
        insights: object,
        idempotency_key: str | None = None,
    ) -> list[object]:
        create_calls.append((received_agent_id, insights, idempotency_key))
        return [_insight(agent_id=received_agent_id)]

    async def update_insight(received_id: uuid.UUID, request: object) -> object:
        update_calls.append((received_id, request))
        return _insight(agent_id=agent_id)

    client = SimpleNamespace(
        insights=SimpleNamespace(create=create_insights, update=update_insight)
    )
    state = _get_state(client)
    await handle_review_manage(
        state,
        InsightsCreate(
            operation="create_insights",
            agent_id=agent_id,
            insights=[
                {
                    "title": "Latency regressed",
                    "data": {"type": "text", "content": "It got slower."},
                }
            ],
            idempotency_key="retry-insights-1",
        ),
    )
    await handle_review_manage(
        state,
        InsightUpdate(
            operation="update_insight",
            insight_id=insight_id,
            description=None,
            clear_description=True,
        ),
    )
    await handle_review_manage(
        state,
        InsightUpdate(
            operation="update_insight",
            insight_id=insight_id,
            title="renamed",
        ),
    )

    received_agent_id, insights, idempotency_key = create_calls[0]
    assert received_agent_id == agent_id
    assert [item.model_dump(mode="json") for item in cast(Any, insights)] == [
        {
            "title": "Latency regressed",
            "description": None,
            "data": {"type": "text", "content": "It got slower."},
            "metadata": {},
        }
    ]
    assert idempotency_key == "retry-insights-1"
    assert cast(Any, update_calls[0][1]).model_dump(exclude_unset=True) == {
        "description": None
    }
    assert cast(Any, update_calls[1][1]).model_dump(exclude_unset=True) == {
        "title": "renamed"
    }


def _insight(*, agent_id: uuid.UUID) -> InsightResponse:
    now = datetime.now(UTC)
    return InsightResponse(
        id=uuid.uuid4(),
        owner_id=uuid.uuid4(),
        agent_id=agent_id,
        title="Latency regressed",
        description=None,
        data={"type": "text", "content": "It got slower."},
        metadata={},
        created=now,
        updated=now,
    )


def test_investigation_create_preserves_empty_session_list() -> None:
    request = InvestigationCreate(
        operation="create_investigation",
        agent_id=uuid.uuid4(),
        name="empty review",
        sessions=[],
    )
    assert request.sessions == []


async def test_review_clear_and_verdict_forward_typed_sparse_dtos() -> None:
    updates: list[object] = []
    session_updates: list[object] = []

    async def update(_id: uuid.UUID, request: object) -> object:
        updates.append(request)
        return SimpleNamespace()

    async def update_session(
        _investigation_id: uuid.UUID, _session_id: uuid.UUID, request: object
    ) -> object:
        session_updates.append(request)
        return SimpleNamespace()

    client = SimpleNamespace(
        investigations=SimpleNamespace(update=update, update_session=update_session)
    )
    await handle_review_manage(
        _get_state(client),
        InvestigationUpdate(
            operation="update_investigation",
            investigation_id=uuid.uuid4(),
            description=None,
            clear_description=True,
        ),
    )
    await handle_review_manage(
        _get_state(client),
        InvestigationUpdate(
            operation="update_investigation",
            investigation_id=uuid.uuid4(),
            status="completed",
        ),
    )
    await handle_review_manage(
        _get_state(client),
        SetInvestigationSessionVerdict(
            operation="set_session_verdict",
            investigation_id=uuid.uuid4(),
            session_id=uuid.uuid4(),
            verdict=InvestigationSessionVerdict.ACCEPTABLE,
        ),
    )
    assert cast(Any, updates[0]).model_dump(exclude_unset=True) == {"description": None}
    assert cast(Any, updates[1]).model_dump(exclude_unset=True) == {
        "status": "completed"
    }
    assert cast(Any, session_updates[0]).model_dump(mode="json") == {
        "verdict": "acceptable"
    }


async def test_investigation_pending_preserves_downstream_transition_error() -> None:
    error = APIError(409, "Cannot move a completed investigation to pending.")

    async def update(_id: uuid.UUID, _request: object) -> object:
        raise error

    client = SimpleNamespace(investigations=SimpleNamespace(update=update))
    with pytest.raises(APIError) as raised:
        await handle_review_manage(
            _get_state(client),
            InvestigationUpdate(
                operation="update_investigation",
                investigation_id=uuid.uuid4(),
                status="pending",
            ),
        )

    assert raised.value is error


async def test_review_creates_and_updates_forward_typed_sdk_dtos() -> None:
    calls: list[str] = []
    investigation_requests: list[object] = []
    investigation_idempotency_keys: list[str | None] = []
    annotation_creates: list[object] = []
    annotation_idempotency_keys: list[str | None] = []
    annotation_updates: list[object] = []

    async def create_investigation(
        request: object, idempotency_key: str | None = None
    ) -> object:
        calls.append("create")
        investigation_requests.append(request)
        investigation_idempotency_keys.append(idempotency_key)
        return _investigation()

    async def get_info() -> ServerInfoResponse:
        calls.append("info")
        return ServerInfoResponse(version="0.0.0", auth_scheme=AuthScheme.LOCAL)

    async def create_annotation(
        request: object, idempotency_key: str | None = None
    ) -> object:
        annotation_creates.append(request)
        annotation_idempotency_keys.append(idempotency_key)
        return SimpleNamespace()

    async def update_annotation(_id: uuid.UUID, request: object) -> object:
        annotation_updates.append(request)
        return SimpleNamespace()

    client = SimpleNamespace(
        base_url="https://api.example.com/",
        investigations=SimpleNamespace(create=create_investigation),
        annotations=SimpleNamespace(create=create_annotation, update=update_annotation),
        info=SimpleNamespace(get=get_info),
    )
    state = _get_state(client)
    session_id = uuid.uuid4()
    investigation_session_id = uuid.uuid4()
    await handle_review_manage(
        state,
        InvestigationCreate(
            operation="create_investigation",
            agent_id=uuid.uuid4(),
            name="failure review",
            sessions=[
                {
                    "session_id": session_id,
                    "questions": [{"key": "root-cause", "question": "Was it correct?"}],
                }
            ],
            idempotency_key="retry-investigation-1",
        ),
    )
    await handle_review_manage(
        state,
        ManualAnnotationCreate(
            operation="create_annotation",
            session_id=uuid.uuid4(),
            value={"label": "bad"},
            idempotency_key="retry-annotation-1",
        ),
    )
    await handle_review_manage(
        state,
        InvestigationAnswerCreate(
            operation="answer_question",
            investigation_session_id=investigation_session_id,
            question_key="root-cause",
            value=False,
            idempotency_key="retry-answer-1",
        ),
    )
    await handle_review_manage(
        state,
        AnnotationUpdate(
            operation="update_annotation",
            annotation_id=uuid.uuid4(),
            value=True,
        ),
    )
    assert cast(Any, investigation_requests[0]).model_dump(mode="json")["sessions"] == [
        {
            "session_id": str(session_id),
            "questions": [
                {"key": "root-cause", "question": "Was it correct?", "highlights": []}
            ],
        }
    ]
    assert cast(Any, annotation_creates[0]).model_dump(mode="json")["session_id"]
    assert cast(Any, annotation_creates[1]).model_dump(mode="json")[
        "investigation_session_id"
    ] == str(investigation_session_id)
    assert (
        cast(Any, annotation_creates[1]).model_dump(mode="json")["question_key"]
        == "root-cause"
    )
    assert cast(Any, annotation_updates[0]).model_dump(mode="json") == {"value": True}
    assert calls == ["info", "create"]
    assert investigation_idempotency_keys == ["retry-investigation-1"]
    assert annotation_idempotency_keys == ["retry-annotation-1", "retry-answer-1"]


def _investigation() -> InvestigationResponse:
    now = datetime.now(UTC)
    return InvestigationResponse(
        id=uuid.uuid4(),
        owner_id=uuid.uuid4(),
        agent_id=uuid.uuid4(),
        name="failure review",
        description=None,
        status="pending",
        metadata={},
        total_sessions=1,
        completed_sessions=0,
        created=now,
        updated=now,
    )


async def _create_investigation(
    investigation: InvestigationResponse,
    get_info: Callable[[], Awaitable[ServerInfoResponse]],
    *,
    handler_timeout: float = 120.0,
) -> CallToolResult:
    async def create(
        _request: object, idempotency_key: str | None = None
    ) -> InvestigationResponse:
        del idempotency_key
        return investigation

    client = SimpleNamespace(
        base_url="https://api.example.com/",
        investigations=SimpleNamespace(create=create),
        info=SimpleNamespace(get=get_info),
    )
    server, context = _get_context(
        client,
        CapabilityMode.STANDARD,
        handler_timeout=handler_timeout,
    )
    result = await server.call_tool(
        "kitaru_review_manage",
        {
            "request": {
                "operation": "create_investigation",
                "agent_id": str(investigation.agent_id),
                "name": investigation.name,
                "sessions": [
                    {
                        "session_id": str(uuid.uuid4()),
                        "questions": [{"key": "root-cause", "question": "Why?"}],
                    }
                ],
            }
        },
        context,
    )
    assert isinstance(result, CallToolResult)
    assert result.is_error is False
    assert result.structured_content is not None
    assert result.structured_content["data"]["id"] == str(investigation.id)
    assert json.loads(cast(TextContent, result.content[0]).text) == (
        result.structured_content
    )
    return result


async def test_investigation_create_returns_dashboard_review_link() -> None:
    investigation = _investigation()

    async def get_info() -> ServerInfoResponse:
        return ServerInfoResponse(
            version="0.0.0",
            auth_scheme=AuthScheme.CONTROL_PLANE,
            dashboard_url="https://cloud.example.com/kitaru-workspaces/ws-1/",
        )

    result = await _create_investigation(investigation, get_info)

    assert result.structured_content is not None
    assert result.structured_content["links"] == {
        "review": (
            "https://cloud.example.com/kitaru-workspaces/ws-1"
            f"/agents/{investigation.agent_id}"
            f"/investigations/{investigation.id}/review"
        )
    }
    assert result.structured_content["warnings"] == []


@pytest.mark.parametrize(
    "info_error",
    [
        APIError(503, "unavailable"),
        ValueError("malformed payload"),
        httpx.ConnectError("unavailable"),
    ],
    ids=["api_error", "malformed_payload", "transport_error"],
)
async def test_investigation_create_preserves_result_when_info_fails(
    info_error: Exception,
) -> None:
    investigation = _investigation()

    async def get_info() -> ServerInfoResponse:
        raise info_error

    result = await _create_investigation(investigation, get_info)

    assert result.structured_content is not None
    assert result.structured_content["links"] == {}
    assert len(result.structured_content["warnings"]) == 1
    assert "review link" in result.structured_content["warnings"][0]


async def test_investigation_create_bounds_info_lookup_before_mutation() -> None:
    investigation = _investigation()

    async def get_info() -> ServerInfoResponse:
        await asyncio.sleep(1)
        raise AssertionError("info lookup exceeded its deadline")

    result = await _create_investigation(
        investigation,
        get_info,
        handler_timeout=0.1,
    )

    assert result.structured_content is not None
    assert result.structured_content["data"]["id"] == str(investigation.id)
    assert result.structured_content["links"] == {}
    assert len(result.structured_content["warnings"]) == 1


async def test_investigation_create_omits_link_for_api_only_server() -> None:
    investigation = _investigation()

    async def get_info() -> ServerInfoResponse:
        return ServerInfoResponse(version="0.0.0", auth_scheme=AuthScheme.LOCAL)

    result = await _create_investigation(investigation, get_info)

    assert result.structured_content is not None
    assert result.structured_content["links"] == {}
    assert result.structured_content["warnings"] == []


async def test_investigation_create_links_bundled_ui_review_page() -> None:
    investigation = _investigation()

    async def get_info() -> ServerInfoResponse:
        return ServerInfoResponse(
            version="0.0.0",
            auth_scheme=AuthScheme.LOCAL,
            ui_version="1.2.3",
        )

    result = await _create_investigation(investigation, get_info)

    assert result.structured_content is not None
    assert result.structured_content["links"] == {
        "review": (
            "https://api.example.com"
            f"/agents/{investigation.agent_id}"
            f"/investigations/{investigation.id}/review"
        )
    }


async def test_review_tag_mutations_forward_typed_sdk_dtos() -> None:
    calls: list[tuple[str, object, object | None]] = []
    now = datetime.now(UTC)
    tag_id = uuid.uuid4()
    resource_id = uuid.uuid4()
    tag = TagResponse(
        id=tag_id,
        owner_id=uuid.uuid4(),
        name="regression",
        created=now,
        updated=now,
    )
    link = TagLinkResponse(
        id=uuid.uuid4(),
        tag_id=tag_id,
        resource_type=TagResourceType.SESSION,
        resource_id=resource_id,
        created=now,
        updated=now,
    )

    async def create(
        request: TagCreateRequest, idempotency_key: str | None = None
    ) -> TagResponse:
        calls.append(("create", request, idempotency_key))
        return tag

    async def update(
        received_tag_id: uuid.UUID, request: TagUpdateRequest
    ) -> TagResponse:
        calls.append(("update", received_tag_id, request))
        return tag

    async def create_link(
        received_tag_id: uuid.UUID, request: TagLinkCreateRequest
    ) -> TagLinkResponse:
        calls.append(("link", received_tag_id, request))
        return link

    state = _get_state(
        SimpleNamespace(
            tags=SimpleNamespace(create=create, update=update, create_link=create_link)
        )
    )
    assert (
        await handle_review_manage(
            state,
            TagCreate(
                operation="create_tag",
                name="regression",
                idempotency_key="retry-tag-1",
            ),
        )
        is tag
    )
    assert (
        await handle_review_manage(
            state,
            TagUpdate(operation="update_tag", tag_id=tag_id, name="known-failure"),
        )
        is tag
    )
    assert (
        await handle_review_manage(
            state,
            TagLink(
                operation="link_tag",
                tag_id=tag_id,
                resource_type=TagResourceType.SESSION,
                resource_id=resource_id,
            ),
        )
        is link
    )

    assert isinstance(calls[0][1], TagCreateRequest)
    assert calls[0][1].model_dump() == {"name": "regression"}
    assert calls[0][2] == "retry-tag-1"
    assert calls[1][1] == tag_id
    assert isinstance(calls[1][2], TagUpdateRequest)
    assert calls[1][2].model_dump() == {"name": "known-failure"}
    assert calls[2][1] == tag_id
    assert isinstance(calls[2][2], TagLinkCreateRequest)
    assert calls[2][2].model_dump(mode="json") == {
        "resource_type": "session",
        "resource_id": str(resource_id),
    }


async def test_review_tag_create_has_typed_protocol_result() -> None:
    now = datetime.now(UTC)
    tag = TagResponse(
        id=uuid.uuid4(),
        owner_id=uuid.uuid4(),
        name="regression",
        created=now,
        updated=now,
    )

    async def create(
        request: TagCreateRequest, idempotency_key: str | None = None
    ) -> TagResponse:
        del idempotency_key
        assert isinstance(request, TagCreateRequest)
        return tag

    server, context = _get_context(
        SimpleNamespace(tags=SimpleNamespace(create=create)), CapabilityMode.STANDARD
    )
    result = await server.call_tool(
        "kitaru_review_manage",
        {"request": {"operation": "create_tag", "name": "regression"}},
        context,
    )

    assert isinstance(result, CallToolResult)
    assert result.is_error is False
    assert result.structured_content is not None
    assert result.structured_content["data"]["id"] == str(tag.id)
    assert json.loads(cast(TextContent, result.content[0]).text) == (
        result.structured_content
    )


@pytest.mark.parametrize(
    ("status_code", "code", "message"),
    [
        (409, "conflict", "conflicts with current remote state"),
        (404, "not_found", "was not found"),
    ],
)
async def test_review_tag_api_errors_use_redacted_protocol_envelope(
    status_code: int, code: str, message: str
) -> None:
    async def create(
        _request: TagCreateRequest, idempotency_key: str | None = None
    ) -> TagResponse:
        del idempotency_key
        raise APIError(status_code, "Bearer sensitive-token")

    server, context = _get_context(
        SimpleNamespace(tags=SimpleNamespace(create=create)), CapabilityMode.STANDARD
    )
    result = await server.call_tool(
        "kitaru_review_manage",
        {"request": {"operation": "create_tag", "name": "regression"}},
        context,
    )

    assert isinstance(result, CallToolResult)
    assert result.is_error is True
    assert result.structured_content is not None
    assert result.structured_content["error"]["code"] == code
    assert message in result.structured_content["error"]["message"]
    assert "sensitive-token" not in cast(TextContent, result.content[0]).text


@pytest.mark.parametrize("resource_type", list(TagResourceType))
def test_tag_link_accepts_every_public_resource_type(
    resource_type: TagResourceType,
) -> None:
    request = TypeAdapter(ReviewManageRequest).validate_python(
        {
            "operation": "link_tag",
            "tag_id": uuid.uuid4(),
            "resource_type": resource_type.value,
            "resource_id": uuid.uuid4(),
        }
    )
    assert isinstance(request, TagLink)
    assert request.resource_type is resource_type


def test_tag_mutations_reject_unknown_resource_type_before_handler() -> None:
    adapter = TypeAdapter(ReviewManageRequest)
    with pytest.raises(ValidationError):
        adapter.validate_python(
            {
                "operation": "link_tag",
                "tag_id": uuid.uuid4(),
                "resource_type": "worker",
                "resource_id": uuid.uuid4(),
            }
        )


def test_evaluation_start_caps_pairs_and_rejects_duplicates() -> None:
    evaluator_id = uuid.uuid4()
    selection = {"evaluator_id": evaluator_id, "version": 1}
    with pytest.raises(ValidationError, match="at most 100"):
        EvaluationStart(
            operation="evaluation",
            session_ids=[uuid.uuid4() for _ in range(51)],
            evaluators=[selection, {"evaluator_id": evaluator_id, "version": 2}],
        )
    session_id = uuid.uuid4()
    with pytest.raises(ValidationError, match="unique"):
        EvaluationStart(
            operation="evaluation",
            session_ids=[session_id, session_id],
            evaluators=[selection],
        )


async def test_workflow_starts_return_without_polling() -> None:
    calls: list[str] = []
    evaluation_idempotency_keys: list[str | None] = []
    experiment_start_idempotency_keys: list[str | None] = []
    evaluator_id = uuid.uuid4()

    async def get_evaluator(item_id: uuid.UUID) -> object:
        calls.append("evaluator")
        return SimpleNamespace(id=item_id, name="accuracy")

    async def get_version(item_id: uuid.UUID, version: int) -> object:
        calls.append("evaluator_version")
        return SimpleNamespace(id=uuid.uuid4(), evaluator_id=item_id, version=version)

    async def create_evaluation(
        request: object, idempotency_key: str | None = None
    ) -> object:
        del request
        calls.append("evaluation_create")
        evaluation_idempotency_keys.append(idempotency_key)
        return SimpleNamespace(model_dump=lambda **_kwargs: {"id": str(uuid.uuid4())})

    async def start_run(
        experiment_id: uuid.UUID, request: object, idempotency_key: str | None = None
    ) -> object:
        calls.append("experiment_start")
        experiment_start_idempotency_keys.append(idempotency_key)
        run_id = uuid.uuid4()
        return SimpleNamespace(
            id=run_id,
            experiment_id=experiment_id,
            cohort_version_id=cast(Any, request).cohort_version_id,
            agent_version_id=cast(Any, request).agent_version_id,
            model_dump=lambda **_kwargs: {
                "id": str(run_id),
                "experiment_id": str(experiment_id),
            },
        )

    client = SimpleNamespace(
        evaluators=SimpleNamespace(get=get_evaluator, get_version=get_version),
        evaluations=SimpleNamespace(create=create_evaluation),
        experiments=SimpleNamespace(start_run=start_run),
    )
    evaluation = cast(
        dict[str, Any],
        await handle_workflow_start(
            _get_state(client),
            EvaluationStart(
                operation="evaluation",
                session_ids=[uuid.uuid4()],
                evaluators=[{"evaluator_id": evaluator_id, "version": 2}],
                idempotency_key="retry-evaluation-1",
            ),
        ),
    )
    assert calls == ["evaluator", "evaluator_version", "evaluation_create"]
    assert evaluation["operation"] == "evaluation"
    assert evaluation_idempotency_keys == ["retry-evaluation-1"]

    experiment = cast(
        dict[str, Any],
        await handle_workflow_start(
            _get_state(client),
            ExperimentRunStart(
                operation="experiment_run",
                experiment_id=uuid.uuid4(),
                cohort_version_id=uuid.uuid4(),
                agent_version_id=uuid.uuid4(),
                idempotency_key="retry-experiment-run-1",
            ),
        ),
    )
    assert calls[-1] == "experiment_start"
    assert experiment["operation"] == "experiment_run"
    assert experiment_start_idempotency_keys == ["retry-experiment-run-1"]


async def test_experiment_run_start_rejects_mismatched_receipt() -> None:
    request = ExperimentRunStart(
        operation="experiment_run",
        experiment_id=uuid.uuid4(),
        cohort_version_id=uuid.uuid4(),
        agent_version_id=uuid.uuid4(),
    )

    async def start_run(
        _experiment_id: uuid.UUID, _request: object, idempotency_key: str | None = None
    ) -> object:
        del idempotency_key
        return SimpleNamespace(
            experiment_id=uuid.uuid4(),
            cohort_version_id=request.cohort_version_id,
            agent_version_id=request.agent_version_id,
        )

    client = SimpleNamespace(experiments=SimpleNamespace(start_run=start_run))
    with pytest.raises(MCPToolError, match="different exact resources"):
        await handle_workflow_start(_get_state(client), request)


async def test_evaluator_resolution_uses_bounded_concurrency() -> None:
    active = 0
    peak = 0
    evaluator_ids = [uuid.uuid4() for _ in range(100)]

    async def wait_for_read() -> None:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0)
        active -= 1

    async def get_evaluator(item_id: uuid.UUID) -> object:
        await wait_for_read()
        return SimpleNamespace(id=item_id, name=f"evaluator-{item_id}")

    async def get_version(item_id: uuid.UUID, version: int) -> object:
        await wait_for_read()
        return SimpleNamespace(id=uuid.uuid4(), evaluator_id=item_id, version=version)

    async def create_evaluation(
        _request: object, idempotency_key: str | None = None
    ) -> object:
        del idempotency_key
        return SimpleNamespace(model_dump=lambda **_kwargs: {"id": str(uuid.uuid4())})

    client = SimpleNamespace(
        evaluators=SimpleNamespace(get=get_evaluator, get_version=get_version),
        evaluations=SimpleNamespace(create=create_evaluation),
    )
    await handle_workflow_start(
        _get_state(client, pool_size=5),
        EvaluationStart(
            operation="evaluation",
            session_ids=[uuid.uuid4()],
            evaluators=[
                {"evaluator_id": evaluator_id, "version": 1}
                for evaluator_id in evaluator_ids
            ],
        ),
    )
    assert peak == 5


async def test_evaluator_resolution_cancels_sibling_reads_on_failure() -> None:
    evaluator_ids = [uuid.uuid4(), uuid.uuid4()]
    blocked = asyncio.Event()
    canceled: list[str] = []

    async def wait_until_canceled(label: str) -> object:
        try:
            await blocked.wait()
        except asyncio.CancelledError:
            canceled.append(label)
            raise
        raise AssertionError("blocked evaluator read unexpectedly completed")

    async def get_evaluator(item_id: uuid.UUID) -> object:
        if item_id == evaluator_ids[0]:
            raise MCPToolError("not_found", "Evaluator was not found.")
        return await wait_until_canceled("parent")

    async def get_version(item_id: uuid.UUID, _version: int) -> object:
        return await wait_until_canceled(f"version:{item_id}")

    client = SimpleNamespace(
        evaluators=SimpleNamespace(get=get_evaluator, get_version=get_version),
        evaluations=SimpleNamespace(
            create=lambda _request: pytest.fail("evaluation must not be submitted")
        ),
    )
    with pytest.raises(MCPToolError, match="not found"):
        await handle_workflow_start(
            _get_state(client, pool_size=4),
            EvaluationStart(
                operation="evaluation",
                session_ids=[uuid.uuid4()],
                evaluators=[
                    {"evaluator_id": evaluator_id, "version": 1}
                    for evaluator_id in evaluator_ids
                ],
            ),
        )
    assert sorted(canceled) == sorted(
        ["parent", *(f"version:{item_id}" for item_id in evaluator_ids)]
    )


async def test_evaluation_start_protocol_returns_typed_receipt() -> None:
    now = datetime.now(UTC)
    evaluator_id = uuid.uuid4()
    evaluator_version_id = uuid.uuid4()
    session_id = uuid.uuid4()
    job_id = uuid.uuid4()

    async def get_evaluator(_id: uuid.UUID) -> EvaluatorResponse:
        return EvaluatorResponse(
            id=evaluator_id,
            owner_id=uuid.uuid4(),
            name="accuracy",
            description=None,
            logo_url=None,
            metadata={},
            latest_version=1,
            agent_id=None,
            created=now,
            updated=now,
        )

    async def get_version(_id: uuid.UUID, _version: int) -> EvaluatorVersionResponse:
        return EvaluatorVersionResponse(
            id=evaluator_version_id,
            evaluator_id=evaluator_id,
            version=1,
            display_version=None,
            source=PackagePluginSource(
                requirement="example==1.0", entrypoint="example:evaluate"
            ),
            created=now,
            updated=now,
        )

    async def create_evaluation(
        _request: object, idempotency_key: str | None = None
    ) -> JobResponse:
        del idempotency_key
        return JobResponse(
            id=job_id,
            owner_id=uuid.uuid4(),
            kind="evaluation",
            status="pending",
            created=now,
            updated=now,
        )

    client = SimpleNamespace(
        evaluators=SimpleNamespace(get=get_evaluator, get_version=get_version),
        evaluations=SimpleNamespace(create=create_evaluation),
    )
    server, context = _get_context(client, CapabilityMode.STANDARD)
    result = await server.call_tool(
        "kitaru_workflow_start",
        {
            "request": {
                "operation": "evaluation",
                "session_ids": [str(session_id)],
                "evaluators": [{"evaluator_id": str(evaluator_id), "version": 1}],
            }
        },
        context,
    )
    assert isinstance(result, CallToolResult)
    assert result.is_error is False
    assert isinstance(result.content[0], TextContent)
    assert json.loads(result.content[0].text) == result.structured_content
    assert result.structured_content is not None
    receipt = result.structured_content["data"]
    assert receipt["input_session_ids"] == [str(session_id)]
    assert receipt["evaluators"] == [
        {
            "evaluator_id": str(evaluator_id),
            "evaluator_version_id": str(evaluator_version_id),
            "version": 1,
        }
    ]
    assert receipt["result"]["id"] == str(job_id)


def test_evaluator_version_sources_require_existing_blob_or_pinned_package() -> None:
    adapter = TypeAdapter(EvaluatorsManageRequest)
    script = EvaluatorVersionCreate(
        operation="create_version",
        evaluator_id=uuid.uuid4(),
        source={
            "type": "script",
            "blob_id": uuid.uuid4(),
            "entrypoint": "evaluate",
        },
    )
    assert script.source.type == "script"
    with pytest.raises(ValidationError, match="exactly pinned"):
        adapter.validate_python(
            {
                "operation": "create_version",
                "evaluator_id": uuid.uuid4(),
                "source": {
                    "type": "package",
                    "requirement": "example>=1",
                    "entrypoint": "example:evaluate",
                },
            }
        )


def test_evaluator_updates_require_explicit_non_conflicting_changes() -> None:
    evaluator_id = uuid.uuid4()

    with pytest.raises(ValidationError, match="change at least one"):
        EvaluatorUpdate(operation="update", evaluator_id=evaluator_id)
    with pytest.raises(ValidationError, match="metadata cannot be null"):
        EvaluatorUpdate(operation="update", evaluator_id=evaluator_id, metadata=None)
    with pytest.raises(ValidationError, match="exactly one"):
        EvaluatorVersionUpdate(
            operation="update_version", evaluator_id=evaluator_id, version=1
        )
    with pytest.raises(ValidationError, match="exactly one"):
        EvaluatorVersionUpdate(
            operation="update_version",
            evaluator_id=evaluator_id,
            version=1,
            display_version="stable",
            clear_display_version=True,
        )


async def test_evaluator_management_uses_only_typed_sdk_mutations() -> None:
    calls: list[tuple[str, object]] = []
    create_idempotency_keys: list[str | None] = []
    create_version_idempotency_keys: list[str | None] = []

    async def create(request: object, idempotency_key: str | None = None) -> object:
        calls.append(("create", request))
        create_idempotency_keys.append(idempotency_key)
        return SimpleNamespace()

    async def update(_id: uuid.UUID, request: object) -> object:
        calls.append(("update", request))
        return SimpleNamespace()

    async def create_version(
        _id: uuid.UUID, request: object, idempotency_key: str | None = None
    ) -> object:
        calls.append(("create_version", request))
        create_version_idempotency_keys.append(idempotency_key)
        return SimpleNamespace()

    async def update_version(_id: uuid.UUID, _version: int, request: object) -> object:
        calls.append(("update_version", request))
        return SimpleNamespace()

    client = SimpleNamespace(
        evaluators=SimpleNamespace(
            create=create,
            update=update,
            create_version=create_version,
            update_version=update_version,
        )
    )
    state = _get_state(client)
    await handle_evaluators_manage(
        state,
        EvaluatorCreate(
            operation="create", name="accuracy", idempotency_key="retry-evaluator-1"
        ),
    )
    await handle_evaluators_manage(
        state,
        EvaluatorUpdate(
            operation="update",
            evaluator_id=uuid.uuid4(),
            description=None,
            clear_description=True,
            metadata={"team": "evals"},
        ),
    )
    await handle_evaluators_manage(
        state,
        EvaluatorVersionCreate(
            operation="create_version",
            evaluator_id=uuid.uuid4(),
            source={
                "type": "package",
                "requirement": "example==1.2.3",
                "entrypoint": "example:evaluate",
            },
            idempotency_key="retry-evaluator-version-1",
        ),
    )
    await handle_evaluators_manage(
        state,
        EvaluatorVersionUpdate(
            operation="update_version",
            evaluator_id=uuid.uuid4(),
            version=2,
            display_version=None,
            clear_display_version=True,
        ),
    )
    assert [name for name, _ in calls] == [
        "create",
        "update",
        "create_version",
        "update_version",
    ]
    assert cast(Any, calls[1][1]).model_dump(exclude_unset=True) == {
        "description": None,
        "metadata": {"team": "evals"},
    }
    assert cast(Any, calls[2][1]).source.model_dump(mode="json") == {
        "type": "package",
        "requirement": "example==1.2.3",
        "entrypoint": "example:evaluate",
    }
    assert cast(Any, calls[3][1]).model_dump(exclude_unset=True) == {
        "display_version": None
    }
    assert create_idempotency_keys == ["retry-evaluator-1"]
    assert create_version_idempotency_keys == ["retry-evaluator-version-1"]


async def test_script_evaluator_version_requires_existing_exact_blob() -> None:
    blob_id = uuid.uuid4()
    calls: list[str] = []

    async def get_blob(item_id: uuid.UUID) -> object:
        calls.append("blob")
        return SimpleNamespace(id=item_id)

    async def create_version(
        _id: uuid.UUID, _request: object, idempotency_key: str | None = None
    ) -> object:
        calls.append("create_version")
        return SimpleNamespace()

    client = SimpleNamespace(
        blobs=SimpleNamespace(get=get_blob),
        evaluators=SimpleNamespace(create_version=create_version),
    )
    await handle_evaluators_manage(
        _get_state(client),
        EvaluatorVersionCreate(
            operation="create_version",
            evaluator_id=uuid.uuid4(),
            source={
                "type": "script",
                "blob_id": blob_id,
                "entrypoint": "evaluate",
            },
        ),
    )
    assert calls == ["blob", "create_version"]


async def test_script_evaluator_version_rejects_mismatched_blob() -> None:
    blob_id = uuid.uuid4()
    create_calls: list[object] = []

    async def get_blob(_item_id: uuid.UUID) -> object:
        return SimpleNamespace(id=uuid.uuid4())

    async def create_version(_id: uuid.UUID, request: object) -> object:
        create_calls.append(request)
        return SimpleNamespace()

    client = SimpleNamespace(
        blobs=SimpleNamespace(get=get_blob),
        evaluators=SimpleNamespace(create_version=create_version),
    )
    with pytest.raises(MCPToolError, match="different blob"):
        await handle_evaluators_manage(
            _get_state(client),
            EvaluatorVersionCreate(
                operation="create_version",
                evaluator_id=uuid.uuid4(),
                source={
                    "type": "script",
                    "blob_id": blob_id,
                    "entrypoint": "evaluate",
                },
            ),
        )
    assert create_calls == []


@pytest.mark.parametrize(
    "kind",
    [
        "cohort",
        "cohort_version",
        "experiment",
        "experiment_run",
        "insight",
        "investigation",
        "annotation",
        "evaluator",
    ],
)
async def test_existing_delete_payloads_keep_exact_resource_behavior(kind: str) -> None:
    deleted: list[tuple[str, uuid.UUID]] = []

    def delete_resource(resource: str) -> Any:
        async def delete(item_id: uuid.UUID) -> None:
            deleted.append((resource, item_id))

        return delete

    client = SimpleNamespace(
        cohorts=SimpleNamespace(delete=delete_resource("cohort")),
        cohort_versions=SimpleNamespace(delete=delete_resource("cohort_version")),
        experiments=SimpleNamespace(delete=delete_resource("experiment")),
        experiment_runs=SimpleNamespace(delete=delete_resource("experiment_run")),
        insights=SimpleNamespace(delete=delete_resource("insight")),
        investigations=SimpleNamespace(delete=delete_resource("investigation")),
        annotations=SimpleNamespace(delete=delete_resource("annotation")),
        evaluators=SimpleNamespace(delete=delete_resource("evaluator")),
    )
    item_id = uuid.uuid4()
    result = cast(
        dict[str, Any],
        await handle_delete(
            _get_state(client),
            ResourceDelete(kind=cast(Any, kind), id=item_id),
        ),
    )
    assert deleted == [(kind, item_id)]
    assert result == {"kind": kind, "id": str(item_id), "deleted": True}


async def test_delete_supports_tag_and_exact_tag_link_receipts() -> None:
    calls: list[tuple[object, ...]] = []
    tag_id = uuid.uuid4()
    resource_id = uuid.uuid4()

    async def delete(received_tag_id: uuid.UUID) -> None:
        calls.append(("tag", received_tag_id))

    async def delete_link(
        received_tag_id: uuid.UUID,
        resource_type: TagResourceType,
        received_resource_id: uuid.UUID,
    ) -> None:
        calls.append(("tag_link", received_tag_id, resource_type, received_resource_id))

    state = _get_state(
        SimpleNamespace(tags=SimpleNamespace(delete=delete, delete_link=delete_link))
    )
    deleted = await handle_delete(state, ResourceDelete(kind="tag", id=tag_id))
    unlinked = await handle_delete(
        state,
        TagLinkDelete(
            kind="tag_link",
            tag_id=tag_id,
            resource_type=TagResourceType.COHORT_VERSION,
            resource_id=resource_id,
        ),
    )

    assert calls == [
        ("tag", tag_id),
        ("tag_link", tag_id, TagResourceType.COHORT_VERSION, resource_id),
    ]
    assert deleted == {"kind": "tag", "id": str(tag_id), "deleted": True}
    assert unlinked == {
        "kind": "tag_link",
        "tag_id": str(tag_id),
        "resource_type": "cohort_version",
        "resource_id": str(resource_id),
        "deleted": True,
    }


def test_tag_link_delete_requires_exact_valid_tuple() -> None:
    adapter = TypeAdapter(DeleteRequest)
    base = {
        "kind": "tag_link",
        "tag_id": uuid.uuid4(),
        "resource_type": "session",
        "resource_id": uuid.uuid4(),
    }
    assert isinstance(adapter.validate_python(base), TagLinkDelete)
    for field in ("tag_id", "resource_type", "resource_id"):
        with pytest.raises(ValidationError):
            adapter.validate_python(
                {key: value for key, value in base.items() if key != field}
            )
    with pytest.raises(ValidationError):
        adapter.validate_python({**base, "resource_type": "worker"})
