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

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.job import JobResponse
from kitaru.api_models.v1.session import SessionResponse
from kitaru.mcp.lifecycle import MCPConnection, MCPServerState
from kitaru.mcp.models.activity import ActivityListRequest
from kitaru.mcp.models.common import PageData
from kitaru.mcp.models.management import (
    CohortUpdate,
    CohortVersionCreate,
    ExperimentUpdate,
)
from kitaru.mcp.models.workflows import (
    ExperimentRunStart,
    ReplayStart,
    SessionEvaluationStart,
    SessionRunStart,
)
from kitaru.mcp.server import create_server
from kitaru.mcp.settings import CapabilityMode, MCPSettings
from kitaru.mcp.tools.activity import handle_activity_read
from kitaru.mcp.tools.cohorts import handle_cohorts_manage
from kitaru.mcp.tools.experiments import handle_experiments_manage
from kitaru.mcp.tools.workflows import handle_workflow_start


class UpdateClient:
    """Capture sparse update DTOs passed to typed SDK resources."""

    def __init__(self) -> None:
        self.cohort_updates: list[object] = []
        self.experiment_updates: list[object] = []
        self.cohorts = SimpleNamespace(update=self._update_cohort)
        self.experiments = SimpleNamespace(update=self._update_experiment)

    async def _update_cohort(self, _item_id: uuid.UUID, request: object) -> object:
        self.cohort_updates.append(request)
        return SimpleNamespace()

    async def _update_experiment(self, _item_id: uuid.UUID, request: object) -> object:
        self.experiment_updates.append(request)
        return SimpleNamespace()


class InvalidResponseClient:
    """Simulate typed SDK response validation failing after a remote call."""

    def __init__(self) -> None:
        self.closed = 0
        self.list_calls: list[object] = []
        self.get_calls: list[uuid.UUID] = []
        self.create_calls: list[tuple[object, str | None]] = []
        self.features: list[str] = []
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

    def __init__(self, *, features: list[str] | None = None) -> None:
        self.closed = 0
        self.list_calls: list[object] = []
        self.get_calls: list[uuid.UUID] = []
        self.create_calls: list[tuple[object, str | None]] = []
        self.features = features or []
        self.sessions = SimpleNamespace(list=self._list_sessions, get=self._get)
        self.session_runs = SimpleNamespace(create=self._create_session_run)
        self.info = SimpleNamespace(get=self._get_info)

    async def _list_sessions(self, params: object) -> Page[SessionResponse]:
        self.list_calls.append(params)
        return Page(items=[_get_session()], next_cursor="opaque")

    async def _get(self, _id: uuid.UUID) -> SessionResponse:
        self.get_calls.append(_id)
        return _get_session(_id)

    async def _create_session_run(
        self, request: object, *, idempotency_key: str | None = None
    ) -> JobResponse:
        self.create_calls.append((request, idempotency_key))
        now = datetime.now(UTC)
        return JobResponse(
            id=uuid.uuid4(),
            owner_id=uuid.uuid4(),
            status="pending",
            created=now,
            updated=now,
        )

    async def _get_info(self) -> object:
        return SimpleNamespace(features=self.features)

    async def close(self) -> None:
        self.closed += 1


def _get_session(session_id: uuid.UUID | None = None) -> SessionResponse:
    now = datetime.now(UTC)
    return SessionResponse(
        id=session_id or uuid.uuid4(),
        owner_id=uuid.uuid4(),
        agent_id=uuid.uuid4(),
        origin="recorded",
        status="completed",
        inputs={},
        outputs={},
        expected=None,
        metadata={},
        llm_call_count=0,
        tool_call_count=0,
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
    return MCPServerState(
        MCPSettings(), cast(MCPConnection, object()), cast(Any, client)
    )


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
    state = MCPServerState(
        MCPSettings(), cast(MCPConnection, object()), cast(Any, client)
    )
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


def test_management_and_workflow_preflight_validation() -> None:
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
    with pytest.raises(ValidationError, match="100 session/evaluator pairs"):
        SessionEvaluationStart(
            operation="session_evaluation",
            request_id="request",
            session_ids=[uuid.uuid4() for _ in range(11)],
            evaluators=[
                {"evaluator_id": uuid.uuid4(), "version": 1} for _ in range(10)
            ],
        )


async def test_clear_flags_forward_explicit_null_update_fields() -> None:
    client = UpdateClient()
    state = MCPServerState(
        MCPSettings(), cast(MCPConnection, object()), cast(Any, client)
    )
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


@pytest.mark.parametrize("request_id", ["has space", "tab\tvalue", "é", "line\nvalue"])
def test_protected_request_ids_reject_non_header_values(request_id: str) -> None:
    common = {"request_id": request_id}
    with pytest.raises(ValidationError):
        SessionRunStart(
            operation="session_run",
            agent_version_id=uuid.uuid4(),
            inputs={},
            **common,
        )
    with pytest.raises(ValidationError):
        ReplayStart(
            operation="replay",
            baseline_session_id=uuid.uuid4(),
            evaluators=[{"evaluator_id": uuid.uuid4(), "version": 1}],
            **common,
        )
    with pytest.raises(ValidationError):
        SessionEvaluationStart(
            operation="session_evaluation",
            session_ids=[uuid.uuid4()],
            evaluators=[{"evaluator_id": uuid.uuid4(), "version": 1}],
            **common,
        )
    with pytest.raises(ValidationError):
        ExperimentRunStart(
            operation="experiment_run",
            experiment_id=uuid.uuid4(),
            cohort_version_id=uuid.uuid4(),
            agent_version_id=uuid.uuid4(),
            **common,
        )


async def test_invalid_request_id_never_reaches_remote_client() -> None:
    client = FakeClient(features=["idempotency.v1"])
    server, context = _get_context(client, mode=CapabilityMode.STANDARD)
    with pytest.raises(Exception, match="validation error"):
        await server.call_tool(
            "kitaru_workflow_start",
            {
                "request": {
                    "operation": "session_run",
                    "request_id": "invalid value",
                    "agent_version_id": str(uuid.uuid4()),
                    "inputs": {},
                }
            },
            context,
        )
    assert client.create_calls == []


async def test_protected_workflow_discovers_capability_and_forwards_request_id() -> (
    None
):
    request = SessionRunStart(
        operation="session_run",
        request_id="stable-request",
        agent_version_id=uuid.uuid4(),
        inputs={"prompt": "hello"},
    )
    unsupported = FakeClient()
    with pytest.raises(Exception, match="does not advertise"):
        await handle_workflow_start(_get_state(unsupported), request)
    assert unsupported.create_calls == []

    supported = FakeClient(features=["idempotency.v1"])
    result = cast(
        dict[str, Any], await handle_workflow_start(_get_state(supported), request)
    )
    assert len(supported.create_calls) == 1
    assert supported.create_calls[0][1] == "stable-request"
    assert result["request_id"] == "stable-request"
