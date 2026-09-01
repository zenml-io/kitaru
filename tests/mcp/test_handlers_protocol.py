#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
"""Focused handler, pagination, protocol, and destructive contracts."""

import json
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, Literal, cast

import pytest
from mcp.server import MCPServer, ServerRequestContext
from mcp.server.mcpserver import Context
from mcp.types import CallToolResult, TextContent
from mcp_fakes import build_server_context
from pydantic import ValidationError

from kitaru.api_models.v1.agent import AgentResponse
from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.cohort import CohortResponse
from kitaru.api_models.v1.investigation import InvestigationSessionResponse
from kitaru.api_models.v1.session import SessionDetailResponse, TokenUsage
from kitaru.api_models.v1.session_node import SessionNodeResponse
from kitaru.api_models.v1.tag import TagResponse
from kitaru.api_models.v1.task import TaskKind
from kitaru.api_models.v1.worker import (
    WorkerClaim,
    WorkerResponse,
    WorkerRuntime,
    WorkerScope,
)
from kitaru.mcp.lifecycle import MCPServerState
from kitaru.mcp.models.activity import ActivityListRequest
from kitaru.mcp.models.common import PageData
from kitaru.mcp.models.management import (
    CohortCreate,
    CohortUpdate,
    CohortVersionCreate,
    EvaluatorSelection,
    ExperimentCreate,
    ExperimentUpdate,
)
from kitaru.mcp.models.registry import (
    RegistryGetWorkerRequest,
    RegistryListRequest,
    RegistryListVersionsRequest,
)
from kitaru.mcp.server import create_server
from kitaru.mcp.settings import CapabilityMode, MCPSettings
from kitaru.mcp.tools.activity import handle_activity_read
from kitaru.mcp.tools.cohorts import handle_cohorts_manage
from kitaru.mcp.tools.experiments import handle_experiments_manage
from kitaru.mcp.tools.registry import handle_registry_read


class UpdateClient:
    """Capture sparse update DTOs passed to typed SDK resources."""

    def __init__(self) -> None:
        self.cohort_updates: list[object] = []
        self.experiment_updates: list[object] = []
        self.cohort_version_creates: list[object] = []
        self.cohort_version_idempotency_keys: list[str | None] = []
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
        self, _item_id: uuid.UUID, request: object, idempotency_key: str | None = None
    ) -> object:
        self.cohort_version_creates.append(request)
        self.cohort_version_idempotency_keys.append(idempotency_key)
        return SimpleNamespace()


class InvalidResponseClient:
    """Simulate typed SDK response validation failing after a remote call."""

    def __init__(self) -> None:
        self.closed = 0
        self.list_calls: list[object] = []
        self.get_calls: list[uuid.UUID] = []
        self.sessions = SimpleNamespace(list=self._list_sessions, get=self._get)

    async def _list_sessions(self, params: object) -> Page[SessionDetailResponse]:
        self.list_calls.append(params)
        return Page(items=[], next_cursor=None)

    async def _get(self, item_id: uuid.UUID) -> SessionDetailResponse:
        self.get_calls.append(item_id)
        return SessionDetailResponse.model_validate({"id": str(item_id)})

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

    async def _list_sessions(self, params: object) -> Page[SessionDetailResponse]:
        self.list_calls.append(params)
        return Page(items=[_get_session()], next_cursor="opaque")

    async def _get(self, _id: uuid.UUID) -> SessionDetailResponse:
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


class RegistryClient:
    """Capture bounded tag, worker, and version registry reads."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []
        self.tag = _get_tag()
        self.worker = _get_worker()
        self.tags = SimpleNamespace(list=self._list_tags)
        self.workers = SimpleNamespace(list=self._list_workers, get=self._get_worker)
        self.agents = SimpleNamespace(
            get=self._get_agent_parent,
            list_versions=self._list_agent_versions,
        )
        self.cohorts = SimpleNamespace(
            get=self._get_cohort_parent,
            list_versions=self._list_cohort_versions,
        )

    async def _list_tags(self, params: object) -> Page[TagResponse]:
        self.calls.append(("list_tags", params))
        return Page(items=[self.tag], next_cursor="tags-next")

    async def _list_workers(self, params: object) -> Page[WorkerResponse]:
        self.calls.append(("list_workers", params))
        return Page(items=[self.worker], next_cursor="workers-next")

    async def _get_worker(self, worker_id: uuid.UUID) -> WorkerResponse:
        self.calls.append(("get_worker", worker_id))
        return self.worker.model_copy(update={"id": worker_id})

    async def _get_agent_parent(self, agent_id: uuid.UUID) -> AgentResponse:
        self.calls.append(("get_agent", agent_id))
        return _get_agent().model_copy(update={"id": agent_id})

    async def _list_agent_versions(
        self, agent_id: uuid.UUID, params: object
    ) -> Page[AgentResponse]:
        self.calls.append(("list_agent_versions", (agent_id, params)))
        return Page(items=[], next_cursor=None)

    async def _get_cohort_parent(self, cohort_id: uuid.UUID) -> CohortResponse:
        self.calls.append(("get_cohort", cohort_id))
        return _get_cohort().model_copy(update={"id": cohort_id})

    async def _list_cohort_versions(
        self, cohort_id: uuid.UUID, params: object
    ) -> Page[CohortResponse]:
        self.calls.append(("list_cohort_versions", (cohort_id, params)))
        return Page(items=[], next_cursor=None)


def _get_session(session_id: uuid.UUID | None = None) -> SessionDetailResponse:
    now = datetime.now(UTC)
    return SessionDetailResponse(
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


def _get_cohort() -> CohortResponse:
    now = datetime.now(UTC)
    return CohortResponse(
        id=uuid.uuid4(),
        owner_id=uuid.uuid4(),
        name="regression-cases",
        description=None,
        agent_id=uuid.uuid4(),
        metadata={},
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


def _get_tag() -> TagResponse:
    now = datetime.now(UTC)
    return TagResponse(
        id=uuid.uuid4(),
        owner_id=uuid.uuid4(),
        name="regression",
        created=now,
        updated=now,
    )


def _get_worker() -> WorkerResponse:
    now = datetime.now(UTC)
    return WorkerResponse(
        id=uuid.uuid4(),
        owner_id=uuid.uuid4(),
        name="worker-a",
        scope=WorkerScope(claims=[WorkerClaim(kind=TaskKind.AGENT)]),
        runtime=WorkerRuntime(platform="docker", hostname="worker-a.local"),
        last_seen_at=now,
        live=True,
        metadata={"region": "eu"},
        created=now,
        updated=now,
    )


def _get_context(
    client: FakeClient, *, mode: CapabilityMode = CapabilityMode.READ_ONLY
) -> tuple[MCPServer[MCPServerState], Context[MCPServerState, Any]]:
    return build_server_context(client, mode=mode)


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


@pytest.mark.parametrize("kind", ["tag", "worker"])
async def test_registry_tag_and_worker_lists_make_one_bounded_call(kind: str) -> None:
    client = RegistryClient()
    result = cast(
        PageData,
        await handle_registry_read(
            _get_state(cast(Any, client)),
            RegistryListRequest(
                operation="list",
                kind=kind,
                cursor="before",
                size=3,
                sort="name:asc",
                filter={"field": "name", "op": "contains", "value": "reg"},
            ),
        ),
    )

    assert len(client.calls) == 1
    call_name, params = client.calls[0]
    assert call_name == f"list_{kind}s"
    assert params.cursor == "before"
    assert params.size == 3
    assert params.sort == "name:asc"
    assert params.filter is not None
    assert params.filter.field == "name"
    assert params.filter.op == "contains"
    assert params.filter.value == "reg"
    assert result.page.size == 3
    assert result.page.has_more is True


async def test_registry_worker_get_preserves_observation_fields() -> None:
    client = RegistryClient()
    worker_id = uuid.uuid4()

    worker = await handle_registry_read(
        _get_state(cast(Any, client)),
        RegistryGetWorkerRequest(operation="get_worker", worker_id=worker_id),
    )

    assert client.calls == [("get_worker", worker_id)]
    assert isinstance(worker, WorkerResponse)
    assert worker.id == worker_id
    assert worker.scope.claims == [WorkerClaim(kind=TaskKind.AGENT)]
    assert worker.runtime.platform == "docker"
    assert worker.metadata == {"region": "eu"}
    assert worker.last_seen_at == client.worker.last_seen_at
    assert worker.live is True


@pytest.mark.parametrize("kind", ["agent", "cohort"])
async def test_registry_version_filter_is_forwarded_only_for_supported_kinds(
    kind: Literal["agent", "cohort"],
) -> None:
    client = RegistryClient()
    parent_id = uuid.uuid4()
    request = RegistryListVersionsRequest(
        operation="list_versions",
        kind=kind,
        parent_reference=str(parent_id),
        filter={"field": "tags", "op": "contains", "value": "regression"},
    )

    await handle_registry_read(_get_state(cast(Any, client)), request)

    _, (_, params) = client.calls[-1]
    assert params.filter == request.filter
    with pytest.raises(ValidationError, match="filter"):
        RegistryListVersionsRequest(
            operation="list_versions",
            kind="importer",
            parent_reference="importer-name",
            filter=request.filter,
        )


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


async def test_public_worker_get_has_canonical_structured_text_parity() -> None:
    client = RegistryClient()
    server, context = _get_context(cast(Any, client))
    worker_id = uuid.uuid4()
    result = await server.call_tool(
        "kitaru_registry_read",
        {
            "request": {
                "operation": "get_worker",
                "worker_id": str(worker_id),
            }
        },
        context,
    )

    assert isinstance(result, CallToolResult)
    assert isinstance(result.content[0], TextContent)
    assert result.is_error is False
    assert result.structured_content is not None
    assert json.loads(result.content[0].text) == result.structured_content
    data = result.structured_content["data"]
    assert data["id"] == str(worker_id)
    assert data["scope"]["claims"] == [{"kind": "agent", "agent_version_id": None}]
    assert data["runtime"]["platform"] == "docker"
    assert data["metadata"] == {"region": "eu"}
    assert datetime.fromisoformat(data["last_seen_at"]) == client.worker.last_seen_at
    assert data["live"] is True


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


async def test_cohort_version_create_forwards_idempotency_key() -> None:
    client = UpdateClient()
    state = MCPServerState(MCPSettings(), cast(Any, client))

    await handle_cohorts_manage(
        state,
        CohortVersionCreate(
            operation="create_version",
            cohort_id=uuid.uuid4(),
            idempotency_key="retry-cohort-version-1",
        ),
    )

    assert client.cohort_version_idempotency_keys == ["retry-cohort-version-1"]


async def test_cohort_create_forwards_idempotency_key() -> None:
    idempotency_keys: list[str | None] = []

    async def create(_request: object, idempotency_key: str | None = None) -> object:
        idempotency_keys.append(idempotency_key)
        return SimpleNamespace()

    client = SimpleNamespace(cohorts=SimpleNamespace(create=create))
    state = MCPServerState(MCPSettings(), cast(Any, client))

    await handle_cohorts_manage(
        state,
        CohortCreate(
            operation="create",
            agent_id=uuid.uuid4(),
            name="regression-cases",
            idempotency_key="retry-cohort-1",
        ),
    )

    assert idempotency_keys == ["retry-cohort-1"]


async def test_experiment_create_forwards_idempotency_key() -> None:
    evaluator_id = uuid.uuid4()
    idempotency_keys: list[str | None] = []

    async def get_evaluator(item_id: uuid.UUID) -> object:
        return SimpleNamespace(id=item_id, name="accuracy")

    async def get_version(item_id: uuid.UUID, version: int) -> object:
        return SimpleNamespace(id=uuid.uuid4(), evaluator_id=item_id, version=version)

    async def create(_request: object, idempotency_key: str | None = None) -> object:
        idempotency_keys.append(idempotency_key)
        return SimpleNamespace()

    client = SimpleNamespace(
        evaluators=SimpleNamespace(get=get_evaluator, get_version=get_version),
        experiments=SimpleNamespace(create=create),
    )
    state = MCPServerState(MCPSettings(), cast(Any, client))

    await handle_experiments_manage(
        state,
        ExperimentCreate(
            operation="create",
            agent_id=uuid.uuid4(),
            name="baseline-comparison",
            evaluators=[EvaluatorSelection(evaluator_id=evaluator_id, version=1)],
            idempotency_key="retry-experiment-1",
        ),
    )

    assert idempotency_keys == ["retry-experiment-1"]


@pytest.mark.parametrize("kind", ["session", "session_nodes"])
@pytest.mark.parametrize(
    "usage", [None, TokenUsage(input_tokens=12, output_tokens=4, cached_input_tokens=2)]
)
async def test_public_activity_preserves_typed_token_usage(
    kind: str, usage: TokenUsage | None
) -> None:
    client = FakeClient()
    session_id = uuid.uuid4()
    session = _get_session(session_id).model_copy(
        update={"tokens": usage, "cost": Decimal("0.1250")}
    )
    node = SessionNodeResponse(
        id=uuid.uuid4(),
        session_id=session_id,
        index=0,
        parent_index=None,
        secondary_parent_indexes=[],
        secondary_parent_ids=[],
        node_type="llm_call",
        name="model",
        status="completed",
        tokens=usage,
        cost=Decimal("0.2500"),
        metadata={},
    )

    async def get_session(_item_id: uuid.UUID) -> SessionDetailResponse:
        return session

    async def list_nodes(
        _session_id: uuid.UUID, _params: object
    ) -> Page[SessionNodeResponse]:
        return Page(items=[node], next_cursor=None)

    client.sessions.get = get_session
    client.sessions.list_nodes = list_nodes
    server, context = _get_context(client)
    request = (
        {"operation": "get", "kind": kind, "id": str(session_id)}
        if kind == "session"
        else {"operation": "list_children", "kind": kind, "parent_id": str(session_id)}
    )
    result = await server.call_tool(
        "kitaru_activity_read", {"request": request}, context
    )

    assert isinstance(result, CallToolResult)
    assert result.is_error is False
    assert isinstance(result.content[0], TextContent)
    assert result.structured_content is not None
    assert json.loads(result.content[0].text) == result.structured_content
    data = result.structured_content["data"]
    item = data if kind == "session" else data["items"][0]
    assert item["tokens"] == (usage.model_dump() if usage is not None else None)
    assert item["cost"] == ("0.1250" if kind == "session" else "0.2500")
