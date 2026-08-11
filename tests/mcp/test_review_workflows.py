#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
"""Review, workflow-start, evaluator-management, and deletion contracts."""

import asyncio
import json
import uuid
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast

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
from kitaru.api_models.v1.investigation import InvestigationSessionVerdict
from kitaru.api_models.v1.job import JobResponse
from kitaru.api_models.v1.plugin import PackagePluginSource
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
    InvestigationAnswerCreate,
    InvestigationCreate,
    InvestigationUpdate,
    ManualAnnotationCreate,
    ReviewGet,
    ReviewList,
    ReviewListSessions,
    ReviewManageRequest,
    SetInvestigationSessionVerdict,
)
from kitaru.mcp.models.workflows import (
    DeleteRequest,
    EvaluationStart,
    ExperimentRunStart,
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
    client: object, mode: CapabilityMode
) -> tuple[MCPServer[MCPServerState], Context[MCPServerState, Any]]:
    settings = MCPSettings(mode=mode)
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


@pytest.mark.parametrize("kind", ["investigation", "annotation"])
async def test_review_get_routes_to_the_selected_resource(kind: str) -> None:
    item_id = uuid.uuid4()
    calls: list[str] = []
    investigation = SimpleNamespace(id=item_id, kind="investigation")
    annotation = SimpleNamespace(id=item_id, kind="annotation")

    async def get_investigation(received_id: uuid.UUID) -> object:
        assert received_id == item_id
        calls.append("investigation")
        return investigation

    async def get_annotation(received_id: uuid.UUID) -> object:
        assert received_id == item_id
        calls.append("annotation")
        return annotation

    client = SimpleNamespace(
        investigations=SimpleNamespace(get=get_investigation),
        annotations=SimpleNamespace(get=get_annotation),
    )
    result = await handle_review_read(
        _get_state(client),
        ReviewGet(operation="get", kind=cast(Any, kind), id=item_id),
    )
    assert result is (investigation if kind == "investigation" else annotation)
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


def test_review_management_rejects_noop_and_unknown_verdict() -> None:
    with pytest.raises(ValidationError, match="change at least one"):
        InvestigationUpdate(
            operation="update_investigation", investigation_id=uuid.uuid4()
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


def test_investigation_create_preserves_empty_sdk_lists() -> None:
    request = InvestigationCreate(
        operation="create_investigation",
        agent_id=uuid.uuid4(),
        name="empty review",
        questions=[],
        sessions=[],
    )
    assert request.questions == []
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


async def test_review_creates_and_updates_forward_typed_sdk_dtos() -> None:
    investigation_requests: list[object] = []
    annotation_creates: list[object] = []
    annotation_updates: list[object] = []

    async def create_investigation(request: object) -> object:
        investigation_requests.append(request)
        return SimpleNamespace()

    async def create_annotation(request: object) -> object:
        annotation_creates.append(request)
        return SimpleNamespace()

    async def update_annotation(_id: uuid.UUID, request: object) -> object:
        annotation_updates.append(request)
        return SimpleNamespace()

    client = SimpleNamespace(
        investigations=SimpleNamespace(create=create_investigation),
        annotations=SimpleNamespace(create=create_annotation, update=update_annotation),
    )
    state = _get_state(client)
    await handle_review_manage(
        state,
        InvestigationCreate(
            operation="create_investigation",
            agent_id=uuid.uuid4(),
            name="failure review",
            questions=[{"key": "correct", "question": "Was it correct?"}],
            sessions=[{"session_id": uuid.uuid4()}],
        ),
    )
    await handle_review_manage(
        state,
        ManualAnnotationCreate(
            operation="create_annotation",
            session_id=uuid.uuid4(),
            value={"label": "bad"},
        ),
    )
    await handle_review_manage(
        state,
        InvestigationAnswerCreate(
            operation="answer_question",
            investigation_session_id=uuid.uuid4(),
            question_key="correct",
            value=False,
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
    assert cast(Any, investigation_requests[0]).model_dump(mode="json")[
        "questions"
    ] == [{"key": "correct", "question": "Was it correct?"}]
    assert cast(Any, annotation_creates[0]).model_dump(mode="json")["session_id"]
    assert (
        cast(Any, annotation_creates[1]).model_dump(mode="json")["question_key"]
        == "correct"
    )
    assert cast(Any, annotation_updates[0]).model_dump(mode="json") == {"value": True}


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
    evaluator_id = uuid.uuid4()

    async def get_evaluator(item_id: uuid.UUID) -> object:
        calls.append("evaluator")
        return SimpleNamespace(id=item_id, name="accuracy")

    async def get_version(item_id: uuid.UUID, version: int) -> object:
        calls.append("evaluator_version")
        return SimpleNamespace(id=uuid.uuid4(), evaluator_id=item_id, version=version)

    async def create_evaluation(request: object) -> object:
        calls.append("evaluation_create")
        return SimpleNamespace(model_dump=lambda **_kwargs: {"id": str(uuid.uuid4())})

    async def start_run(experiment_id: uuid.UUID, request: object) -> object:
        calls.append("experiment_start")
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
            ),
        ),
    )
    assert calls == ["evaluator", "evaluator_version", "evaluation_create"]
    assert evaluation["operation"] == "evaluation"

    experiment = cast(
        dict[str, Any],
        await handle_workflow_start(
            _get_state(client),
            ExperimentRunStart(
                operation="experiment_run",
                experiment_id=uuid.uuid4(),
                cohort_version_id=uuid.uuid4(),
                agent_version_id=uuid.uuid4(),
            ),
        ),
    )
    assert calls[-1] == "experiment_start"
    assert experiment["operation"] == "experiment_run"


async def test_experiment_run_start_rejects_mismatched_receipt() -> None:
    request = ExperimentRunStart(
        operation="experiment_run",
        experiment_id=uuid.uuid4(),
        cohort_version_id=uuid.uuid4(),
        agent_version_id=uuid.uuid4(),
    )

    async def start_run(_experiment_id: uuid.UUID, _request: object) -> object:
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

    async def create_evaluation(_request: object) -> object:
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

    async def create_evaluation(_request: object) -> JobResponse:
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

    async def create(request: object) -> object:
        calls.append(("create", request))
        return SimpleNamespace()

    async def update(_id: uuid.UUID, request: object) -> object:
        calls.append(("update", request))
        return SimpleNamespace()

    async def create_version(_id: uuid.UUID, request: object) -> object:
        calls.append(("create_version", request))
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
        state, EvaluatorCreate(operation="create", name="accuracy")
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


async def test_script_evaluator_version_requires_existing_exact_blob() -> None:
    blob_id = uuid.uuid4()
    calls: list[str] = []

    async def get_blob(item_id: uuid.UUID) -> object:
        calls.append("blob")
        return SimpleNamespace(id=item_id)

    async def create_version(_id: uuid.UUID, _request: object) -> object:
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


@pytest.mark.parametrize("kind", ["investigation", "annotation", "evaluator"])
async def test_delete_supports_new_exact_resources(kind: str) -> None:
    deleted: list[tuple[str, uuid.UUID]] = []

    def delete_resource(resource: str) -> Any:
        async def delete(item_id: uuid.UUID) -> None:
            deleted.append((resource, item_id))

        return delete

    client = SimpleNamespace(
        investigations=SimpleNamespace(delete=delete_resource("investigation")),
        annotations=SimpleNamespace(delete=delete_resource("annotation")),
        evaluators=SimpleNamespace(delete=delete_resource("evaluator")),
    )
    item_id = uuid.uuid4()
    result = cast(
        dict[str, Any],
        await handle_delete(
            _get_state(client),
            DeleteRequest(kind=cast(Any, kind), id=item_id),
        ),
    )
    assert deleted == [(kind, item_id)]
    assert result == {"kind": kind, "id": str(item_id), "deleted": True}
