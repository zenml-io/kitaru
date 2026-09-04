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
"""Tests for session node use cases."""

import uuid
from decimal import Decimal
from typing import Any

import pytest

from conftest import (
    FakeAgentRepository,
    FakeAgentVersionRepository,
    FakeBlobDataStore,
    FakeBlobRepository,
    FakeImportRepository,
    FakeReplayRepository,
    FakeSessionNodeRepository,
    FakeSessionRepository,
    FakeTaskRepository,
    build_payload_store,
    create_session,
)
from kitaru.api_models.v1.session import SessionOrigin, SessionStatus, TokenUsage
from kitaru.api_models.v1.session_node import NodeStatus, NodeType
from kitaru.cache_keys import compute_tool_cache_key
from kitaru.server.application.models.auth import (
    AuthContext,
    GrantKind,
    TaskPrincipal,
)
from kitaru.server.application.models.session import SessionUpdate
from kitaru.server.application.models.session_node import (
    SessionNodeFilter,
    SessionNodeUpsert,
)
from kitaru.server.application.payload_store import PayloadStore
from kitaru.server.application.services.session_node_service import (
    SessionNodeService,
)
from kitaru.server.application.services.session_service import SessionService
from kitaru.server.domain.account import Account
from kitaru.server.domain.blob import BlobStorageBackend
from kitaru.server.domain.payload import PayloadMediaType
from kitaru.server.domain.session import SessionAccessDenied
from kitaru.server.domain.session_node import SessionNodeParentNotFound
from kitaru.server.domain.task import AgentTask

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="ann"))


@pytest.fixture
def session_repository() -> FakeSessionRepository:
    """Provide a fake session repository."""
    return FakeSessionRepository()


@pytest.fixture
def node_repository() -> FakeSessionNodeRepository:
    """Provide a fake session node repository."""
    return FakeSessionNodeRepository()


@pytest.fixture
def task_repository() -> FakeTaskRepository:
    """Provide a fake task repository."""
    return FakeTaskRepository()


@pytest.fixture
def payload_store() -> PayloadStore:
    """Provide a payload store backed by fresh fake blob storage."""
    return build_payload_store().store


@pytest.fixture
def service(
    node_repository: FakeSessionNodeRepository,
    session_repository: FakeSessionRepository,
    task_repository: FakeTaskRepository,
    payload_store: PayloadStore,
) -> SessionNodeService:
    """Provide a session node service backed by the fake repositories."""
    return SessionNodeService(
        repository=node_repository,
        session_repository=session_repository,
        task_repository=task_repository,
        payload_store=payload_store,
    )


@pytest.fixture
def session_service(
    session_repository: FakeSessionRepository, payload_store: PayloadStore
) -> SessionService:
    """Provide a session service sharing the fake session repository."""
    return SessionService(
        repository=session_repository,
        task_repository=FakeTaskRepository(),
        agent_version_repository=FakeAgentVersionRepository(FakeAgentRepository()),
        replay_repository=FakeReplayRepository(),
        import_repository=FakeImportRepository(),
        payload_store=payload_store,
    )


@pytest.fixture
async def session_id(session_repository: FakeSessionRepository) -> uuid.UUID:
    """Provide the id of an in-progress recorded session."""
    session = await create_session(
        session_repository,
        ACTOR.account.id,
        agent_id=uuid.uuid4(),
        origin=SessionOrigin.RECORDED,
        status=SessionStatus.IN_PROGRESS,
    )
    return session.id


def _llm_node(index: int, **overrides: Any) -> SessionNodeUpsert:
    values: dict[str, Any] = {
        "index": index,
        "node_type": NodeType.LLM_CALL,
        "name": "call",
        "status": NodeStatus.COMPLETED,
    }
    values.update(overrides)
    return SessionNodeUpsert(**values)


async def test_ingest_insert_assigns_ids_and_rollups(
    service: SessionNodeService,
    session_repository: FakeSessionRepository,
    session_id: uuid.UUID,
) -> None:
    """Insert new nodes and roll up their cost, tokens, and call counts."""
    batch = [
        _llm_node(0, cost=Decimal("1.50"), tokens=TokenUsage(input_tokens=10)),
        SessionNodeUpsert(
            index=1,
            parent_index=0,
            node_type=NodeType.TOOL_CALL,
            name="search",
            status=NodeStatus.COMPLETED,
            tool_name="search",
            inputs={"q": "hi"},
        ),
    ]
    stored = await service.ingest_nodes(session_id, batch, actor=ACTOR)

    assert stored[0].parent_id is None
    assert stored[1].parent_id == stored[0].id
    assert stored[1].cache_key is not None

    session = await session_repository.get(session_id, include_payloads=True)
    assert session.cost == Decimal("1.50")
    assert session.tokens is not None
    assert session.tokens.input_tokens == 10
    assert session.llm_call_count == 1
    assert session.tool_call_count == 1


async def test_ingest_cache_key_null_when_tool_name_missing(
    service: SessionNodeService, session_id: uuid.UUID
) -> None:
    """Leave cache_key null on a tool call node without a tool name."""
    batch = [
        SessionNodeUpsert(
            index=0,
            node_type=NodeType.TOOL_CALL,
            name="unknown-tool",
            status=NodeStatus.COMPLETED,
        )
    ]
    stored = await service.ingest_nodes(session_id, batch, actor=ACTOR)
    assert stored[0].cache_key is None


async def test_ingest_cache_key_null_when_inputs_missing(
    service: SessionNodeService, session_id: uuid.UUID
) -> None:
    """Leave cache_key null on a tool call node without recorded inputs."""
    batch = [
        SessionNodeUpsert(
            index=0,
            node_type=NodeType.TOOL_CALL,
            name="search",
            status=NodeStatus.COMPLETED,
            tool_name="search",
        )
    ]
    stored = await service.ingest_nodes(session_id, batch, actor=ACTOR)
    assert stored[0].cache_key is None


async def test_ingest_secondary_parents_resolve(
    service: SessionNodeService, session_id: uuid.UUID
) -> None:
    """Resolve secondary_parent_indexes into secondary_parent_ids."""
    batch = [
        _llm_node(0),
        _llm_node(1),
        SessionNodeUpsert(
            index=2,
            parent_index=0,
            secondary_parent_indexes=[1],
            node_type=NodeType.SUBAGENT_CALL,
            name="merge",
            status=NodeStatus.COMPLETED,
        ),
    ]
    stored = await service.ingest_nodes(session_id, batch, actor=ACTOR)
    assert stored[2].parent_id == stored[0].id
    assert stored[2].secondary_parent_ids == [stored[1].id]


async def test_ingest_parent_resolves_against_stored_row(
    service: SessionNodeService, session_id: uuid.UUID
) -> None:
    """Resolve a parent_index against a row stored in an earlier batch."""
    first = await service.ingest_nodes(session_id, [_llm_node(0)], actor=ACTOR)
    second = await service.ingest_nodes(
        session_id,
        [
            SessionNodeUpsert(
                index=1,
                parent_index=0,
                node_type=NodeType.TOOL_CALL,
                name="search",
                status=NodeStatus.COMPLETED,
            )
        ],
        actor=ACTOR,
    )
    assert second[0].parent_id == first[0].id


async def test_ingest_unresolved_parent_index_raises(
    service: SessionNodeService, session_id: uuid.UUID
) -> None:
    """Raise when a parent_index matches no stored or batched node."""
    batch = [
        SessionNodeUpsert(
            index=1,
            parent_index=0,
            node_type=NodeType.TOOL_CALL,
            name="search",
            status=NodeStatus.COMPLETED,
        )
    ]
    with pytest.raises(SessionNodeParentNotFound):
        await service.ingest_nodes(session_id, batch, actor=ACTOR)


async def test_ingest_replace_clears_omitted_fields(
    service: SessionNodeService, session_id: uuid.UUID
) -> None:
    """Replace a node whole, clearing fields the resent version omits."""
    created = await service.ingest_nodes(
        session_id,
        [_llm_node(0, error="boom", requested_model="gpt", tool_name="unused")],
        actor=ACTOR,
    )
    replaced = await service.ingest_nodes(session_id, [_llm_node(0)], actor=ACTOR)
    assert replaced[0].id == created[0].id
    assert replaced[0].error is None
    assert replaced[0].requested_model is None
    assert replaced[0].tool_name is None


async def test_ingest_replace_preserves_id(
    service: SessionNodeService, session_id: uuid.UUID
) -> None:
    """Preserve the row id when replacing an already-stored index."""
    created = await service.ingest_nodes(session_id, [_llm_node(0)], actor=ACTOR)
    replaced = await service.ingest_nodes(
        session_id, [_llm_node(0, name="renamed")], actor=ACTOR
    )
    assert replaced[0].id == created[0].id
    assert replaced[0].name == "renamed"


async def test_ingest_replace_updates_rollup_delta(
    service: SessionNodeService,
    session_repository: FakeSessionRepository,
    session_id: uuid.UUID,
) -> None:
    """Correct the session rollup when a replace changes cost and tokens."""
    await service.ingest_nodes(
        session_id,
        [_llm_node(0, cost=Decimal("1.00"), tokens=TokenUsage(input_tokens=10))],
        actor=ACTOR,
    )
    await service.ingest_nodes(
        session_id,
        [_llm_node(0, cost=Decimal("4.00"), tokens=TokenUsage(input_tokens=30))],
        actor=ACTOR,
    )
    session = await session_repository.get(session_id, include_payloads=True)
    assert session.cost == Decimal("4.00")
    assert session.tokens is not None
    assert session.tokens.input_tokens == 30
    assert session.llm_call_count == 1


async def test_ingest_replace_changing_node_type_updates_call_counts(
    service: SessionNodeService,
    session_repository: FakeSessionRepository,
    session_id: uuid.UUID,
) -> None:
    """Move the call count from one kind to another when the type changes."""
    await service.ingest_nodes(session_id, [_llm_node(0)], actor=ACTOR)
    await service.ingest_nodes(
        session_id,
        [
            SessionNodeUpsert(
                index=0,
                node_type=NodeType.SPAN,
                name="span",
                status=NodeStatus.COMPLETED,
            )
        ],
        actor=ACTOR,
    )
    session = await session_repository.get(session_id, include_payloads=True)
    assert session.llm_call_count == 0
    assert session.tool_call_count == 0


async def test_ingest_retry_identical_batch_nets_zero_delta(
    service: SessionNodeService,
    session_repository: FakeSessionRepository,
    session_id: uuid.UUID,
) -> None:
    """Net a zero rollup delta when an identical batch is retried."""
    batch = [_llm_node(0, cost=Decimal("2.00"), tokens=TokenUsage(input_tokens=5))]
    await service.ingest_nodes(session_id, batch, actor=ACTOR)
    before = await session_repository.get(session_id, include_payloads=True)
    await service.ingest_nodes(session_id, batch, actor=ACTOR)
    after = await session_repository.get(session_id, include_payloads=True)
    assert after.cost == before.cost
    assert after.tokens == before.tokens
    assert after.llm_call_count == before.llm_call_count


async def test_ingest_into_terminal_imported_session_allowed(
    service: SessionNodeService,
    session_service: SessionService,
    session_repository: FakeSessionRepository,
) -> None:
    """Allow node ingest into an imported session created already terminal."""
    session = await create_session(
        session_repository,
        ACTOR.account.id,
        agent_id=uuid.uuid4(),
        origin=SessionOrigin.IMPORTED,
        status=SessionStatus.COMPLETED,
    )
    stored = await service.ingest_nodes(session.id, [_llm_node(0)], actor=ACTOR)
    assert len(stored) == 1


async def test_ingest_into_terminal_import_sourced_session_allowed(
    service: SessionNodeService,
    session_service: SessionService,
    session_repository: FakeSessionRepository,
) -> None:
    """Allow node ingest into a finished session naming an import source."""
    session = await create_session(
        session_repository,
        ACTOR.account.id,
        agent_id=uuid.uuid4(),
        origin=SessionOrigin.REPLAY,
        status=SessionStatus.COMPLETED,
        imported_from="langfuse",
    )
    stored = await service.ingest_nodes(session.id, [_llm_node(0)], actor=ACTOR)
    assert len(stored) == 1


async def test_ingest_into_terminal_recorded_session_rejected(
    service: SessionNodeService,
    session_service: SessionService,
    session_id: uuid.UUID,
) -> None:
    """Reject node ingest into a terminal recorded session."""
    await session_service.update_session(
        session_id, SessionUpdate(status=SessionStatus.COMPLETED), actor=ACTOR
    )
    with pytest.raises(Exception, match="does not accept node ingestion"):
        await service.ingest_nodes(session_id, [_llm_node(0)], actor=ACTOR)


async def test_list_nodes_include_payloads_true(
    service: SessionNodeService, session_id: uuid.UUID
) -> None:
    """Populate inputs, outputs, and attributes when include_payloads is set."""
    await service.ingest_nodes(
        session_id,
        [_llm_node(0, inputs={"q": "hi"}, outputs={"a": "there"}, attributes={"k": 1})],
        actor=ACTOR,
    )
    nodes, next_cursor = await service.list_nodes(
        SessionNodeFilter(session_id=session_id, include_payloads=True), actor=ACTOR
    )
    assert next_cursor is None
    assert nodes[0].inputs is not None
    assert nodes[0].inputs.value == {"q": "hi"}
    assert nodes[0].outputs is not None
    assert nodes[0].outputs.value == {"a": "there"}
    assert nodes[0].attributes is not None
    assert nodes[0].attributes.value == {"k": 1}


async def test_list_nodes_include_payloads_false(
    service: SessionNodeService, session_id: uuid.UUID
) -> None:
    """Null inputs, outputs, and attributes when include_payloads is unset."""
    await service.ingest_nodes(
        session_id,
        [_llm_node(0, inputs={"q": "hi"}, outputs={"a": "there"}, attributes={"k": 1})],
        actor=ACTOR,
    )
    nodes, _ = await service.list_nodes(
        SessionNodeFilter(session_id=session_id, include_payloads=False), actor=ACTOR
    )
    assert nodes[0].inputs is None
    assert nodes[0].outputs is None
    assert nodes[0].attributes is None


async def test_list_nodes_ordered_by_index_with_pagination(
    service: SessionNodeService, session_id: uuid.UUID
) -> None:
    """Page through nodes in index-ascending order via next_cursor."""
    batch = [_llm_node(index) for index in (2, 0, 1, 4, 3)]
    await service.ingest_nodes(session_id, batch, actor=ACTOR)

    collected: list[int] = []
    cursor = None
    while True:
        nodes, next_cursor = await service.list_nodes(
            SessionNodeFilter(session_id=session_id, cursor=cursor, size=2),
            actor=ACTOR,
        )
        collected.extend(node.index for node in nodes)
        if next_cursor is None:
            break
        cursor = next_cursor

    assert collected == [0, 1, 2, 3, 4]


async def test_ingest_empty_batch_is_a_no_op(
    service: SessionNodeService, session_id: uuid.UUID
) -> None:
    """Return an empty list for an empty batch without touching rollups."""
    stored = await service.ingest_nodes(session_id, [], actor=ACTOR)
    assert stored == []


def _task_principal(
    task_id: uuid.UUID, granted_session_id: uuid.UUID | None = None
) -> AuthContext:
    """Build an auth context for a task principal owning the given task."""
    grants: dict[GrantKind, frozenset[uuid.UUID]] = {}
    if granted_session_id is not None:
        grants[GrantKind.SESSION] = frozenset({granted_session_id})
    return AuthContext(
        account=Account(id=uuid.uuid4(), name="job-owner"),
        principal=TaskPrincipal(
            task_id=task_id,
            attempt=1,
            worker_id=uuid.uuid4(),
            job_id=uuid.uuid4(),
            grants=grants,
        ),
    )


async def test_ingest_nodes_denies_a_task_principal_for_another_tasks_session(
    service: SessionNodeService, session_repository: FakeSessionRepository
) -> None:
    """Reject a task principal ingesting nodes into a session it does not own."""
    session = await create_session(
        session_repository,
        uuid.uuid4(),
        agent_id=uuid.uuid4(),
        task_id=uuid.uuid4(),
        status=SessionStatus.IN_PROGRESS,
    )
    actor = _task_principal(uuid.uuid4())
    with pytest.raises(SessionAccessDenied):
        await service.ingest_nodes(session.id, [_llm_node(0)], actor=actor)


async def test_ingest_nodes_denies_a_task_principal_for_its_input_session(
    service: SessionNodeService, session_repository: FakeSessionRepository
) -> None:
    """Reject a task principal writing nodes into its read-only input session."""
    session = await create_session(
        session_repository,
        uuid.uuid4(),
        agent_id=uuid.uuid4(),
        task_id=uuid.uuid4(),
        status=SessionStatus.IN_PROGRESS,
    )
    actor = _task_principal(uuid.uuid4(), granted_session_id=session.id)
    with pytest.raises(SessionAccessDenied):
        await service.ingest_nodes(session.id, [_llm_node(0)], actor=actor)


async def test_ingest_nodes_allows_a_task_principal_for_its_own_session(
    service: SessionNodeService,
    session_repository: FakeSessionRepository,
    task_repository: FakeTaskRepository,
) -> None:
    """Allow a task principal to ingest nodes into the session it owns."""
    task = await task_repository.create(
        AgentTask(job_id=uuid.uuid4(), agent_version_id=uuid.uuid4(), attempt=1)
    )
    task_id = task.id
    session = await create_session(
        session_repository,
        uuid.uuid4(),
        agent_id=uuid.uuid4(),
        task_id=task_id,
        status=SessionStatus.IN_PROGRESS,
    )
    actor = _task_principal(task_id)
    stored = await service.ingest_nodes(session.id, [_llm_node(0)], actor=actor)
    assert len(stored) == 1


async def test_get_indexes_by_ids_denies_a_task_principal_for_another_tasks_session(
    service: SessionNodeService, session_repository: FakeSessionRepository
) -> None:
    """Reject a task principal reading the index of a session it does not own."""
    session = await create_session(
        session_repository, uuid.uuid4(), agent_id=uuid.uuid4(), task_id=uuid.uuid4()
    )
    actor = _task_principal(uuid.uuid4())
    with pytest.raises(SessionAccessDenied):
        await service.get_indexes_by_ids(session.id, [], actor=actor)


async def test_get_indexes_by_ids_allows_a_task_principal_for_its_input_session(
    service: SessionNodeService, session_repository: FakeSessionRepository
) -> None:
    """Allow a task principal to read the index of its input session."""
    session = await create_session(
        session_repository, uuid.uuid4(), agent_id=uuid.uuid4(), task_id=uuid.uuid4()
    )
    actor = _task_principal(uuid.uuid4(), granted_session_id=session.id)
    index_by_id = await service.get_indexes_by_ids(session.id, [], actor=actor)
    assert index_by_id == {}


async def test_get_indexes_by_ids_skips_the_ownership_check_for_an_account_principal(
    service: SessionNodeService,
) -> None:
    """Preserve the existing empty-dict result for an unknown session id."""
    index_by_id = await service.get_indexes_by_ids(uuid.uuid4(), [], actor=ACTOR)
    assert index_by_id == {}


async def test_list_nodes_denies_a_task_principal_for_another_tasks_session(
    service: SessionNodeService, session_repository: FakeSessionRepository
) -> None:
    """Reject a task principal listing the nodes of a session it does not own."""
    session = await create_session(
        session_repository, uuid.uuid4(), agent_id=uuid.uuid4(), task_id=uuid.uuid4()
    )
    actor = _task_principal(uuid.uuid4())
    with pytest.raises(SessionAccessDenied):
        await service.list_nodes(SessionNodeFilter(session_id=session.id), actor=actor)


async def test_list_nodes_allows_a_task_principal_for_its_input_session(
    service: SessionNodeService, session_repository: FakeSessionRepository
) -> None:
    """Allow a task principal to list the nodes of its input session."""
    session = await create_session(
        session_repository, uuid.uuid4(), agent_id=uuid.uuid4(), task_id=uuid.uuid4()
    )
    actor = _task_principal(uuid.uuid4(), granted_session_id=session.id)
    nodes, next_cursor = await service.list_nodes(
        SessionNodeFilter(session_id=session.id), actor=actor
    )
    assert nodes == []
    assert next_cursor is None


def _service_with_threshold(
    node_repository: FakeSessionNodeRepository,
    session_repository: FakeSessionRepository,
    task_repository: FakeTaskRepository,
    threshold_bytes: int,
) -> tuple[SessionNodeService, FakeBlobRepository, FakeBlobDataStore]:
    """Build a session node service backed by a payload store at a given threshold."""
    fakes = build_payload_store(threshold_bytes)
    service = SessionNodeService(
        repository=node_repository,
        session_repository=session_repository,
        task_repository=task_repository,
        payload_store=fakes.store,
    )
    return service, fakes.blob_repository, fakes.blob_data_store


async def test_ingest_offloads_over_threshold_payloads(
    node_repository: FakeSessionNodeRepository,
    session_repository: FakeSessionRepository,
    task_repository: FakeTaskRepository,
    session_id: uuid.UUID,
) -> None:
    """Offload reasoning, inputs, outputs, and attributes above the threshold."""
    service, blob_repository, _ = _service_with_threshold(
        node_repository, session_repository, task_repository, threshold_bytes=10
    )
    reasoning = "r" * 50
    inputs = {"a": "i" * 50}
    outputs = {"b": "o" * 50}
    attributes = {"c": "attr" * 50}
    batch = [
        _llm_node(
            0,
            reasoning=reasoning,
            inputs=inputs,
            outputs=outputs,
            attributes=attributes,
        )
    ]
    await service.ingest_nodes(session_id, batch, actor=ACTOR)

    raw = (
        await node_repository.get_by_indexes(session_id, [0], include_payloads=True)
    )[0]
    assert raw.reasoning is not None
    assert raw.reasoning.blob_id is not None
    assert raw.inputs is not None
    assert raw.inputs.blob_id is not None
    assert raw.outputs is not None
    assert raw.outputs.blob_id is not None
    assert raw.attributes is not None
    assert raw.attributes.blob_id is not None

    inputs_blob = await blob_repository.get(raw.inputs.blob_id)
    assert inputs_blob.owner_id == ACTOR.account.id
    assert inputs_blob.media_type == PayloadMediaType.JSON
    assert inputs_blob.stored_in == BlobStorageBackend.DATABASE

    reasoning_blob = await blob_repository.get(raw.reasoning.blob_id)
    assert reasoning_blob.media_type == PayloadMediaType.TEXT


async def test_ingest_under_threshold_stays_inline(
    node_repository: FakeSessionNodeRepository,
    session_repository: FakeSessionRepository,
    task_repository: FakeTaskRepository,
    session_id: uuid.UUID,
) -> None:
    """Keep small payloads inline, with no blob reference."""
    service, _, _ = _service_with_threshold(
        node_repository, session_repository, task_repository, threshold_bytes=1024
    )
    batch = [
        _llm_node(0, reasoning="short", inputs={"a": 1}, attributes={"c": 3}),
    ]
    await service.ingest_nodes(session_id, batch, actor=ACTOR)

    raw = (
        await node_repository.get_by_indexes(session_id, [0], include_payloads=True)
    )[0]
    assert raw.reasoning is not None
    assert raw.reasoning.value == "short"
    assert raw.reasoning.blob_id is None
    assert raw.inputs is not None
    assert raw.inputs.value == {"a": 1}
    assert raw.inputs.blob_id is None
    assert raw.attributes is not None
    assert raw.attributes.value == {"c": 3}
    assert raw.attributes.blob_id is None
    assert raw.outputs is None


async def test_ingest_dedupes_identical_inputs_across_nodes(
    node_repository: FakeSessionNodeRepository,
    session_repository: FakeSessionRepository,
    task_repository: FakeTaskRepository,
    session_id: uuid.UUID,
) -> None:
    """Share one blob between two nodes offloading the same value."""
    service, _, _ = _service_with_threshold(
        node_repository, session_repository, task_repository, threshold_bytes=10
    )
    shared_inputs = {"a": "i" * 50}
    batch = [
        _llm_node(0, inputs=shared_inputs),
        _llm_node(1, inputs=shared_inputs),
    ]
    await service.ingest_nodes(session_id, batch, actor=ACTOR)

    raw = await node_repository.get_by_indexes(
        session_id, [0, 1], include_payloads=True
    )
    assert raw[0].inputs is not None
    assert raw[1].inputs is not None
    assert raw[0].inputs.blob_id is not None
    assert raw[0].inputs.blob_id == raw[1].inputs.blob_id


async def test_ingest_threshold_zero_offloads_every_non_null_payload(
    node_repository: FakeSessionNodeRepository,
    session_repository: FakeSessionRepository,
    task_repository: FakeTaskRepository,
    session_id: uuid.UUID,
) -> None:
    """Offload every non-null payload when the threshold is zero."""
    service, _, _ = _service_with_threshold(
        node_repository, session_repository, task_repository, threshold_bytes=0
    )
    batch = [_llm_node(0, inputs={"a": 1})]
    await service.ingest_nodes(session_id, batch, actor=ACTOR)

    raw = (
        await node_repository.get_by_indexes(session_id, [0], include_payloads=True)
    )[0]
    assert raw.inputs is not None
    assert raw.inputs.blob_id is not None
    # outputs was never set, so it stays trivially None with no payload at all.
    assert raw.outputs is None


async def test_ingest_cache_key_computed_from_raw_inputs_before_offload(
    node_repository: FakeSessionNodeRepository,
    session_repository: FakeSessionRepository,
    task_repository: FakeTaskRepository,
    session_id: uuid.UUID,
) -> None:
    """Compute the cache key from the raw inputs, unaffected by offload."""
    service, _, _ = _service_with_threshold(
        node_repository, session_repository, task_repository, threshold_bytes=10
    )
    inputs = {"q": "i" * 50}
    batch = [
        SessionNodeUpsert(
            index=0,
            node_type=NodeType.TOOL_CALL,
            name="search",
            status=NodeStatus.COMPLETED,
            tool_name="search",
            inputs=inputs,
        )
    ]
    stored = await service.ingest_nodes(session_id, batch, actor=ACTOR)
    assert stored[0].cache_key == compute_tool_cache_key("search", inputs)


async def test_list_all_nodes_include_payloads_hydrates_offloaded_values(
    node_repository: FakeSessionNodeRepository,
    session_repository: FakeSessionRepository,
    task_repository: FakeTaskRepository,
    session_id: uuid.UUID,
) -> None:
    """Return the original values for offloaded payloads when listing with payloads."""
    service, _, _ = _service_with_threshold(
        node_repository, session_repository, task_repository, threshold_bytes=10
    )
    inputs = {"a": "i" * 50}
    await service.ingest_nodes(session_id, [_llm_node(0, inputs=inputs)], actor=ACTOR)

    nodes = await service.list_all_nodes(session_id, include_payloads=True, actor=ACTOR)
    assert nodes[0].inputs is not None
    assert nodes[0].inputs.value == inputs


async def test_list_all_nodes_exclude_payloads_returns_no_payloads(
    node_repository: FakeSessionNodeRepository,
    session_repository: FakeSessionRepository,
    task_repository: FakeTaskRepository,
    session_id: uuid.UUID,
) -> None:
    """Return no payloads, offloaded or inline, when payloads are excluded."""
    service, _, _ = _service_with_threshold(
        node_repository, session_repository, task_repository, threshold_bytes=10
    )
    inputs = {"a": "i" * 50}
    await service.ingest_nodes(session_id, [_llm_node(0, inputs=inputs)], actor=ACTOR)

    nodes = await service.list_all_nodes(
        session_id, include_payloads=False, actor=ACTOR
    )
    assert nodes[0].inputs is None
    assert nodes[0].outputs is None
    assert nodes[0].attributes is None
