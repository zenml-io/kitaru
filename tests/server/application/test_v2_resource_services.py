#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
"""Tests for the v2 resource application services."""

import uuid
from io import BytesIO
from typing import Any

import pytest

from kitaru.server.application.models.agent import AgentUpdate
from kitaru.server.application.models.agent_version import AgentVersionUpdate
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.cohort import CohortCreate
from kitaru.server.application.models.evaluation import EvaluationFilter
from kitaru.server.application.models.plugin import PluginFilter, PluginUpdate
from kitaru.server.application.models.session import SessionCreate
from kitaru.server.application.models.tag import TagUpdate
from kitaru.server.application.services.agent_service import AgentService
from kitaru.server.application.services.agent_version_service import (
    AgentVersionService,
)
from kitaru.server.application.services.blob_service import BlobService
from kitaru.server.application.services.cohort_service import CohortService
from kitaru.server.application.services.evaluation_service import EvaluationService
from kitaru.server.application.services.plugin_service import PluginService
from kitaru.server.application.services.session_service import SessionService
from kitaru.server.application.services.tag_service import TagService
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.agent_version import AgentCapabilities, AgentVersion
from kitaru.server.domain.base import ConflictError, ValidationError
from kitaru.server.domain.blob import BlobNotFound, BlobTooLarge
from kitaru.server.domain.cohort import InvalidCohortMembers
from kitaru.server.domain.plugin import (
    PackagePluginSource,
    Plugin,
    PluginKind,
    PluginNotFound,
    ScriptPluginSource,
)
from kitaru.server.domain.session import Session, SessionOrigin
from kitaru.server.domain.tag import TagResourceType
from kitaru.server.domain.task import AgentTask, TaskStatus


def actor() -> AuthContext:
    """Create a caller context."""
    return AuthContext(account=Account(name="tester"))


class EntityRepository:
    """Small repository fake for entity CRUD tests."""

    def __init__(self, *entities: Any) -> None:
        self.entities = {entity.id: entity for entity in entities}
        self.updated: list[Any] = []
        self.deleted: list[uuid.UUID] = []

    async def create(self, entity: Any):
        self.entities[entity.id] = entity
        return entity

    async def get(self, entity_id: uuid.UUID, exclusive: bool = False):
        _ = exclusive
        return self.entities[entity_id]

    async def get_many(self, ids: list[uuid.UUID]):
        return {
            item_id: self.entities[item_id]
            for item_id in ids
            if item_id in self.entities
        }

    async def query(self, filter_: Any):
        self.last_filter = filter_
        return list(self.entities.values()), None

    async def update(self, entity: Any):
        self.updated.append(entity)
        self.entities[entity.id] = entity
        return entity

    async def delete(self, entity_id: uuid.UUID):
        self.deleted.append(entity_id)


async def test_agent_service_preserves_patch_presence() -> None:
    """Apply explicit description clears and reject a null name."""
    stored = Agent(owner_id=uuid.uuid4(), name="old", description="description")
    repository: Any = EntityRepository(stored)
    service = AgentService(repository)
    context = actor()
    created = await service.create_agent("new", None, context)
    assert created.owner_id == context.account.id
    updated = await service.update_agent(
        stored.id, AgentUpdate(description=None), actor()
    )
    assert updated.description is None
    with pytest.raises(ValidationError, match="cannot be null"):
        await service.update_agent(stored.id, AgentUpdate(name=None), actor())


async def test_agent_version_service_assigns_version_and_freezes_execution() -> None:
    """Use the parent's next number and enforce execution-field freezing."""
    agent = Agent(owner_id=uuid.uuid4(), name="agent")
    agent_repository: Any = EntityRepository(agent)
    agent_repository.next_version = lambda agent_id: _async_value(3)
    agent_repository.version_is_frozen = lambda version_id: _async_value(True)
    version_repository: Any = EntityRepository()
    secret_repository: Any = EntityRepository()
    service = AgentVersionService(
        version_repository, agent_repository, secret_repository
    )
    version = await service.create_version(agent.id, "v3", None, None, None, actor())
    assert version.version == 3
    assert version.capabilities == AgentCapabilities()
    with pytest.raises(ConflictError, match="frozen"):
        await service.update_version(
            version.id,
            AgentVersionUpdate(capabilities=AgentCapabilities(tools=["new"])),
            actor(),
        )


async def _async_value(value: Any) -> Any:
    return value


class BlobRepositoryFake:
    """Digest-aware blob repository fake."""

    def __init__(self) -> None:
        self.by_sha: dict[str, Any] = {}
        self.by_id: dict[uuid.UUID, Any] = {}
        self.creates = 0

    async def get_by_sha256(self, sha256: str):
        if sha256 not in self.by_sha:
            raise BlobNotFound(uuid.UUID(int=0))
        return self.by_sha[sha256]

    async def create(self, blob):
        self.creates += 1
        self.by_sha[blob.sha256] = blob
        self.by_id[blob.id] = blob
        return blob, True

    async def get(self, blob_id: uuid.UUID):
        return self.by_id[blob_id].model_copy(update={"data": None})

    async def get_content(self, blob_id: uuid.UUID):
        return self.by_id[blob_id]


async def test_blob_service_enforces_cap_and_deduplicates() -> None:
    """Reject oversized data and avoid a second insert on a digest hit."""
    repository: Any = BlobRepositoryFake()
    service = BlobService(repository, max_size_bytes=3)
    with pytest.raises(BlobTooLarge):
        await service.upload_blob(b"four", "text/plain", actor())
    first, created = await service.upload_blob(BytesIO(b"one"), "text/plain", actor())
    second, created_again = await service.upload_blob(
        BytesIO(b"one"), "application/octet-stream", actor()
    )
    assert created is True
    assert created_again is False
    assert second is first
    assert repository.creates == 1
    metadata = await service.get_blob(first.id, actor())
    content = await service.download_blob(first.id, actor())
    assert metadata.data is None
    assert content.data == b"one"


class CohortRepositoryFake(EntityRepository):
    """Cohort repository fake retaining member order."""

    async def create(self, entity, session_ids=None):
        assert session_ids is not None
        self.session_ids = list(session_ids)
        return await super().create(entity)

    async def get_session_ids(self, cohort_id):
        await self.get(cohort_id)
        return self.session_ids


async def test_cohort_service_validates_members_and_keeps_order() -> None:
    """Reject duplicate ids and persist valid members in input order."""
    agent = Agent(owner_id=uuid.uuid4(), name="agent")
    sessions = [
        Session(
            owner_id=uuid.uuid4(),
            agent_id=agent.id,
            origin=SessionOrigin.RECORDED,
        )
        for _ in range(2)
    ]
    cohort_repository: Any = CohortRepositoryFake()
    session_repository: Any = EntityRepository(*sessions)
    agent_repository: Any = EntityRepository(agent)
    service = CohortService(cohort_repository, session_repository, agent_repository)
    with pytest.raises(InvalidCohortMembers, match="unique"):
        await service.create_cohort(
            CohortCreate(
                name="bad",
                agent_id=agent.id,
                session_ids=[sessions[0].id, sessions[0].id],
            ),
            actor(),
        )
    cohort = await service.create_cohort(
        CohortCreate(
            name="valid",
            agent_id=agent.id,
            session_ids=[sessions[1].id, sessions[0].id],
        ),
        actor(),
    )
    assert cohort.session_count == 2
    assert cohort_repository.session_ids == [sessions[1].id, sessions[0].id]


class EvaluationRepositoryFake(EntityRepository):
    """Evaluation repository fake."""

    async def upsert_manual(self, evaluations):
        self.manual = evaluations
        return evaluations


async def test_evaluation_service_forces_manual_identity() -> None:
    """Clear evaluator/task provenance and set caller/session identity."""
    session = Session(
        owner_id=uuid.uuid4(),
        agent_id=uuid.uuid4(),
        origin=SessionOrigin.RECORDED,
    )
    repository: Any = EvaluationRepositoryFake()
    session_repository: Any = EntityRepository(session)
    plugin_repository: Any = PluginRepositoryFake()
    service = EvaluationService(repository, session_repository, plugin_repository)
    result = await service.merge_evaluations(
        session.id,
        [
            {
                "name": "quality",
                "score": 0.5,
                "value": None,
                "explanation": None,
            },
            {
                "name": "passed",
                "score": True,
                "value": None,
                "explanation": None,
            },
            {
                "name": "label",
                "score": None,
                "value": "good",
                "explanation": None,
            },
            {
                "name": "graded-label",
                "score": 0.8,
                "value": "good",
                "explanation": None,
            },
        ],
        actor(),
    )
    assert result[0].session_id == session.id
    assert result[0].task_id is None
    assert result[0].evaluator_version_id is None
    assert [item.data_type.value for item in result] == [
        "float",
        "bool",
        "str",
        "categorical",
    ]
    assert await service.list_evaluations(EvaluationFilter(), actor()) == (
        [],
        None,
    )


class PluginRepositoryFake(EntityRepository):
    """Plugin and version repository fake."""

    async def next_version(self, plugin_id):
        return 1

    async def create_version(self, version):
        self.version = version
        return version

    async def get_version_number(self, plugin_id, version):
        return self.version

    async def query_versions(self, version_filter):
        return [self.version], None

    async def update_version(self, version):
        return version


async def test_plugin_service_scopes_kind_and_validates_script_blob() -> None:
    """Hide another plugin kind and require script blobs to exist."""
    evaluator = Plugin(owner_id=uuid.uuid4(), kind=PluginKind.EVALUATOR, name="judge")
    importer = Plugin(owner_id=uuid.uuid4(), kind=PluginKind.IMPORTER, name="parser")
    repository: Any = PluginRepositoryFake(evaluator, importer)
    blob_repository: Any = EntityRepository()
    service = PluginService(repository, blob_repository, PluginKind.EVALUATOR)
    with pytest.raises(PluginNotFound):
        await service.get_plugin(importer.id, actor())
    await service.list_plugins(PluginFilter(kind=PluginKind.IMPORTER), actor())
    assert repository.last_filter.kind is PluginKind.EVALUATOR
    package = await service.create_version(
        evaluator.id,
        PackagePluginSource(requirement="package==1.0", entrypoint="package:evaluate"),
        None,
        actor(),
    )
    assert package.version == 1
    blob = type("BlobRef", (), {"id": uuid.uuid4()})()
    blob_repository.entities[blob.id] = blob
    script = await service.create_version(
        evaluator.id,
        ScriptPluginSource(blob_id=blob.id, entrypoint="evaluate"),
        None,
        actor(),
    )
    assert isinstance(script.source, ScriptPluginSource)
    assert script.source.blob_id == blob.id
    with pytest.raises(ValidationError, match="metadata"):
        await service.update_plugin(evaluator.id, PluginUpdate(metadata=None), actor())


async def test_session_service_links_one_running_agent_task() -> None:
    """Create a task-linked session and reject a non-running task."""
    agent = Agent(owner_id=uuid.uuid4(), name="agent")
    version = AgentVersion(owner_id=uuid.uuid4(), agent_id=agent.id, version=1)
    task = AgentTask(
        job_id=uuid.uuid4(),
        agent_version_id=version.id,
        status=TaskStatus.RUNNING,
    )
    session_repository: Any = EntityRepository()
    task_repository: Any = EntityRepository(task)
    agent_repository: Any = EntityRepository(agent)
    version_repository: Any = EntityRepository(version)
    service = SessionService(
        session_repository,
        agent_repository,
        version_repository,
        task_repository,
    )
    session = await service.create_session(
        SessionCreate(
            agent_id=agent.id,
            agent_version_id=version.id,
            origin=SessionOrigin.RECORDED,
            task_id=task.id,
        ),
        actor(),
    )
    assert task.result_session_id == session.id
    task.status = TaskStatus.COMPLETED
    with pytest.raises(ConflictError, match="not running"):
        await service.create_session(
            SessionCreate(
                agent_id=agent.id,
                agent_version_id=version.id,
                origin=SessionOrigin.RECORDED,
                task_id=task.id,
            ),
            actor(),
        )


async def test_tag_service_updates_and_links() -> None:
    """Update a tag and create a polymorphic link."""
    repository: Any = EntityRepository()

    async def create_link(link):
        return link

    repository.create_link = create_link
    service = TagService(repository)
    tag = await service.create_tag("one", actor())
    updated = await service.update_tag(tag.id, TagUpdate(name="two"), actor())
    assert updated.name == "two"
    link = await service.create_tag_link(
        tag.id, TagResourceType.SESSION, uuid.uuid4(), actor()
    )
    assert link.tag_id == tag.id
