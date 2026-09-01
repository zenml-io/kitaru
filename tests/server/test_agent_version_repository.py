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
"""Contract tests for agent version repositories."""

import uuid
from collections.abc import AsyncGenerator, Awaitable, Callable

import pytest

from conftest import (
    FakeAgentRepository,
    FakeAgentVersionRepository,
    FakeExperimentRunRepository,
    FakeSecretRepository,
    FakeSessionRepository,
    FakeTagRepository,
    pg_session,
    pg_session_with_engine,
    postgres_available,
)
from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.session import SessionOrigin
from kitaru.api_models.v1.tag import TagResourceType
from kitaru.server.adapters.db.encryption import AesGcmCipher
from kitaru.server.adapters.db.orm.agent_version import AgentVersionORM
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.agent_repository import SQLAgentRepository
from kitaru.server.adapters.db.repositories.agent_version_repository import (
    SQLAgentVersionRepository,
)
from kitaru.server.adapters.db.repositories.cohort_repository import (
    SQLCohortRepository,
)
from kitaru.server.adapters.db.repositories.cohort_version_repository import (
    SQLCohortVersionRepository,
)
from kitaru.server.adapters.db.repositories.experiment_repository import (
    SQLExperimentRepository,
)
from kitaru.server.adapters.db.repositories.experiment_run_repository import (
    SQLExperimentRunRepository,
)
from kitaru.server.adapters.db.repositories.secret_repository import (
    SQLSecretRepository,
)
from kitaru.server.adapters.db.repositories.session_repository import (
    SQLSessionRepository,
)
from kitaru.server.adapters.db.repositories.tag_repository import SQLTagRepository
from kitaru.server.application.interfaces.agent_version_repository import (
    AgentVersionRepository,
)
from kitaru.server.application.interfaces.session_repository import SessionRepository
from kitaru.server.application.interfaces.tag_repository import TagRepository
from kitaru.server.application.models.agent_version import AgentVersionFilter
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent, AgentNotFound
from kitaru.server.domain.agent_version import (
    AgentCapabilities,
    AgentVersion,
    AgentVersionInUse,
    AgentVersionNotFound,
    RunSpec,
    RuntimeCapabilities,
)
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.cohort import Cohort
from kitaru.server.domain.cohort_version import CohortVersion
from kitaru.server.domain.experiment import Experiment
from kitaru.server.domain.experiment_run import ExperimentRun
from kitaru.server.domain.hook import (
    CopyWorkdirHook,
    SetupCommandHook,
    TeardownCommandHook,
)
from kitaru.server.domain.replay_config import (
    PassthroughConfig,
    ReplayConfig,
    ToolPolicy,
)
from kitaru.server.domain.secret import Secret
from kitaru.server.domain.session import Session
from kitaru.server.domain.tag import Tag, TagLink
from kitaru.server.filtering import FilterCondition

Setup = tuple[
    AgentVersionRepository,
    uuid.UUID,
    uuid.UUID,
    Callable[[], Awaitable[uuid.UUID]],
    Callable[[], Awaitable[uuid.UUID]],
    TagRepository,
    SessionRepository,
    Callable[[uuid.UUID], Awaitable[None]],
]


@pytest.fixture(params=["fake", "postgres"])
async def setup(request: pytest.FixtureRequest) -> AsyncGenerator[Setup, None]:
    """Provide each agent version repository implementation and its collaborators.

    Yields the repository, an owner id, an agent id to attach versions to,
    factories for further agent ids and secret ids a run spec can reference,
    a tag repository sharing the backend, a session repository sharing the
    backend, and a factory attaching an experiment run to a given agent
    version id.
    """
    if request.param == "fake":
        agents = FakeAgentRepository()
        secrets = FakeSecretRepository()
        tags = FakeTagRepository()
        sessions = FakeSessionRepository()
        experiment_runs = FakeExperimentRunRepository()
        owner_id = uuid.uuid4()

        async def make_agent_id() -> uuid.UUID:
            created = await agents.create(
                Agent(owner_id=owner_id, name=f"agent-{uuid.uuid4().hex[:8]}")
            )
            return created.id

        async def make_secret_id() -> uuid.UUID:
            created = await secrets.create(
                Secret(
                    owner_id=owner_id, name=f"secret-{uuid.uuid4().hex[:8]}", values={}
                )
            )
            return created.id

        async def attach_experiment_run(agent_version_id: uuid.UUID) -> None:
            await experiment_runs.create(
                ExperimentRun(
                    owner_id=owner_id,
                    experiment_id=uuid.uuid4(),
                    number=1,
                    cohort_version_id=uuid.uuid4(),
                    agent_version_id=agent_version_id,
                )
            )

        agent_id = await make_agent_id()
        yield (
            FakeAgentVersionRepository(
                agents, tags=tags, experiment_runs=experiment_runs, sessions=sessions
            ),
            owner_id,
            agent_id,
            make_agent_id,
            make_secret_id,
            tags,
            sessions,
            attach_experiment_run,
        )
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session_with_engine() as (session, engine):
        accounts = SQLAccountRepository(session)
        owner = await accounts.create(Account(name="owner"))
        agent_repository = SQLAgentRepository(session)
        secret_repository = SQLSecretRepository(
            session, AesGcmCipher("test-encryption-key")
        )

        async def make_agent_id() -> uuid.UUID:
            created = await agent_repository.create(
                Agent(owner_id=owner.id, name=f"agent-{uuid.uuid4().hex[:8]}")
            )
            return created.id

        async def make_secret_id() -> uuid.UUID:
            created = await secret_repository.create(
                Secret(
                    owner_id=owner.id, name=f"secret-{uuid.uuid4().hex[:8]}", values={}
                )
            )
            return created.id

        agent_id = await make_agent_id()
        cohorts_repository = SQLCohortRepository(session)
        cohort_versions_repository = SQLCohortVersionRepository(session)
        experiments_repository = SQLExperimentRepository(session)
        experiment_run_repository = SQLExperimentRunRepository(session)

        async def attach_experiment_run(agent_version_id: uuid.UUID) -> None:
            config = await experiments_repository.create_replay_config(
                ReplayConfig(
                    owner_id=owner.id,
                    tool_policy=ToolPolicy(default=PassthroughConfig()),
                    evaluators=[],
                )
            )
            experiment = await experiments_repository.create(
                Experiment(
                    owner_id=owner.id,
                    name=f"exp-{uuid.uuid4().hex[:8]}",
                    agent_id=agent_id,
                    replay_config_id=config.id,
                )
            )
            cohort = await cohorts_repository.create(
                Cohort(
                    owner_id=owner.id,
                    name=f"cohort-{uuid.uuid4().hex[:8]}",
                    agent_id=agent_id,
                )
            )
            cohort_version = await cohort_versions_repository.create(
                CohortVersion(owner_id=owner.id, cohort_id=cohort.id, session_count=0),
                [],
            )
            await experiment_run_repository.create(
                ExperimentRun(
                    owner_id=owner.id,
                    experiment_id=experiment.id,
                    number=1,
                    cohort_version_id=cohort_version.id,
                    agent_version_id=agent_version_id,
                )
            )

        yield (
            SQLAgentVersionRepository(session),
            owner.id,
            agent_id,
            make_agent_id,
            make_secret_id,
            SQLTagRepository(session),
            SQLSessionRepository(session, engine),
            attach_experiment_run,
        )


async def test_create_sets_version_and_timestamps(setup: Setup) -> None:
    """Assign version 1 and both timestamps to the first version."""
    repository, owner_id, agent_id, _, _, _, _, _ = setup
    version = await repository.create(
        AgentVersion(
            owner_id=owner_id,
            agent_id=agent_id,
            display_version="v1",
            description="First cut",
        )
    )
    assert version.agent_id == agent_id
    assert version.owner_id == owner_id
    assert version.version == 1
    assert version.display_version == "v1"
    assert version.description == "First cut"
    assert version.run_spec is None
    assert version.capabilities == AgentCapabilities()
    assert version.created is not None
    assert version.updated is not None


async def test_create_numbers_versions_sequentially(setup: Setup) -> None:
    """Assign consecutive version numbers per agent."""
    repository, owner_id, agent_id, _, _, _, _, _ = setup
    first = await repository.create(AgentVersion(owner_id=owner_id, agent_id=agent_id))
    second = await repository.create(AgentVersion(owner_id=owner_id, agent_id=agent_id))
    third = await repository.create(AgentVersion(owner_id=owner_id, agent_id=agent_id))
    assert [first.version, second.version, third.version] == [1, 2, 3]


async def test_create_missing_agent(setup: Setup) -> None:
    """Raise when the agent does not exist."""
    repository, owner_id, _, _, _, _, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(AgentNotFound, match=f"Agent {missing_id} was not found"):
        await repository.create(AgentVersion(owner_id=owner_id, agent_id=missing_id))


async def test_create_with_run_spec_and_secret_order(setup: Setup) -> None:
    """Round-trip a run spec, preserving the secret id order."""
    repository, owner_id, agent_id, _, make_secret_id, _, _, _ = setup
    secret_ids = [
        await make_secret_id(),
        await make_secret_id(),
        await make_secret_id(),
    ]
    run_spec = RunSpec(
        command="run.sh",
        working_dir="/app",
        env={"FOO": "bar"},
        secret_ids=secret_ids,
        timeout_seconds=120,
    )
    version = await repository.create(
        AgentVersion(owner_id=owner_id, agent_id=agent_id, run_spec=run_spec)
    )
    assert version.run_spec == run_spec
    loaded = await repository.get(version.id)
    assert loaded.run_spec is not None
    assert loaded.run_spec.secret_ids == secret_ids


async def test_create_with_run_spec_hooks(setup: Setup) -> None:
    """Round-trip a run spec's hooks, preserving each variant and its fields."""
    repository, owner_id, agent_id, _, _, _, _, _ = setup
    hooks = [
        CopyWorkdirHook(),
        SetupCommandHook(command="setup.sh"),
        TeardownCommandHook(command="teardown.sh", on="always"),
    ]
    version = await repository.create(
        AgentVersion(
            owner_id=owner_id,
            agent_id=agent_id,
            run_spec=RunSpec(command="run.sh", hooks=hooks),
        )
    )
    assert version.run_spec is not None
    assert version.run_spec.hooks == hooks
    loaded = await repository.get(version.id)
    assert loaded.run_spec is not None
    assert loaded.run_spec.hooks == hooks


async def test_create_with_run_spec_without_hooks(setup: Setup) -> None:
    """Read back an empty hooks list for a run spec created without hooks."""
    repository, owner_id, agent_id, _, _, _, _, _ = setup
    version = await repository.create(
        AgentVersion(
            owner_id=owner_id,
            agent_id=agent_id,
            run_spec=RunSpec(command="run.sh"),
        )
    )
    loaded = await repository.get(version.id)
    assert loaded.run_spec is not None
    assert loaded.run_spec.hooks == []


async def test_create_with_run_spec_runtime_capabilities(setup: Setup) -> None:
    """Round-trip a run spec's declared runtime capabilities."""
    repository, owner_id, agent_id, _, _, _, _, _ = setup
    runtime_capabilities = RuntimeCapabilities(overrides=False, tool_policies=False)
    version = await repository.create(
        AgentVersion(
            owner_id=owner_id,
            agent_id=agent_id,
            run_spec=RunSpec(
                command="run.sh", runtime_capabilities=runtime_capabilities
            ),
        )
    )
    assert version.run_spec is not None
    assert version.run_spec.runtime_capabilities == runtime_capabilities
    loaded = await repository.get(version.id)
    assert loaded.run_spec is not None
    assert loaded.run_spec.runtime_capabilities == runtime_capabilities


async def test_create_with_run_spec_without_runtime_capabilities(setup: Setup) -> None:
    """Read back all-true runtime capabilities for a run spec created without them."""
    repository, owner_id, agent_id, _, _, _, _, _ = setup
    version = await repository.create(
        AgentVersion(
            owner_id=owner_id,
            agent_id=agent_id,
            run_spec=RunSpec(command="run.sh"),
        )
    )
    loaded = await repository.get(version.id)
    assert loaded.run_spec is not None
    assert loaded.run_spec.runtime_capabilities == RuntimeCapabilities()


async def test_create_with_unknown_secret_id_rejected() -> None:
    """Reject a run spec that references a secret that does not exist."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        owner = await SQLAccountRepository(session).create(Account(name="owner"))
        agent = await SQLAgentRepository(session).create(
            Agent(owner_id=owner.id, name=f"agent-{uuid.uuid4().hex[:8]}")
        )
        repository = SQLAgentVersionRepository(session)
        with pytest.raises(ValidationError):
            await repository.create(
                AgentVersion(
                    owner_id=owner.id,
                    agent_id=agent.id,
                    run_spec=RunSpec(command="run.sh", secret_ids=[uuid.uuid4()]),
                )
            )


async def test_null_run_hooks_column_reads_back_empty() -> None:
    """Read back an empty hooks list from a row stored before the hooks column."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        owner = await SQLAccountRepository(session).create(Account(name="owner"))
        agent = await SQLAgentRepository(session).create(
            Agent(owner_id=owner.id, name=f"agent-{uuid.uuid4().hex[:8]}")
        )
        repository = SQLAgentVersionRepository(session)
        created = await repository.create(
            AgentVersion(
                owner_id=owner.id,
                agent_id=agent.id,
                run_spec=RunSpec(command="run.sh"),
            )
        )
        row = await session.get(AgentVersionORM, created.id)
        assert row is not None
        row.run_hooks = None
        await session.flush()
        loaded = await repository.get(created.id)
        assert loaded.run_spec is not None
        assert loaded.run_spec.hooks == []


async def test_null_run_runtime_capabilities_column_reads_back_all_true() -> None:
    """Read back all-true runtime capabilities from a row stored before the column."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        owner = await SQLAccountRepository(session).create(Account(name="owner"))
        agent = await SQLAgentRepository(session).create(
            Agent(owner_id=owner.id, name=f"agent-{uuid.uuid4().hex[:8]}")
        )
        repository = SQLAgentVersionRepository(session)
        created = await repository.create(
            AgentVersion(
                owner_id=owner.id,
                agent_id=agent.id,
                run_spec=RunSpec(command="run.sh"),
            )
        )
        row = await session.get(AgentVersionORM, created.id)
        assert row is not None
        row.run_runtime_capabilities = None
        await session.flush()
        loaded = await repository.get(created.id)
        assert loaded.run_spec is not None
        assert loaded.run_spec.runtime_capabilities == RuntimeCapabilities()


async def test_get(setup: Setup) -> None:
    """Load a stored agent version by id."""
    repository, owner_id, agent_id, _, _, _, _, _ = setup
    created = await repository.create(
        AgentVersion(owner_id=owner_id, agent_id=agent_id)
    )
    loaded = await repository.get(created.id)
    assert loaded == created


async def test_get_not_found(setup: Setup) -> None:
    """Raise for an unknown agent version id."""
    repository, _, _, _, _, _, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(
        AgentVersionNotFound, match=f"Agent version {missing_id} was not found"
    ):
        await repository.get(missing_id)


async def test_query_scoped_to_agent(setup: Setup) -> None:
    """Query only the versions of the requested agent, newest-first."""
    repository, owner_id, agent_id, make_agent_id, _, _, _, _ = setup
    other_agent_id = await make_agent_id()

    v1 = await repository.create(AgentVersion(owner_id=owner_id, agent_id=agent_id))
    v2 = await repository.create(AgentVersion(owner_id=owner_id, agent_id=agent_id))
    await repository.create(AgentVersion(owner_id=owner_id, agent_id=other_agent_id))

    versions, next_cursor = await repository.query(
        AgentVersionFilter(agent_id=agent_id)
    )
    assert next_cursor is None
    assert [version.id for version in versions] == [v2.id, v1.id]


async def test_query_filters_by_tag(setup: Setup) -> None:
    """Filter agent versions linked to a tag through tag_link."""
    repository, owner_id, agent_id, _, _, tags, _, _ = setup
    tagged = await repository.create(AgentVersion(owner_id=owner_id, agent_id=agent_id))
    await repository.create(AgentVersion(owner_id=owner_id, agent_id=agent_id))
    tag = await tags.create(Tag(owner_id=owner_id, name="smoke-test"))
    await tags.create_link(
        TagLink(
            tag_id=tag.id,
            resource_type=TagResourceType.AGENT_VERSION,
            resource_id=tagged.id,
        )
    )
    versions, _ = await repository.query(
        AgentVersionFilter(
            expression=FilterCondition(field="tag", op=FilterOp.EQ, value="smoke-test")
        )
    )
    assert [version.id for version in versions] == [tagged.id]


async def test_query_walks_pages(setup: Setup) -> None:
    """Walk every page via next_cursor without duplicates or gaps."""
    repository, owner_id, agent_id, _, _, _, _, _ = setup
    created = [
        await repository.create(AgentVersion(owner_id=owner_id, agent_id=agent_id))
        for _ in range(5)
    ]
    expected_order = list(reversed(created))

    collected: list[AgentVersion] = []
    cursor = None
    while True:
        versions, next_cursor = await repository.query(
            AgentVersionFilter(agent_id=agent_id, cursor=cursor, size=2)
        )
        collected.extend(versions)
        if next_cursor is None:
            break
        cursor = next_cursor

    assert collected == expected_order
    assert len({version.id for version in collected}) == 5


async def test_update(setup: Setup) -> None:
    """Persist field changes and renew the updated timestamp."""
    repository, owner_id, agent_id, _, _, _, _, _ = setup
    created = await repository.create(
        AgentVersion(
            owner_id=owner_id,
            agent_id=agent_id,
            display_version="v1",
            description="First cut",
        )
    )
    created.update_display_version("v1.1")
    created.update_description("Second cut")
    created.update_capabilities(AgentCapabilities(tools=["search"]))
    updated = await repository.update(created)
    assert updated.display_version == "v1.1"
    assert updated.description == "Second cut"
    assert updated.capabilities == AgentCapabilities(tools=["search"])
    assert updated.created == created.created
    assert updated.updated is not None
    assert created.updated is not None
    assert updated.updated > created.updated
    loaded = await repository.get(created.id)
    assert loaded == updated


async def test_update_not_found(setup: Setup) -> None:
    """Raise for an unknown agent version id."""
    repository, owner_id, agent_id, _, _, _, _, _ = setup
    version = AgentVersion(owner_id=owner_id, agent_id=agent_id)
    with pytest.raises(
        AgentVersionNotFound, match=f"Agent version {version.id} was not found"
    ):
        await repository.update(version)


async def test_update_replaces_run_spec_and_secret_links(setup: Setup) -> None:
    """Replace the run spec, including its secret links, on update."""
    repository, owner_id, agent_id, _, make_secret_id, _, _, _ = setup
    old_secret_ids = [await make_secret_id(), await make_secret_id()]
    created = await repository.create(
        AgentVersion(
            owner_id=owner_id,
            agent_id=agent_id,
            run_spec=RunSpec(command="old.sh", secret_ids=old_secret_ids),
        )
    )
    new_secret_ids = [await make_secret_id()]
    new_run_spec = RunSpec(command="new.sh", secret_ids=new_secret_ids)
    created.update_run_spec(new_run_spec)
    updated = await repository.update(created)
    assert updated.run_spec == new_run_spec

    loaded = await repository.get(created.id)
    assert loaded.run_spec is not None
    assert loaded.run_spec.secret_ids == new_secret_ids


async def test_update_replaces_run_spec_hooks(setup: Setup) -> None:
    """Replace the run spec's hooks on update."""
    repository, owner_id, agent_id, _, _, _, _, _ = setup
    created = await repository.create(
        AgentVersion(
            owner_id=owner_id,
            agent_id=agent_id,
            run_spec=RunSpec(command="run.sh", hooks=[CopyWorkdirHook()]),
        )
    )
    new_hooks = [
        SetupCommandHook(command="setup.sh"),
        TeardownCommandHook(command="teardown.sh", on="always"),
    ]
    created.update_run_spec(RunSpec(command="run.sh", hooks=new_hooks))
    updated = await repository.update(created)
    assert updated.run_spec is not None
    assert updated.run_spec.hooks == new_hooks

    loaded = await repository.get(created.id)
    assert loaded.run_spec is not None
    assert loaded.run_spec.hooks == new_hooks


async def test_update_replaces_run_spec_runtime_capabilities(setup: Setup) -> None:
    """Replace the run spec's runtime capabilities on update."""
    repository, owner_id, agent_id, _, _, _, _, _ = setup
    created = await repository.create(
        AgentVersion(
            owner_id=owner_id,
            agent_id=agent_id,
            run_spec=RunSpec(command="run.sh"),
        )
    )
    new_runtime_capabilities = RuntimeCapabilities(overrides=False, tool_policies=True)
    created.update_run_spec(
        RunSpec(command="run.sh", runtime_capabilities=new_runtime_capabilities)
    )
    updated = await repository.update(created)
    assert updated.run_spec is not None
    assert updated.run_spec.runtime_capabilities == new_runtime_capabilities

    loaded = await repository.get(created.id)
    assert loaded.run_spec is not None
    assert loaded.run_spec.runtime_capabilities == new_runtime_capabilities


async def test_update_clears_run_spec_and_secret_links(setup: Setup) -> None:
    """Drop the secret links when the run spec is cleared."""
    repository, owner_id, agent_id, _, make_secret_id, _, _, _ = setup
    created = await repository.create(
        AgentVersion(
            owner_id=owner_id,
            agent_id=agent_id,
            run_spec=RunSpec(command="run.sh", secret_ids=[await make_secret_id()]),
        )
    )
    created.update_run_spec(None)
    updated = await repository.update(created)
    assert updated.run_spec is None
    loaded = await repository.get(created.id)
    assert loaded.run_spec is None


async def test_delete(setup: Setup) -> None:
    """Delete a stored agent version."""
    repository, owner_id, agent_id, _, _, _, _, _ = setup
    created = await repository.create(
        AgentVersion(owner_id=owner_id, agent_id=agent_id)
    )
    await repository.delete(created.id)
    with pytest.raises(AgentVersionNotFound):
        await repository.get(created.id)


async def test_delete_not_found(setup: Setup) -> None:
    """Raise for an unknown agent version id."""
    repository, _, _, _, _, _, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(
        AgentVersionNotFound, match=f"Agent version {missing_id} was not found"
    ):
        await repository.delete(missing_id)


async def test_delete_restricted_by_experiment_run(setup: Setup) -> None:
    """Reject deleting a version referenced by an experiment run."""
    repository, owner_id, agent_id, _, _, _, _, attach_experiment_run = setup
    created = await repository.create(
        AgentVersion(owner_id=owner_id, agent_id=agent_id)
    )
    await attach_experiment_run(created.id)

    with pytest.raises(AgentVersionInUse):
        await repository.delete(created.id)


async def test_delete_nulls_session_agent_version(setup: Setup) -> None:
    """Null a session's agent version pointer when the version is deleted."""
    repository, owner_id, agent_id, _, _, _, sessions, _ = setup
    created = await repository.create(
        AgentVersion(owner_id=owner_id, agent_id=agent_id)
    )
    session = await sessions.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            agent_version_id=created.id,
            number=1,
            origin=SessionOrigin.RECORDED,
        )
    )

    await repository.delete(created.id)

    reloaded = await sessions.get(session.id, include_payloads=True)
    assert reloaded.agent_version_id is None
