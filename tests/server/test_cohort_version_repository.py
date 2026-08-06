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
"""Contract tests for cohort version repositories."""

import itertools
import uuid
from collections.abc import AsyncGenerator, Awaitable, Callable

import pytest

from conftest import (
    FakeCohortRepository,
    FakeCohortVersionRepository,
    FakeExperimentRunRepository,
    FakeSessionRepository,
    FakeTagRepository,
    create_cohort,
    create_session,
    pg_session_with_engine,
    postgres_available,
)
from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.session import SessionOrigin
from kitaru.api_models.v1.tag import TagResourceType
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
from kitaru.server.adapters.db.repositories.session_repository import (
    SQLSessionRepository,
)
from kitaru.server.adapters.db.repositories.tag_repository import SQLTagRepository
from kitaru.server.application.interfaces.cohort_version_repository import (
    CohortVersionRepository,
)
from kitaru.server.application.interfaces.session_repository import SessionRepository
from kitaru.server.application.interfaces.tag_repository import TagRepository
from kitaru.server.application.models.cohort import CohortVersionFilter
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.agent_version import AgentVersion
from kitaru.server.domain.cohort import Cohort, CohortNotFound
from kitaru.server.domain.cohort_version import (
    CohortVersion,
    CohortVersionIdNotFound,
    CohortVersionInUse,
    CohortVersionNotFound,
)
from kitaru.server.domain.experiment import Experiment
from kitaru.server.domain.experiment_run import ExperimentRun
from kitaru.server.domain.replay_config import (
    PassthroughConfig,
    ReplayConfig,
    ToolPolicy,
)
from kitaru.server.domain.session import Session, SessionInUse
from kitaru.server.domain.tag import Tag, TagLink
from kitaru.server.filtering import FilterCondition

Setup = tuple[
    CohortVersionRepository,
    SessionRepository,
    uuid.UUID,
    uuid.UUID,
    Callable[[], Awaitable[uuid.UUID]],
    Callable[[uuid.UUID], Awaitable[None]],
    TagRepository,
]


@pytest.fixture(params=["fake", "postgres"])
async def setup(request: pytest.FixtureRequest) -> AsyncGenerator[Setup, None]:
    """Provide each cohort version repository implementation, a session
    repository sharing its backend, an owner id, a cohort id to attach
    versions to, a factory for further session ids on the cohort's agent,
    a factory attaching an experiment run to a given cohort version id, and
    a tag repository sharing the backend."""
    if request.param == "fake":
        sessions = FakeSessionRepository()
        cohorts = FakeCohortRepository()
        experiment_runs = FakeExperimentRunRepository()
        tags = FakeTagRepository()
        cohort_versions = FakeCohortVersionRepository(
            cohorts, sessions, experiment_runs=experiment_runs, tags=tags
        )
        owner_id = uuid.uuid4()
        agent_id = uuid.uuid4()
        cohort = await create_cohort(cohorts, owner_id, agent_id)

        async def make_session_id() -> uuid.UUID:
            created = await create_session(sessions, owner_id, agent_id=agent_id)
            return created.id

        async def attach_experiment_run(cohort_version_id: uuid.UUID) -> None:
            await experiment_runs.create(
                ExperimentRun(
                    owner_id=owner_id,
                    experiment_id=uuid.uuid4(),
                    number=1,
                    cohort_version_id=cohort_version_id,
                    agent_version_id=uuid.uuid4(),
                )
            )

        yield (
            cohort_versions,
            sessions,
            owner_id,
            cohort.id,
            make_session_id,
            attach_experiment_run,
            tags,
        )
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session_with_engine() as (session, engine):
        accounts = SQLAccountRepository(session)
        owner = await accounts.create(Account(name="owner"))
        agents = SQLAgentRepository(session)
        agent = await agents.create(Agent(owner_id=owner.id, name="assistant"))
        cohorts_repository = SQLCohortRepository(session)
        cohort = await cohorts_repository.create(
            Cohort(owner_id=owner.id, name="cohort", agent_id=agent.id)
        )
        sessions_repository = SQLSessionRepository(session, engine)
        agent_versions_repository = SQLAgentVersionRepository(session)
        experiments_repository = SQLExperimentRepository(session)
        experiment_run_repository = SQLExperimentRunRepository(session)
        session_numbers = itertools.count(1)

        async def make_session_id() -> uuid.UUID:
            created = await sessions_repository.create(
                Session(
                    owner_id=owner.id,
                    agent_id=agent.id,
                    number=next(session_numbers),
                    origin=SessionOrigin.RECORDED,
                )
            )
            return created.id

        async def attach_experiment_run(cohort_version_id: uuid.UUID) -> None:
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
                    replay_config_id=config.id,
                )
            )
            agent_version = await agent_versions_repository.create(
                AgentVersion(owner_id=owner.id, agent_id=agent.id)
            )
            await experiment_run_repository.create(
                ExperimentRun(
                    owner_id=owner.id,
                    experiment_id=experiment.id,
                    number=1,
                    cohort_version_id=cohort_version_id,
                    agent_version_id=agent_version.id,
                )
            )

        yield (
            SQLCohortVersionRepository(session),
            sessions_repository,
            owner.id,
            cohort.id,
            make_session_id,
            attach_experiment_run,
            SQLTagRepository(session),
        )


async def test_create_sets_version_and_timestamps(setup: Setup) -> None:
    """Assign version 1 and both timestamps to the first version."""
    repository, _, owner_id, cohort_id, make_session_id, _, _ = setup
    session_ids = [await make_session_id(), await make_session_id()]
    version = await repository.create(
        CohortVersion(
            owner_id=owner_id,
            cohort_id=cohort_id,
            display_version="v1",
            session_count=len(session_ids),
        ),
        session_ids,
    )
    assert version.cohort_id == cohort_id
    assert version.owner_id == owner_id
    assert version.version == 1
    assert version.display_version == "v1"
    assert version.session_count == 2
    assert version.created is not None
    assert version.updated is not None


async def test_create_numbers_versions_sequentially(setup: Setup) -> None:
    """Assign consecutive version numbers per cohort."""
    repository, _, owner_id, cohort_id, _, _, _ = setup
    first = await repository.create(
        CohortVersion(owner_id=owner_id, cohort_id=cohort_id, session_count=0), []
    )
    second = await repository.create(
        CohortVersion(owner_id=owner_id, cohort_id=cohort_id, session_count=0), []
    )
    third = await repository.create(
        CohortVersion(owner_id=owner_id, cohort_id=cohort_id, session_count=0), []
    )
    assert [first.version, second.version, third.version] == [1, 2, 3]


async def test_create_missing_cohort(setup: Setup) -> None:
    """Raise when the cohort does not exist."""
    repository, _, owner_id, _, _, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(CohortNotFound, match=f"Cohort {missing_id} was not found"):
        await repository.create(
            CohortVersion(owner_id=owner_id, cohort_id=missing_id, session_count=0), []
        )


async def test_get(setup: Setup) -> None:
    """Load a stored cohort version by id."""
    repository, _, owner_id, cohort_id, _, _, _ = setup
    created = await repository.create(
        CohortVersion(owner_id=owner_id, cohort_id=cohort_id, session_count=0), []
    )
    loaded = await repository.get(created.id)
    assert loaded == created


async def test_get_not_found(setup: Setup) -> None:
    """Raise for an unknown cohort version id."""
    repository, _, _, _, _, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(
        CohortVersionIdNotFound, match=f"Cohort version {missing_id} was not found"
    ):
        await repository.get(missing_id)


async def test_get_by_number(setup: Setup) -> None:
    """Load a stored cohort version by cohort id and version number."""
    repository, _, owner_id, cohort_id, _, _, _ = setup
    created = await repository.create(
        CohortVersion(owner_id=owner_id, cohort_id=cohort_id, session_count=0), []
    )
    loaded = await repository.get_by_number(cohort_id, created.version)
    assert loaded == created


async def test_get_by_number_not_found(setup: Setup) -> None:
    """Raise for an unknown version number."""
    repository, _, _, cohort_id, _, _, _ = setup
    with pytest.raises(
        CohortVersionNotFound, match=f"Version 1 of cohort {cohort_id} was not found"
    ):
        await repository.get_by_number(cohort_id, 1)


async def test_query_scoped_to_cohort(setup: Setup) -> None:
    """Query only the versions of the requested cohort, newest-first."""
    repository, _, owner_id, cohort_id, _, _, _ = setup
    v1 = await repository.create(
        CohortVersion(owner_id=owner_id, cohort_id=cohort_id, session_count=0), []
    )
    v2 = await repository.create(
        CohortVersion(owner_id=owner_id, cohort_id=cohort_id, session_count=0), []
    )
    versions, next_cursor = await repository.query(
        CohortVersionFilter(cohort_id=cohort_id)
    )
    assert next_cursor is None
    assert [version.id for version in versions] == [v2.id, v1.id]


async def test_query_filters_by_tag(setup: Setup) -> None:
    """Filter cohort versions linked to a tag through tag_link."""
    repository, _, owner_id, cohort_id, _, _, tags = setup
    tagged = await repository.create(
        CohortVersion(owner_id=owner_id, cohort_id=cohort_id, session_count=0), []
    )
    await repository.create(
        CohortVersion(owner_id=owner_id, cohort_id=cohort_id, session_count=0), []
    )
    tag = await tags.create(Tag(owner_id=owner_id, name="smoke-test"))
    await tags.create_link(
        TagLink(
            tag_id=tag.id,
            resource_type=TagResourceType.COHORT_VERSION,
            resource_id=tagged.id,
        )
    )
    versions, _ = await repository.query(
        CohortVersionFilter(
            cohort_id=cohort_id,
            expression=FilterCondition(field="tag", op=FilterOp.EQ, value="smoke-test"),
        )
    )
    assert [version.id for version in versions] == [tagged.id]


async def test_query_walks_pages(setup: Setup) -> None:
    """Walk every page via next_cursor without duplicates or gaps."""
    repository, _, owner_id, cohort_id, _, _, _ = setup
    created = [
        await repository.create(
            CohortVersion(owner_id=owner_id, cohort_id=cohort_id, session_count=0), []
        )
        for _ in range(5)
    ]
    expected_order = list(reversed(created))

    collected: list[CohortVersion] = []
    cursor = None
    while True:
        versions, next_cursor = await repository.query(
            CohortVersionFilter(cohort_id=cohort_id, cursor=cursor, size=2)
        )
        collected.extend(versions)
        if next_cursor is None:
            break
        cursor = next_cursor

    assert collected == expected_order
    assert len({version.id for version in collected}) == 5


async def test_list_session_ids_preserves_order(setup: Setup) -> None:
    """List a version's member session ids in the order they were given."""
    repository, _, owner_id, cohort_id, make_session_id, _, _ = setup
    session_ids = [
        await make_session_id(),
        await make_session_id(),
        await make_session_id(),
    ]
    created = await repository.create(
        CohortVersion(
            owner_id=owner_id, cohort_id=cohort_id, session_count=len(session_ids)
        ),
        session_ids,
    )
    listed = await repository.list_session_ids(created.id)
    assert listed == session_ids


async def test_list_session_ids_empty_version(setup: Setup) -> None:
    """List an empty member list for a version without members."""
    repository, _, owner_id, cohort_id, _, _, _ = setup
    created = await repository.create(
        CohortVersion(owner_id=owner_id, cohort_id=cohort_id, session_count=0), []
    )
    assert await repository.list_session_ids(created.id) == []


async def test_list_session_ids_not_found(setup: Setup) -> None:
    """Raise for an unknown cohort version id."""
    repository, _, _, _, _, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(
        CohortVersionIdNotFound, match=f"Cohort version {missing_id} was not found"
    ):
        await repository.list_session_ids(missing_id)


async def test_update(setup: Setup) -> None:
    """Persist a display version change and renew the updated timestamp."""
    repository, _, owner_id, cohort_id, _, _, _ = setup
    created = await repository.create(
        CohortVersion(
            owner_id=owner_id,
            cohort_id=cohort_id,
            display_version="v1",
            session_count=0,
        ),
        [],
    )
    created.update_display_version("v1.1")
    updated = await repository.update(created)
    assert updated.display_version == "v1.1"
    assert updated.created == created.created
    assert updated.updated is not None
    assert created.updated is not None
    assert updated.updated >= created.updated
    loaded = await repository.get(created.id)
    assert loaded == updated


async def test_update_not_found(setup: Setup) -> None:
    """Raise for an unknown cohort version id."""
    repository, _, owner_id, cohort_id, _, _, _ = setup
    version = CohortVersion(owner_id=owner_id, cohort_id=cohort_id, session_count=0)
    with pytest.raises(
        CohortVersionIdNotFound, match=f"Cohort version {version.id} was not found"
    ):
        await repository.update(version)


async def test_delete(setup: Setup) -> None:
    """Delete a stored cohort version."""
    repository, _, owner_id, cohort_id, _, _, _ = setup
    created = await repository.create(
        CohortVersion(owner_id=owner_id, cohort_id=cohort_id, session_count=0), []
    )
    await repository.delete(created.id)
    with pytest.raises(CohortVersionIdNotFound):
        await repository.get(created.id)


async def test_delete_not_found(setup: Setup) -> None:
    """Raise for an unknown cohort version id."""
    repository, _, _, _, _, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(
        CohortVersionIdNotFound, match=f"Cohort version {missing_id} was not found"
    ):
        await repository.delete(missing_id)


async def test_delete_in_use(setup: Setup) -> None:
    """Reject deleting a version referenced by an experiment run."""
    repository, _, owner_id, cohort_id, _, attach_experiment_run, _ = setup
    created = await repository.create(
        CohortVersion(owner_id=owner_id, cohort_id=cohort_id, session_count=0), []
    )
    await attach_experiment_run(created.id)
    with pytest.raises(CohortVersionInUse, match=f"{created.id}"):
        await repository.delete(created.id)


async def test_delete_frees_member_session(setup: Setup) -> None:
    """Free a member session for deletion once its cohort version is gone."""
    repository, sessions, owner_id, cohort_id, make_session_id, _, _ = setup
    session_id = await make_session_id()
    created = await repository.create(
        CohortVersion(owner_id=owner_id, cohort_id=cohort_id, session_count=1),
        [session_id],
    )
    await repository.delete(created.id)
    await sessions.delete(session_id)


async def test_session_in_cohort_version_delete_conflict(setup: Setup) -> None:
    """Reject deleting a session that belongs to a cohort version."""
    repository, sessions, owner_id, cohort_id, make_session_id, _, _ = setup
    session_id = await make_session_id()
    await repository.create(
        CohortVersion(owner_id=owner_id, cohort_id=cohort_id, session_count=1),
        [session_id],
    )
    with pytest.raises(
        SessionInUse,
        match=f"Session {session_id} belongs to a cohort version",
    ):
        await sessions.delete(session_id)
