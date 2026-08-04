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
"""Tests for cohort version use cases."""

import uuid
from typing import Any

import pytest

from conftest import (
    FakeAgentRepository,
    FakeCohortRepository,
    FakeCohortVersionRepository,
    FakeExperimentRunRepository,
    FakeSessionRepository,
    create_agent,
    create_cohort,
    create_experiment_run,
    create_session,
)
from kitaru.analytics.events import AnalyticsEvent
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.cohort import (
    CohortVersionCreate,
    CohortVersionFilter,
    CohortVersionUpdate,
)
from kitaru.server.application.services.cohort_version_service import (
    CohortVersionService,
)
from kitaru.server.application.services.server_analytics import ServerAnalytics
from kitaru.server.domain.account import Account
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.cohort import CohortNotFound
from kitaru.server.domain.cohort_version import (
    CohortVersionIdNotFound,
    CohortVersionInUse,
)
from kitaru.server.domain.names import InvalidName

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="ann"))


class _RecordingAnalytics(ServerAnalytics):
    """Analytics tracker recording track calls instead of buffering them."""

    def __init__(self) -> None:
        """Initialize the tracker."""
        self.tracked: list[tuple[uuid.UUID, AnalyticsEvent | str, dict[str, Any]]] = []

    def track(
        self,
        user_id: uuid.UUID,
        event: AnalyticsEvent | str,
        properties: dict[str, Any] | None = None,
    ) -> None:
        """Record a track call instead of buffering it.

        Args:
            user_id: User id.
            event: Event name.
            properties: Event properties.
        """
        self.tracked.append((user_id, event, properties or {}))


@pytest.fixture
def agent_repository() -> FakeAgentRepository:
    """Provide a fake agent repository."""
    return FakeAgentRepository()


@pytest.fixture
def session_repository() -> FakeSessionRepository:
    """Provide a fake session repository."""
    return FakeSessionRepository()


@pytest.fixture
def cohort_repository() -> FakeCohortRepository:
    """Provide a fake cohort repository."""
    return FakeCohortRepository()


@pytest.fixture
def experiment_run_repository() -> FakeExperimentRunRepository:
    """Provide a fake experiment run repository, consulted by delete."""
    return FakeExperimentRunRepository()


@pytest.fixture
def repository(
    cohort_repository: FakeCohortRepository,
    session_repository: FakeSessionRepository,
    experiment_run_repository: FakeExperimentRunRepository,
) -> FakeCohortVersionRepository:
    """Provide a fake cohort version repository sharing the cohort backend."""
    return FakeCohortVersionRepository(
        cohort_repository, session_repository, experiment_runs=experiment_run_repository
    )


@pytest.fixture
def service(
    repository: FakeCohortVersionRepository,
    cohort_repository: FakeCohortRepository,
    session_repository: FakeSessionRepository,
) -> CohortVersionService:
    """Provide a cohort version service backed by fake repositories."""
    return CohortVersionService(
        repository=repository,
        cohort_repository=cohort_repository,
        session_repository=session_repository,
    )


@pytest.fixture
async def agent_id(agent_repository: FakeAgentRepository) -> uuid.UUID:
    """Provide an agent id owned by the actor."""
    agent = await create_agent(agent_repository, ACTOR.account.id)
    return agent.id


@pytest.fixture
async def cohort_id(
    cohort_repository: FakeCohortRepository, agent_id: uuid.UUID
) -> uuid.UUID:
    """Provide a cohort id owned by the actor."""
    cohort = await create_cohort(cohort_repository, ACTOR.account.id, agent_id)
    return cohort.id


async def _make_session_id(
    session_repository: FakeSessionRepository, agent_id: uuid.UUID
) -> uuid.UUID:
    """Store a session on the given agent and return its id."""
    session = await create_session(session_repository, ACTOR.account.id, agent_id)
    return session.id


async def test_create_first_version_from_empty_base(
    service: CohortVersionService,
    session_repository: FakeSessionRepository,
    agent_id: uuid.UUID,
    cohort_id: uuid.UUID,
) -> None:
    """Start the first version's member list empty when only removing."""
    session_ids = [
        await _make_session_id(session_repository, agent_id),
        await _make_session_id(session_repository, agent_id),
    ]
    version = await service.create_version(
        cohort_id,
        CohortVersionCreate(add_session_ids=session_ids, display_version="v1"),
        actor=ACTOR,
    )
    assert version.cohort_id == cohort_id
    assert version.owner_id == ACTOR.account.id
    assert version.version == 1
    assert version.display_version == "v1"
    assert version.session_count == 2
    assert version.created is not None
    assert version.updated is not None


async def test_create_version_numbering_across_creates(
    service: CohortVersionService,
    session_repository: FakeSessionRepository,
    agent_id: uuid.UUID,
    cohort_id: uuid.UUID,
) -> None:
    """Assign consecutive version numbers per cohort."""
    first = await service.create_version(cohort_id, CohortVersionCreate(), actor=ACTOR)
    second = await service.create_version(
        cohort_id,
        CohortVersionCreate(
            add_session_ids=[await _make_session_id(session_repository, agent_id)]
        ),
        actor=ACTOR,
    )
    assert first.version == 1
    assert second.version == 2


async def test_create_version_remove_only(
    service: CohortVersionService,
    session_repository: FakeSessionRepository,
    agent_id: uuid.UUID,
    cohort_id: uuid.UUID,
) -> None:
    """Drop a member from the base version's list."""
    session_ids = [
        await _make_session_id(session_repository, agent_id),
        await _make_session_id(session_repository, agent_id),
    ]
    base = await service.create_version(
        cohort_id, CohortVersionCreate(add_session_ids=session_ids), actor=ACTOR
    )
    removed = await service.create_version(
        cohort_id,
        CohortVersionCreate(remove_session_ids=[session_ids[0]]),
        actor=ACTOR,
    )
    assert removed.session_count == 1
    _ = base


async def test_create_version_add_and_remove(
    service: CohortVersionService,
    session_repository: FakeSessionRepository,
    agent_id: uuid.UUID,
    cohort_id: uuid.UUID,
) -> None:
    """Apply an add and a remove in the same delta."""
    kept_id = await _make_session_id(session_repository, agent_id)
    removed_id = await _make_session_id(session_repository, agent_id)
    await service.create_version(
        cohort_id,
        CohortVersionCreate(add_session_ids=[kept_id, removed_id]),
        actor=ACTOR,
    )
    added_id = await _make_session_id(session_repository, agent_id)
    version = await service.create_version(
        cohort_id,
        CohortVersionCreate(
            add_session_ids=[added_id], remove_session_ids=[removed_id]
        ),
        actor=ACTOR,
    )
    assert version.session_count == 2


async def test_create_version_from_baseline(
    service: CohortVersionService,
    repository: FakeCohortVersionRepository,
    session_repository: FakeSessionRepository,
    agent_id: uuid.UUID,
    cohort_id: uuid.UUID,
) -> None:
    """Restore an older version's member list via a baseline with no delta."""
    session_ids = [
        await _make_session_id(session_repository, agent_id),
        await _make_session_id(session_repository, agent_id),
    ]
    baseline = await service.create_version(
        cohort_id, CohortVersionCreate(add_session_ids=session_ids), actor=ACTOR
    )
    await service.create_version(
        cohort_id,
        CohortVersionCreate(remove_session_ids=[session_ids[0]]),
        actor=ACTOR,
    )
    restored = await service.create_version(
        cohort_id, CohortVersionCreate(baseline_id=baseline.id), actor=ACTOR
    )
    assert restored.version == 3
    assert restored.session_count == 2
    assert await repository.list_session_ids(restored.id) == session_ids


async def test_create_version_from_baseline_with_delta(
    service: CohortVersionService,
    repository: FakeCohortVersionRepository,
    session_repository: FakeSessionRepository,
    agent_id: uuid.UUID,
    cohort_id: uuid.UUID,
) -> None:
    """Apply the delta to the baseline's members instead of the latest's."""
    first_id = await _make_session_id(session_repository, agent_id)
    baseline = await service.create_version(
        cohort_id, CohortVersionCreate(add_session_ids=[first_id]), actor=ACTOR
    )
    second_id = await _make_session_id(session_repository, agent_id)
    await service.create_version(
        cohort_id, CohortVersionCreate(add_session_ids=[second_id]), actor=ACTOR
    )
    added_id = await _make_session_id(session_repository, agent_id)
    version = await service.create_version(
        cohort_id,
        CohortVersionCreate(
            baseline_id=baseline.id,
            add_session_ids=[added_id],
            remove_session_ids=[first_id],
        ),
        actor=ACTOR,
    )
    assert version.session_count == 1
    assert await repository.list_session_ids(version.id) == [added_id]


async def test_create_version_baseline_delta_checked_against_baseline(
    service: CohortVersionService,
    session_repository: FakeSessionRepository,
    agent_id: uuid.UUID,
    cohort_id: uuid.UUID,
) -> None:
    """Reject removing a session in the latest version but not the baseline."""
    first_id = await _make_session_id(session_repository, agent_id)
    baseline = await service.create_version(
        cohort_id, CohortVersionCreate(add_session_ids=[first_id]), actor=ACTOR
    )
    second_id = await _make_session_id(session_repository, agent_id)
    await service.create_version(
        cohort_id, CohortVersionCreate(add_session_ids=[second_id]), actor=ACTOR
    )
    with pytest.raises(
        ValidationError, match="Cannot remove a session that is not in the base version"
    ):
        await service.create_version(
            cohort_id,
            CohortVersionCreate(
                baseline_id=baseline.id, remove_session_ids=[second_id]
            ),
            actor=ACTOR,
        )


async def test_create_version_baseline_not_found(
    service: CohortVersionService, cohort_id: uuid.UUID
) -> None:
    """Raise for an unknown baseline id."""
    missing_id = uuid.uuid4()
    with pytest.raises(
        CohortVersionIdNotFound, match=f"Cohort version {missing_id} was not found"
    ):
        await service.create_version(
            cohort_id, CohortVersionCreate(baseline_id=missing_id), actor=ACTOR
        )


async def test_create_version_baseline_wrong_cohort(
    service: CohortVersionService,
    cohort_repository: FakeCohortRepository,
    agent_id: uuid.UUID,
    cohort_id: uuid.UUID,
) -> None:
    """Reject a baseline that belongs to a different cohort."""
    other_cohort = await create_cohort(
        cohort_repository, ACTOR.account.id, agent_id, name="other"
    )
    foreign = await service.create_version(
        other_cohort.id, CohortVersionCreate(), actor=ACTOR
    )
    with pytest.raises(
        ValidationError,
        match=f"Cohort version {foreign.id} does not belong to cohort {cohort_id}",
    ):
        await service.create_version(
            cohort_id, CohortVersionCreate(baseline_id=foreign.id), actor=ACTOR
        )


async def test_create_version_remove_nonmember(
    service: CohortVersionService, cohort_id: uuid.UUID
) -> None:
    """Reject removing a session absent from the base version."""
    with pytest.raises(
        ValidationError, match="Cannot remove a session that is not in the base version"
    ):
        await service.create_version(
            cohort_id,
            CohortVersionCreate(remove_session_ids=[uuid.uuid4()]),
            actor=ACTOR,
        )


async def test_create_version_add_duplicate_member(
    service: CohortVersionService,
    session_repository: FakeSessionRepository,
    agent_id: uuid.UUID,
    cohort_id: uuid.UUID,
) -> None:
    """Reject adding a session already present in the base version."""
    session_id = await _make_session_id(session_repository, agent_id)
    await service.create_version(
        cohort_id, CohortVersionCreate(add_session_ids=[session_id]), actor=ACTOR
    )
    with pytest.raises(
        ValidationError,
        match="Cannot add a session that is already in the base version",
    ):
        await service.create_version(
            cohort_id,
            CohortVersionCreate(add_session_ids=[session_id]),
            actor=ACTOR,
        )


async def test_create_version_add_list_duplicate_ids(
    service: CohortVersionService,
    session_repository: FakeSessionRepository,
    agent_id: uuid.UUID,
    cohort_id: uuid.UUID,
) -> None:
    """Reject a repeated session id inside the add list."""
    session_id = await _make_session_id(session_repository, agent_id)
    with pytest.raises(
        ValidationError, match="Add list contains a duplicate session id"
    ):
        await service.create_version(
            cohort_id,
            CohortVersionCreate(add_session_ids=[session_id, session_id]),
            actor=ACTOR,
        )


async def test_create_version_remove_list_duplicate_ids(
    service: CohortVersionService,
    session_repository: FakeSessionRepository,
    agent_id: uuid.UUID,
    cohort_id: uuid.UUID,
) -> None:
    """Reject a repeated session id inside the remove list."""
    session_id = await _make_session_id(session_repository, agent_id)
    await service.create_version(
        cohort_id, CohortVersionCreate(add_session_ids=[session_id]), actor=ACTOR
    )
    with pytest.raises(
        ValidationError, match="Remove list contains a duplicate session id"
    ):
        await service.create_version(
            cohort_id,
            CohortVersionCreate(remove_session_ids=[session_id, session_id]),
            actor=ACTOR,
        )


async def test_create_version_added_session_missing(
    service: CohortVersionService, cohort_id: uuid.UUID
) -> None:
    """Reject an added session that does not exist."""
    missing_session_id = uuid.uuid4()
    with pytest.raises(
        ValidationError, match=f"Session {missing_session_id} was not found"
    ):
        await service.create_version(
            cohort_id,
            CohortVersionCreate(add_session_ids=[missing_session_id]),
            actor=ACTOR,
        )


async def test_create_version_added_session_wrong_agent(
    service: CohortVersionService,
    session_repository: FakeSessionRepository,
    agent_repository: FakeAgentRepository,
    agent_id: uuid.UUID,
    cohort_id: uuid.UUID,
) -> None:
    """Reject an added session that belongs to a different agent."""
    other_agent = await create_agent(agent_repository, ACTOR.account.id, name="other")
    foreign_session_id = await _make_session_id(session_repository, other_agent.id)
    with pytest.raises(
        ValidationError, match=f"Session {foreign_session_id} does not belong"
    ):
        await service.create_version(
            cohort_id,
            CohortVersionCreate(add_session_ids=[foreign_session_id]),
            actor=ACTOR,
        )


async def test_create_version_missing_cohort(service: CohortVersionService) -> None:
    """Raise when the cohort does not exist."""
    missing_id = uuid.uuid4()
    with pytest.raises(CohortNotFound, match=f"Cohort {missing_id} was not found"):
        await service.create_version(missing_id, CohortVersionCreate(), actor=ACTOR)


@pytest.mark.parametrize("display_version", ["v1", "1.2.3+build.5", "feature/branch-1"])
async def test_create_version_display_version_accepted(
    service: CohortVersionService, cohort_id: uuid.UUID, display_version: str
) -> None:
    """Accept semver and branch-style display versions."""
    version = await service.create_version(
        cohort_id,
        CohortVersionCreate(display_version=display_version),
        actor=ACTOR,
    )
    assert version.display_version == display_version


async def test_create_version_invalid_display_version_rejected(
    service: CohortVersionService, cohort_id: uuid.UUID
) -> None:
    """Reject a display version with a leading separator."""
    with pytest.raises(InvalidName):
        await service.create_version(
            cohort_id, CohortVersionCreate(display_version="/bad"), actor=ACTOR
        )


async def test_get_version(service: CohortVersionService, cohort_id: uuid.UUID) -> None:
    """Load a stored cohort version by id."""
    created = await service.create_version(
        cohort_id, CohortVersionCreate(), actor=ACTOR
    )
    loaded = await service.get_version(created.id, actor=ACTOR)
    assert loaded == created


async def test_get_version_not_found(service: CohortVersionService) -> None:
    """Raise for an unknown cohort version id."""
    missing_id = uuid.uuid4()
    with pytest.raises(
        CohortVersionIdNotFound, match=f"Cohort version {missing_id} was not found"
    ):
        await service.get_version(missing_id, actor=ACTOR)


async def test_list_versions(
    service: CohortVersionService,
    cohort_repository: FakeCohortRepository,
    agent_id: uuid.UUID,
    cohort_id: uuid.UUID,
) -> None:
    """List only the versions of the requested cohort, newest-first."""
    other_cohort = await create_cohort(
        cohort_repository, ACTOR.account.id, agent_id, name="other"
    )
    v1 = await service.create_version(cohort_id, CohortVersionCreate(), actor=ACTOR)
    v2 = await service.create_version(cohort_id, CohortVersionCreate(), actor=ACTOR)
    await service.create_version(other_cohort.id, CohortVersionCreate(), actor=ACTOR)

    versions, next_cursor = await service.list_versions(
        CohortVersionFilter(cohort_id=cohort_id), actor=ACTOR
    )
    assert next_cursor is None
    assert [version.id for version in versions] == [v2.id, v1.id]


async def test_list_versions_missing_cohort(service: CohortVersionService) -> None:
    """Raise when the filter names a cohort that does not exist."""
    missing_id = uuid.uuid4()
    with pytest.raises(CohortNotFound, match=f"Cohort {missing_id} was not found"):
        await service.list_versions(
            CohortVersionFilter(cohort_id=missing_id), actor=ACTOR
        )


async def test_list_versions_walks_pages(
    service: CohortVersionService, cohort_id: uuid.UUID
) -> None:
    """Walk every page of a cohort's versions via next_cursor."""
    created = [
        await service.create_version(cohort_id, CohortVersionCreate(), actor=ACTOR)
        for _ in range(3)
    ]
    expected_order = list(reversed([version.id for version in created]))

    collected: list[uuid.UUID] = []
    cursor = None
    while True:
        versions, next_cursor = await service.list_versions(
            CohortVersionFilter(cohort_id=cohort_id, cursor=cursor, size=1),
            actor=ACTOR,
        )
        collected.extend(version.id for version in versions)
        if next_cursor is None:
            break
        cursor = next_cursor

    assert collected == expected_order


async def test_update_version_display_version(
    service: CohortVersionService, cohort_id: uuid.UUID
) -> None:
    """Update a version's display version."""
    created = await service.create_version(
        cohort_id, CohortVersionCreate(display_version="v1"), actor=ACTOR
    )
    updated = await service.update_version(
        created.id, CohortVersionUpdate(display_version="v1.1"), actor=ACTOR
    )
    assert updated.display_version == "v1.1"
    assert updated.updated is not None
    assert created.updated is not None
    assert updated.updated > created.updated


async def test_update_version_clears_display_version(
    service: CohortVersionService, cohort_id: uuid.UUID
) -> None:
    """Clear the display version with an explicit null."""
    created = await service.create_version(
        cohort_id, CohortVersionCreate(display_version="v1"), actor=ACTOR
    )
    updated = await service.update_version(
        created.id, CohortVersionUpdate(display_version=None), actor=ACTOR
    )
    assert updated.display_version is None


async def test_update_version_omitted_fields_unchanged(
    service: CohortVersionService, cohort_id: uuid.UUID
) -> None:
    """Leave the display version unchanged when the command sets none of it."""
    created = await service.create_version(
        cohort_id, CohortVersionCreate(display_version="v1"), actor=ACTOR
    )
    updated = await service.update_version(
        created.id, CohortVersionUpdate(), actor=ACTOR
    )
    assert updated.display_version == "v1"


async def test_update_version_invalid_display_version_rejected(
    service: CohortVersionService, cohort_id: uuid.UUID
) -> None:
    """Reject a display version containing a space."""
    created = await service.create_version(
        cohort_id, CohortVersionCreate(), actor=ACTOR
    )
    with pytest.raises(InvalidName):
        await service.update_version(
            created.id, CohortVersionUpdate(display_version="bad name"), actor=ACTOR
        )


async def test_update_version_not_found(service: CohortVersionService) -> None:
    """Raise for an unknown cohort version id."""
    with pytest.raises(CohortVersionIdNotFound):
        await service.update_version(
            uuid.uuid4(), CohortVersionUpdate(display_version="v2"), actor=ACTOR
        )


async def test_delete_version(
    service: CohortVersionService, cohort_id: uuid.UUID
) -> None:
    """Delete a stored cohort version."""
    created = await service.create_version(
        cohort_id, CohortVersionCreate(), actor=ACTOR
    )
    await service.delete_version(created.id, actor=ACTOR)
    with pytest.raises(CohortVersionIdNotFound):
        await service.get_version(created.id, actor=ACTOR)


async def test_delete_version_not_found(service: CohortVersionService) -> None:
    """Raise for an unknown cohort version id."""
    with pytest.raises(CohortVersionIdNotFound):
        await service.delete_version(uuid.uuid4(), actor=ACTOR)


async def test_delete_version_does_not_lower_latest_version(
    service: CohortVersionService,
    cohort_repository: FakeCohortRepository,
    cohort_id: uuid.UUID,
) -> None:
    """Keep the cohort's latest_version high-water mark after a delete."""
    await service.create_version(cohort_id, CohortVersionCreate(), actor=ACTOR)
    second = await service.create_version(cohort_id, CohortVersionCreate(), actor=ACTOR)
    await service.delete_version(second.id, actor=ACTOR)
    cohort = await cohort_repository.get(cohort_id)
    assert cohort.latest_version == 2


async def test_delete_version_in_use(
    service: CohortVersionService,
    experiment_run_repository: FakeExperimentRunRepository,
    cohort_id: uuid.UUID,
) -> None:
    """Reject deleting a version referenced by an experiment run."""
    created = await service.create_version(
        cohort_id, CohortVersionCreate(), actor=ACTOR
    )
    await create_experiment_run(
        experiment_run_repository,
        ACTOR.account.id,
        experiment_id=uuid.uuid4(),
        cohort_version_id=created.id,
        agent_version_id=uuid.uuid4(),
    )
    with pytest.raises(CohortVersionInUse):
        await service.delete_version(created.id, actor=ACTOR)


async def test_create_version_tracks_cohort_version_created(
    repository: FakeCohortVersionRepository,
    cohort_repository: FakeCohortRepository,
    session_repository: FakeSessionRepository,
    agent_id: uuid.UUID,
    cohort_id: uuid.UUID,
) -> None:
    """Fire COHORT_VERSION_CREATED with the new version's session count."""
    analytics = _RecordingAnalytics()
    service = CohortVersionService(
        repository=repository,
        cohort_repository=cohort_repository,
        session_repository=session_repository,
        analytics=analytics,
    )
    session_ids = [
        await _make_session_id(session_repository, agent_id),
        await _make_session_id(session_repository, agent_id),
    ]

    await service.create_version(
        cohort_id,
        CohortVersionCreate(add_session_ids=session_ids),
        actor=ACTOR,
    )

    assert len(analytics.tracked) == 1
    user_id, event, properties = analytics.tracked[0]
    assert user_id == ACTOR.account.id
    assert event == AnalyticsEvent.COHORT_VERSION_CREATED
    assert properties == {"session_count": 2}


async def test_create_version_without_analytics_tracker(
    service: CohortVersionService, cohort_id: uuid.UUID
) -> None:
    """Create a cohort version normally when no analytics tracker is configured."""
    version = await service.create_version(
        cohort_id, CohortVersionCreate(), actor=ACTOR
    )
    assert version.owner_id == ACTOR.account.id
