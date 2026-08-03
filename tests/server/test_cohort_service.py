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
"""Tests for cohort use cases."""

import uuid
from typing import Any

import pytest

from conftest import FakeAgentRepository, FakeCohortRepository, create_agent
from kitaru.analytics.events import AnalyticsEvent
from kitaru.api_models.v1.filter import FilterOp
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.cohort import (
    CohortCreate,
    CohortFilter,
    CohortUpdate,
)
from kitaru.server.application.services.cohort_service import CohortService
from kitaru.server.application.services.server_analytics import ServerAnalytics
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import AgentNotFound
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.cohort import CohortNotFound, DuplicateCohortName
from kitaru.server.filtering import FilterCondition

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
def cohort_repository() -> FakeCohortRepository:
    """Provide a fake cohort repository."""
    return FakeCohortRepository()


@pytest.fixture
def service(
    cohort_repository: FakeCohortRepository,
    agent_repository: FakeAgentRepository,
) -> CohortService:
    """Provide a cohort service backed by fake repositories."""
    return CohortService(
        repository=cohort_repository, agent_repository=agent_repository
    )


@pytest.fixture
async def agent_id(agent_repository: FakeAgentRepository) -> uuid.UUID:
    """Provide an agent id owned by the actor."""
    agent = await create_agent(agent_repository, ACTOR.account.id)
    return agent.id


async def test_create_cohort(service: CohortService, agent_id: uuid.UUID) -> None:
    """Create a cohort namespace owned by the caller."""
    cohort = await service.create_cohort(
        CohortCreate(
            name="smoke-test",
            description="A cohort",
            agent_id=agent_id,
            metadata={"team": "eval"},
        ),
        actor=ACTOR,
    )
    assert cohort.name == "smoke-test"
    assert cohort.description == "A cohort"
    assert cohort.agent_id == agent_id
    assert cohort.metadata == {"team": "eval"}
    assert cohort.latest_version == 0
    assert cohort.owner_id == ACTOR.account.id
    assert cohort.created is not None
    assert cohort.updated is not None


async def test_create_cohort_missing_agent(service: CohortService) -> None:
    """Raise when the agent does not exist."""
    missing_agent_id = uuid.uuid4()
    with pytest.raises(AgentNotFound):
        await service.create_cohort(
            CohortCreate(name="cohort", agent_id=missing_agent_id), actor=ACTOR
        )


async def test_create_cohort_duplicate_name(
    service: CohortService, agent_id: uuid.UUID
) -> None:
    """Reject a second cohort with the same name."""
    await service.create_cohort(
        CohortCreate(name="cohort", agent_id=agent_id), actor=ACTOR
    )
    with pytest.raises(DuplicateCohortName):
        await service.create_cohort(
            CohortCreate(name="cohort", agent_id=agent_id), actor=ACTOR
        )


async def test_get_cohort(service: CohortService, agent_id: uuid.UUID) -> None:
    """Load a stored cohort by id."""
    created = await service.create_cohort(
        CohortCreate(name="cohort", agent_id=agent_id), actor=ACTOR
    )
    loaded = await service.get_cohort(created.id, actor=ACTOR)
    assert loaded == created


async def test_get_cohort_not_found(service: CohortService) -> None:
    """Raise for an unknown cohort id."""
    missing_id = uuid.uuid4()
    with pytest.raises(CohortNotFound, match=f"Cohort {missing_id} was not found"):
        await service.get_cohort(missing_id, actor=ACTOR)


async def test_list_cohorts(service: CohortService, agent_id: uuid.UUID) -> None:
    """List cohorts newest-first with a name filter."""
    for name in ["alpha", "beta"]:
        await service.create_cohort(
            CohortCreate(name=name, agent_id=agent_id), actor=ACTOR
        )

    cohorts, next_cursor = await service.list_cohorts(CohortFilter(), actor=ACTOR)
    assert next_cursor is None
    assert [cohort.name for cohort in cohorts] == ["beta", "alpha"]

    cohorts, next_cursor = await service.list_cohorts(
        CohortFilter(
            expression=FilterCondition(field="name", op=FilterOp.EQ, value="alpha")
        ),
        actor=ACTOR,
    )
    assert [cohort.name for cohort in cohorts] == ["alpha"]


async def test_update_cohort_name(service: CohortService, agent_id: uuid.UUID) -> None:
    """Update a cohort's name."""
    created = await service.create_cohort(
        CohortCreate(name="cohort", agent_id=agent_id), actor=ACTOR
    )
    updated = await service.update_cohort(
        created.id, CohortUpdate(name="renamed"), actor=ACTOR
    )
    assert updated.name == "renamed"
    assert updated.updated is not None
    assert created.updated is not None
    assert updated.updated > created.updated


async def test_update_cohort_description(
    service: CohortService, agent_id: uuid.UUID
) -> None:
    """Update a cohort's description without touching its name."""
    created = await service.create_cohort(
        CohortCreate(name="cohort", description="old", agent_id=agent_id), actor=ACTOR
    )
    updated = await service.update_cohort(
        created.id, CohortUpdate(description="new"), actor=ACTOR
    )
    assert updated.name == "cohort"
    assert updated.description == "new"


async def test_update_cohort_metadata(
    service: CohortService, agent_id: uuid.UUID
) -> None:
    """Update a cohort's metadata without touching its name."""
    created = await service.create_cohort(
        CohortCreate(name="cohort", agent_id=agent_id), actor=ACTOR
    )
    updated = await service.update_cohort(
        created.id, CohortUpdate(metadata={"team": "eval"}), actor=ACTOR
    )
    assert updated.name == "cohort"
    assert updated.metadata == {"team": "eval"}


async def test_update_cohort_omitted_fields_unchanged(
    service: CohortService, agent_id: uuid.UUID
) -> None:
    """Leave every field unchanged when the command sets none of it."""
    created = await service.create_cohort(
        CohortCreate(name="cohort", description="old", agent_id=agent_id), actor=ACTOR
    )
    updated = await service.update_cohort(created.id, CohortUpdate(), actor=ACTOR)
    assert updated.name == "cohort"
    assert updated.description == "old"


async def test_update_cohort_cannot_clear_name(
    service: CohortService, agent_id: uuid.UUID
) -> None:
    """Reject clearing the cohort name with an explicit null."""
    created = await service.create_cohort(
        CohortCreate(name="cohort", agent_id=agent_id), actor=ACTOR
    )
    with pytest.raises(ValidationError, match="Cohort name cannot be cleared"):
        await service.update_cohort(created.id, CohortUpdate(name=None), actor=ACTOR)


async def test_update_cohort_not_found(service: CohortService) -> None:
    """Raise for an unknown cohort id."""
    with pytest.raises(CohortNotFound):
        await service.update_cohort(uuid.uuid4(), CohortUpdate(name="x"), actor=ACTOR)


async def test_update_cohort_duplicate_name(
    service: CohortService, agent_id: uuid.UUID
) -> None:
    """Reject renaming a cohort to a registered name."""
    await service.create_cohort(
        CohortCreate(name="alpha", agent_id=agent_id), actor=ACTOR
    )
    other = await service.create_cohort(
        CohortCreate(name="beta", agent_id=agent_id), actor=ACTOR
    )
    with pytest.raises(DuplicateCohortName):
        await service.update_cohort(other.id, CohortUpdate(name="alpha"), actor=ACTOR)


async def test_delete_cohort(service: CohortService, agent_id: uuid.UUID) -> None:
    """Delete a stored cohort."""
    created = await service.create_cohort(
        CohortCreate(name="cohort", agent_id=agent_id), actor=ACTOR
    )
    await service.delete_cohort(created.id, actor=ACTOR)
    with pytest.raises(CohortNotFound):
        await service.get_cohort(created.id, actor=ACTOR)


async def test_delete_cohort_not_found(service: CohortService) -> None:
    """Raise for an unknown cohort id."""
    with pytest.raises(CohortNotFound):
        await service.delete_cohort(uuid.uuid4(), actor=ACTOR)


async def test_create_cohort_tracks_cohort_created(
    cohort_repository: FakeCohortRepository,
    agent_repository: FakeAgentRepository,
    agent_id: uuid.UUID,
) -> None:
    """Fire COHORT_CREATED with no properties beyond the commons."""
    analytics = _RecordingAnalytics()
    service = CohortService(
        repository=cohort_repository,
        agent_repository=agent_repository,
        analytics=analytics,
    )

    await service.create_cohort(
        CohortCreate(name="cohort", agent_id=agent_id), actor=ACTOR
    )

    assert len(analytics.tracked) == 1
    user_id, event, properties = analytics.tracked[0]
    assert user_id == ACTOR.account.id
    assert event == AnalyticsEvent.COHORT_CREATED
    assert properties == {}


async def test_create_cohort_without_analytics_tracker(
    service: CohortService, agent_id: uuid.UUID
) -> None:
    """Create a cohort normally when no analytics tracker is configured."""
    cohort = await service.create_cohort(
        CohortCreate(name="cohort", agent_id=agent_id), actor=ACTOR
    )
    assert cohort.owner_id == ACTOR.account.id
