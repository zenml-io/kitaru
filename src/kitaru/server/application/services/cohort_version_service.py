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
"""Cohort version use cases."""

import uuid
from collections.abc import Sequence

from kitaru.analytics.events import AnalyticsEvent
from kitaru.server.application.interfaces.cohort_repository import CohortRepository
from kitaru.server.application.interfaces.cohort_version_repository import (
    CohortVersionRepository,
)
from kitaru.server.application.interfaces.session_repository import SessionRepository
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.cohort import (
    CohortVersionCreate,
    CohortVersionFilter,
    CohortVersionUpdate,
)
from kitaru.server.application.services import analytics_events
from kitaru.server.application.services.server_analytics import ServerAnalytics
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.cohort import Cohort
from kitaru.server.domain.cohort_version import CohortVersion, apply_membership_delta


class CohortVersionService:
    """Cohort version use cases."""

    def __init__(
        self,
        repository: CohortVersionRepository,
        cohort_repository: CohortRepository,
        session_repository: SessionRepository,
        analytics: ServerAnalytics | None = None,
    ) -> None:
        """Initialize the service.

        Args:
            repository: Cohort version repository.
            cohort_repository: Cohort repository, to resolve the parent
                cohort and its latest version.
            session_repository: Session repository, to validate added
                sessions exist and belong to the cohort's agent.
            analytics: Analytics tracker, None skips tracking.
        """
        self._repository = repository
        self._cohorts = cohort_repository
        self._sessions = session_repository
        self._analytics = analytics

    async def _resolve_base_members(
        self, cohort: Cohort, baseline_id: uuid.UUID | None
    ) -> list[uuid.UUID]:
        """Read the ordered member list a membership delta applies to.

        Args:
            cohort: Cohort the new version belongs to.
            baseline_id: Id of the base version, None reads the latest
                version.

        Raises:
            CohortVersionIdNotFound: No cohort version has the baseline id.
            ValidationError: The baseline belongs to a different cohort.

        Returns:
            Ordered member session ids, empty when the cohort has no
            version yet.
        """
        if baseline_id is None:
            if cohort.latest_version == 0:
                return []
            baseline = await self._repository.get_by_number(
                cohort.id, cohort.latest_version
            )
        else:
            baseline = await self._repository.get(baseline_id)
            if baseline.cohort_id != cohort.id:
                raise ValidationError(
                    f"Cohort version {baseline_id} does not belong to cohort "
                    f"{cohort.id}"
                )
        return await self._repository.list_session_ids(baseline.id)

    async def _validate_added_sessions(
        self, session_ids: Sequence[uuid.UUID], agent_id: uuid.UUID
    ) -> None:
        """Validate added sessions exist and belong to the cohort's agent.

        Args:
            session_ids: Session ids being added to the new version.
            agent_id: Id of the cohort's agent.

        Raises:
            ValidationError: A session is missing or belongs to a different
                agent.
        """
        sessions_by_id = await self._sessions.get_many(session_ids)
        for session_id in session_ids:
            session = sessions_by_id.get(session_id)
            if session is None:
                raise ValidationError(f"Session {session_id} was not found")
            if session.agent_id != agent_id:
                raise ValidationError(
                    f"Session {session_id} does not belong to agent {agent_id}"
                )

    async def create_version(
        self, cohort_id: uuid.UUID, command: CohortVersionCreate, actor: AuthContext
    ) -> CohortVersion:
        """Create a cohort version from a membership delta on a base version.

        The delta applies to the command's baseline version, or to the
        latest version when the command has no baseline. The new member
        list is the base version's list minus ``remove_session_ids`` plus
        ``add_session_ids`` appended, or an empty list when the cohort has
        no version yet.

        Args:
            cohort_id: Id of the cohort this version belongs to.
            command: Baseline, membership delta, and display version for
                the new version.
            actor: Caller context.

        Raises:
            CohortNotFound: No cohort has this id.
            CohortVersionIdNotFound: No cohort version has the command's
                baseline id.
            ValidationError: The baseline belongs to a different cohort,
                the delta removes a session absent from the base version,
                adds a session already present, repeats a session id, or an
                added session is missing or belongs to a different agent.

        Returns:
            Created cohort version.
        """
        cohort = await self._cohorts.get(cohort_id)
        base_members = await self._resolve_base_members(cohort, command.baseline_id)
        new_members = apply_membership_delta(
            base_members, command.add_session_ids, command.remove_session_ids
        )
        cohort.check_members(new_members)
        await self._validate_added_sessions(command.add_session_ids, cohort.agent_id)
        version = CohortVersion(
            owner_id=actor.account.id,
            cohort_id=cohort.id,
            display_version=command.display_version,
            session_count=len(new_members),
        )
        version = await self._repository.create(version, new_members)
        if self._analytics is not None:
            self._analytics.track(
                actor.account.id,
                AnalyticsEvent.COHORT_VERSION_CREATED,
                analytics_events.build_cohort_version_created_properties(
                    version.session_count
                ),
            )
        return version

    async def get_version(
        self, cohort_version_id: uuid.UUID, actor: AuthContext
    ) -> CohortVersion:
        """Get a cohort version by id.

        Args:
            cohort_version_id: Id of the cohort version.
            actor: Caller context.

        Raises:
            CohortVersionIdNotFound: No cohort version has this id.

        Returns:
            Stored cohort version.
        """
        _ = actor
        return await self._repository.get(cohort_version_id)

    async def list_versions(
        self, version_filter: CohortVersionFilter, actor: AuthContext
    ) -> tuple[list[CohortVersion], str | None]:
        """List cohort versions matching a filter.

        Args:
            version_filter: Filter and pagination parameters.
            actor: Caller context.

        Raises:
            CohortNotFound: No cohort has the filter's cohort id.

        Returns:
            Page of matching cohort versions and the next cursor.
        """
        _ = actor
        await self._cohorts.get(version_filter.cohort_id)
        return await self._repository.query(version_filter)

    async def update_version(
        self,
        cohort_version_id: uuid.UUID,
        command: CohortVersionUpdate,
        actor: AuthContext,
    ) -> CohortVersion:
        """Partially update a cohort version's display version.

        Args:
            cohort_version_id: Id of the cohort version.
            command: Fields to change, built from the request's set fields.
            actor: Caller context.

        Raises:
            CohortVersionIdNotFound: No cohort version has this id.

        Returns:
            Updated cohort version.
        """
        _ = actor
        version = await self._repository.get(cohort_version_id)
        if "display_version" in command.model_fields_set:
            version.update_display_version(command.display_version)
        return await self._repository.update(version)

    async def delete_version(
        self, cohort_version_id: uuid.UUID, actor: AuthContext
    ) -> None:
        """Delete a cohort version.

        Deleting a version does not renumber or lower the cohort's
        latest_version high-water mark.

        Args:
            cohort_version_id: Id of the cohort version.
            actor: Caller context.

        Raises:
            CohortVersionIdNotFound: No cohort version has this id.
            CohortVersionInUse: An experiment run references this version.
        """
        _ = actor
        await self._repository.delete(cohort_version_id)
