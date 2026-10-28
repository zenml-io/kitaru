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
"""In-memory fakes for cohorts, cohort versions, and workers."""

import uuid
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.tag import TagResourceType
from kitaru.api_models.v1.worker import WorkerRuntime, WorkerScope
from kitaru.server.application.models.cohort import CohortFilter, CohortVersionFilter
from kitaru.server.application.models.worker import WorkerFilter
from kitaru.server.domain.cohort import (
    Cohort,
    CohortInUse,
    CohortNotFound,
    DuplicateCohortName,
)
from kitaru.server.domain.cohort_version import (
    CohortVersion,
    CohortVersionIdNotFound,
    CohortVersionInUse,
    CohortVersionNotFound,
)
from kitaru.server.domain.worker import Worker, WorkerNotFound
from kitaru.server.filtering import FilterCondition
from kitaru_test_support.common import (
    UNSCOPED_WORKER_SCOPE,
    _evaluate_filter_expression,
    _paginate_fake,
    _renewed_timestamp,
)
from kitaru_test_support.sessions import FakeSessionRepository, FakeTagRepository

# Named only in string annotations; a runtime import could be circular.
if TYPE_CHECKING:
    from kitaru_test_support.evaluations import FakeExperimentRunRepository


class FakeCohortRepository:
    """In-memory cohort repository."""

    def __init__(self, tags: FakeTagRepository | None = None) -> None:
        """Initialize the repository.

        Args:
            tags: Fake tag repository, consulted by the ``tag`` filter.
        """
        self._cohorts: dict[uuid.UUID, Cohort] = {}
        self._tags = tags
        # Wired back by FakeCohortVersionRepository, consulted by delete to
        # check for an in-use version.
        self._cohort_versions: FakeCohortVersionRepository | None = None

    def _check_duplicate_name(self, cohort: Cohort) -> None:
        for other in self._cohorts.values():
            if (
                other.id != cohort.id
                and other.agent_id == cohort.agent_id
                and other.name == cohort.name
            ):
                raise DuplicateCohortName(cohort.name)

    def increment_latest_version(self, cohort_id: uuid.UUID) -> int:
        """Bump and return the cohort's version counter.

        Mirrors the SQL repository's ``UPDATE ... RETURNING`` bump.

        Args:
            cohort_id: Id of the cohort to bump.

        Raises:
            CohortNotFound: No cohort has this id.

        Returns:
            New version number.
        """
        cohort = self._cohorts.get(cohort_id)
        if cohort is None:
            raise CohortNotFound(cohort_id)
        cohort.latest_version += 1
        return cohort.latest_version

    async def create(self, cohort: Cohort) -> Cohort:
        """Persist a new cohort.

        Args:
            cohort: Cohort to store.

        Raises:
            DuplicateCohortName: The cohort name is already registered.

        Returns:
            Stored cohort with timestamps set.
        """
        self._check_duplicate_name(cohort)
        now = datetime.now(UTC)
        stored = cohort.model_copy(update={"created": now, "updated": now})
        self._cohorts[stored.id] = stored
        return stored.model_copy()

    async def get(self, cohort_id: uuid.UUID, exclusive: bool = False) -> Cohort:
        """Load a cohort by id.

        Args:
            cohort_id: Id of the cohort.
            exclusive: Ignored, the fake holds no rows to lock.

        Raises:
            CohortNotFound: No cohort has this id.

        Returns:
            Stored cohort.
        """
        cohort = self._cohorts.get(cohort_id)
        if cohort is None:
            raise CohortNotFound(cohort_id)
        return cohort.model_copy()

    def _cohort_ids_tagged(self, tag_name: str) -> set[uuid.UUID]:
        """Resolve the ids of cohorts linked to a tag by name.

        Args:
            tag_name: Name of the tag to resolve.

        Returns:
            Ids of cohorts linked to the tag.
        """
        if self._tags is None:
            return set()
        tag_id = next(
            (tag.id for tag in self._tags._tags.values() if tag.name == tag_name),
            None,
        )
        if tag_id is None:
            return set()
        return {
            link.resource_id
            for link in self._tags._links.values()
            if link.tag_id == tag_id and link.resource_type == TagResourceType.COHORT
        }

    async def query(
        self, cohort_filter: CohortFilter
    ) -> tuple[list[Cohort], str | None]:
        """Query cohorts matching a filter.

        Args:
            cohort_filter: Filter and pagination parameters.

        Returns:
            Page of matching cohorts and the next cursor.
        """
        cohorts = list(self._cohorts.values())
        if cohort_filter.expression is not None:
            resolvers = {"tag": self._evaluate_tag_condition}
            cohorts = [
                c
                for c in cohorts
                if _evaluate_filter_expression(c, cohort_filter.expression, resolvers)
            ]
        page, next_cursor = _paginate_fake(cohorts, cohort_filter)
        return [c.model_copy() for c in page], next_cursor

    def _evaluate_tag_condition(
        self, cohort: Cohort, condition: FilterCondition
    ) -> bool:
        """Evaluate a tag filter condition against a cohort.

        Args:
            cohort: Cohort to evaluate.
            condition: Validated tag condition.

        Returns:
            Whether the cohort has a matching tag.
        """
        names = condition.value if condition.op is FilterOp.IN else (condition.value,)
        return any(cohort.id in self._cohort_ids_tagged(name) for name in names)

    async def update(self, cohort: Cohort) -> Cohort:
        """Persist changes to an existing cohort.

        Args:
            cohort: Cohort with modified fields.

        Raises:
            CohortNotFound: No cohort has this id.
            DuplicateCohortName: The cohort name is already registered.

        Returns:
            Stored cohort with the updated timestamp renewed.
        """
        stored = self._cohorts.get(cohort.id)
        if stored is None:
            raise CohortNotFound(cohort.id)
        self._check_duplicate_name(cohort)
        now = _renewed_timestamp(stored.updated)
        updated = cohort.model_copy(update={"created": stored.created, "updated": now})
        self._cohorts[cohort.id] = updated
        return updated.model_copy()

    async def delete(self, cohort_id: uuid.UUID) -> None:
        """Delete a cohort by id.

        Deleting a cohort cascades its versions.

        Args:
            cohort_id: Id of the cohort.

        Raises:
            CohortNotFound: No cohort has this id.
            CohortInUse: An experiment run references one of its versions.
        """
        if cohort_id not in self._cohorts:
            raise CohortNotFound(cohort_id)
        if self._cohort_versions is not None:
            version_ids = {
                version.id
                for version in self._cohort_versions._versions.values()
                if version.cohort_id == cohort_id
            }
            runs = self._cohort_versions._experiment_runs
            if runs is not None and any(
                run.cohort_version_id in version_ids for run in runs._runs.values()
            ):
                raise CohortInUse(cohort_id)
        del self._cohorts[cohort_id]


async def create_cohort(
    repository: FakeCohortRepository,
    owner_id: uuid.UUID,
    agent_id: uuid.UUID,
    name: str = "cohort",
    description: str | None = None,
) -> Cohort:
    """Store a cohort in the fake repository.

    Args:
        repository: Fake cohort repository.
        owner_id: Id of the owning account.
        agent_id: Id of the agent the cohort's sessions belong to.
        name: Cohort name.
        description: Cohort description.

    Returns:
        Stored cohort.
    """
    return await repository.create(
        Cohort(
            owner_id=owner_id,
            name=name,
            description=description,
            agent_id=agent_id,
        )
    )


class FakeCohortVersionRepository:
    """In-memory cohort version repository."""

    def __init__(
        self,
        cohorts: FakeCohortRepository,
        sessions: FakeSessionRepository,
        experiment_runs: "FakeExperimentRunRepository | None" = None,
        tags: FakeTagRepository | None = None,
    ) -> None:
        """Initialize the repository.

        Args:
            cohorts: Fake cohort repository sharing the version counter. Also
                wired back onto the cohort repository so its delete can check
                for an in-use version.
            sessions: Fake session repository, to mark member sessions
                in-cohort-version. Also wired back onto the session
                repository so its query can resolve the
                ``cohort_version_id`` filter.
            experiment_runs: Fake experiment run repository, consulted by
                delete to check for an in-use version. Also wired back onto
                the run repository so its query can resolve the ``cohort_id``
                filter.
            tags: Fake tag repository, consulted by the ``tag`` filter.
        """
        self._cohorts = cohorts
        self._cohorts._cohort_versions = self
        self._sessions = sessions
        self._sessions._cohort_versions = self
        self._experiment_runs = experiment_runs
        self._tags = tags
        if experiment_runs is not None:
            experiment_runs._cohort_versions = self
        self._versions: dict[uuid.UUID, CohortVersion] = {}
        self._members: dict[uuid.UUID, list[uuid.UUID]] = {}

    async def create(
        self, version: CohortVersion, session_ids: Sequence[uuid.UUID]
    ) -> CohortVersion:
        """Persist a new cohort version with a server-assigned version number.

        Args:
            version: Cohort version to store.
            session_ids: Ordered member session ids to link.

        Raises:
            CohortNotFound: No cohort has the version's cohort id.

        Returns:
            Stored cohort version with its assigned version number and
            timestamps set.
        """
        version_number = self._cohorts.increment_latest_version(version.cohort_id)
        now = datetime.now(UTC)
        stored = version.model_copy(
            update={"version": version_number, "created": now, "updated": now}
        )
        self._versions[stored.id] = stored
        self._members[stored.id] = list(session_ids)
        for session_id in session_ids:
            self._sessions._mark_cohort_member(session_id)
        return stored.model_copy()

    async def get(
        self, cohort_version_id: uuid.UUID, exclusive: bool = False
    ) -> CohortVersion:
        """Load a cohort version by id.

        Args:
            cohort_version_id: Id of the cohort version.
            exclusive: Ignored, the fake holds no rows to lock.

        Raises:
            CohortVersionIdNotFound: No cohort version has this id.

        Returns:
            Stored cohort version.
        """
        version = self._versions.get(cohort_version_id)
        if version is None:
            raise CohortVersionIdNotFound(cohort_version_id)
        return version.model_copy()

    async def get_agent_id(self, cohort_version_id: uuid.UUID) -> uuid.UUID:
        """Load the id of the agent a version's cohort belongs to.

        Args:
            cohort_version_id: Id of the cohort version.

        Raises:
            CohortVersionIdNotFound: No cohort version has this id.

        Returns:
            Id of the owning agent.
        """
        version = self._versions.get(cohort_version_id)
        if version is None:
            raise CohortVersionIdNotFound(cohort_version_id)
        cohort = await self._cohorts.get(version.cohort_id)
        return cohort.agent_id

    async def get_by_number(self, cohort_id: uuid.UUID, version: int) -> CohortVersion:
        """Load a cohort version by cohort id and version number.

        Args:
            cohort_id: Id of the cohort.
            version: Version number.

        Raises:
            CohortVersionNotFound: No version with this number exists for
                this cohort.

        Returns:
            Stored cohort version.
        """
        for stored in self._versions.values():
            if stored.cohort_id == cohort_id and stored.version == version:
                return stored.model_copy()
        raise CohortVersionNotFound(cohort_id, version)

    def _cohort_version_ids_tagged(self, tag_name: str) -> set[uuid.UUID]:
        """Resolve the ids of cohort versions linked to a tag by name.

        Args:
            tag_name: Name of the tag to resolve.

        Returns:
            Ids of cohort versions linked to the tag.
        """
        if self._tags is None:
            return set()
        tag_id = next(
            (tag.id for tag in self._tags._tags.values() if tag.name == tag_name),
            None,
        )
        if tag_id is None:
            return set()
        return {
            link.resource_id
            for link in self._tags._links.values()
            if link.tag_id == tag_id
            and link.resource_type == TagResourceType.COHORT_VERSION
        }

    async def query(
        self, version_filter: CohortVersionFilter
    ) -> tuple[list[CohortVersion], str | None]:
        """Query cohort versions matching a filter.

        Args:
            version_filter: Filter and pagination parameters.

        Returns:
            Page of matching cohort versions and the next cursor.
        """
        versions = [
            version
            for version in self._versions.values()
            if version.cohort_id == version_filter.cohort_id
        ]
        if version_filter.expression is not None:
            resolvers = {"tag": self._evaluate_tag_condition}
            versions = [
                v
                for v in versions
                if _evaluate_filter_expression(v, version_filter.expression, resolvers)
            ]
        page, next_cursor = _paginate_fake(versions, version_filter)
        return [version.model_copy() for version in page], next_cursor

    def _evaluate_tag_condition(
        self, version: CohortVersion, condition: FilterCondition
    ) -> bool:
        """Evaluate a tag filter condition against a cohort version.

        Args:
            version: Cohort version to evaluate.
            condition: Validated tag condition.

        Returns:
            Whether the cohort version has a matching tag.
        """
        names = condition.value if condition.op is FilterOp.IN else (condition.value,)
        return any(
            version.id in self._cohort_version_ids_tagged(name) for name in names
        )

    async def list_session_ids(self, cohort_version_id: uuid.UUID) -> list[uuid.UUID]:
        """List a version's member session ids, in order.

        Args:
            cohort_version_id: Id of the cohort version.

        Raises:
            CohortVersionIdNotFound: No cohort version has this id.

        Returns:
            Ordered member session ids.
        """
        if cohort_version_id not in self._versions:
            raise CohortVersionIdNotFound(cohort_version_id)
        return list(self._members.get(cohort_version_id, []))

    async def update(self, version: CohortVersion) -> CohortVersion:
        """Persist changes to an existing cohort version.

        Args:
            version: Cohort version with modified fields.

        Raises:
            CohortVersionIdNotFound: No cohort version has this id.

        Returns:
            Stored cohort version with the updated timestamp renewed.
        """
        stored = self._versions.get(version.id)
        if stored is None:
            raise CohortVersionIdNotFound(version.id)
        now = _renewed_timestamp(stored.updated)
        updated = version.model_copy(update={"created": stored.created, "updated": now})
        self._versions[version.id] = updated
        return updated.model_copy()

    async def delete(self, cohort_version_id: uuid.UUID) -> None:
        """Delete a cohort version by id, cascading its member links.

        Args:
            cohort_version_id: Id of the cohort version.

        Raises:
            CohortVersionIdNotFound: No cohort version has this id.
            CohortVersionInUse: An experiment run references this version.
        """
        if cohort_version_id not in self._versions:
            raise CohortVersionIdNotFound(cohort_version_id)
        if self._experiment_runs is not None and any(
            run.cohort_version_id == cohort_version_id
            for run in self._experiment_runs._runs.values()
        ):
            raise CohortVersionInUse(cohort_version_id)
        del self._versions[cohort_version_id]
        for session_id in self._members.pop(cohort_version_id, []):
            self._sessions._unmark_cohort_member(session_id)


async def create_cohort_version(
    repository: FakeCohortVersionRepository,
    owner_id: uuid.UUID,
    cohort_id: uuid.UUID,
    session_ids: Sequence[uuid.UUID] = (),
    display_version: str | None = None,
) -> CohortVersion:
    """Store a cohort version in the fake repository.

    Args:
        repository: Fake cohort version repository.
        owner_id: Id of the owning account.
        cohort_id: Id of the cohort this version belongs to.
        session_ids: Ordered member session ids.
        display_version: Human-readable designator.

    Returns:
        Stored cohort version.
    """
    return await repository.create(
        CohortVersion(
            owner_id=owner_id,
            cohort_id=cohort_id,
            display_version=display_version,
            session_count=len(session_ids),
        ),
        session_ids,
    )


class FakeWorkerRepository:
    """In-memory worker repository."""

    def __init__(self) -> None:
        """Initialize the repository."""
        self._workers: dict[uuid.UUID, Worker] = {}

    async def register(self, worker: Worker) -> Worker:
        """Persist a new worker.

        Args:
            worker: Worker to store.

        Returns:
            Stored worker with its id, created, and updated timestamp set.
        """
        now = datetime.now(UTC)
        stored = worker.model_copy(update={"created": now, "updated": now})
        self._workers[stored.id] = stored
        return stored.model_copy()

    async def get(self, worker_id: uuid.UUID) -> Worker:
        """Load a worker by id.

        Args:
            worker_id: Id of the worker.

        Raises:
            WorkerNotFound: No worker has this id.

        Returns:
            Stored worker.
        """
        worker = self._workers.get(worker_id)
        if worker is None:
            raise WorkerNotFound(worker_id)
        return worker.model_copy()

    async def update_last_seen_at(self, worker_id: uuid.UUID, now: datetime) -> None:
        """Stamp the time the worker was last seen.

        Args:
            worker_id: Id of the worker.
            now: Current time.

        Raises:
            WorkerNotFound: No worker has this id.
        """
        worker = self._workers.get(worker_id)
        if worker is None:
            raise WorkerNotFound(worker_id)
        self._workers[worker_id] = worker.model_copy(
            update={
                "last_seen_at": now,
                "updated": _renewed_timestamp(worker.updated),
            }
        )

    async def query(
        self, worker_filter: WorkerFilter, live_cutoff: datetime | None
    ) -> tuple[list[Worker], str | None]:
        """Query workers matching a filter.

        Args:
            worker_filter: Filter and pagination parameters.
            live_cutoff: Bound the last heartbeat must be at or after, None
                keeps stale workers.

        Returns:
            Page of matching workers and the next cursor.
        """
        workers = list(self._workers.values())
        if live_cutoff is not None:
            workers = [
                worker for worker in workers if worker.last_seen_at >= live_cutoff
            ]
        if worker_filter.expression is not None:
            workers = [
                worker
                for worker in workers
                if _evaluate_filter_expression(worker, worker_filter.expression)
            ]
        page, next_cursor = _paginate_fake(workers, worker_filter)
        return [worker.model_copy() for worker in page], next_cursor

    async def list_live(self, cutoff: datetime) -> list[Worker]:
        """List workers seen at or after a cutoff.

        Args:
            cutoff: Bound the last heartbeat must be at or after.

        Returns:
            Live workers in id order.
        """
        live = [
            worker for worker in self._workers.values() if worker.last_seen_at >= cutoff
        ]
        live.sort(key=lambda worker: worker.id)
        return [worker.model_copy() for worker in live]

    async def delete(self, worker_id: uuid.UUID) -> None:
        """Delete a worker by id.

        Args:
            worker_id: Id of the worker.

        Raises:
            WorkerNotFound: No worker has this id.
        """
        if worker_id not in self._workers:
            raise WorkerNotFound(worker_id)
        del self._workers[worker_id]


async def create_worker(
    repository: FakeWorkerRepository,
    owner_id: uuid.UUID,
    name: str = "worker-1",
    scope: WorkerScope | None = None,
    runtime: WorkerRuntime | None = None,
    metadata: dict[str, str] | None = None,
    last_seen_at: datetime | None = None,
) -> Worker:
    """Store a worker in the fake repository.

    Args:
        repository: Fake worker repository.
        owner_id: Id of the owning account.
        name: Worker name.
        scope: Claim scope the worker reports.
        runtime: Runtime the worker reports.
        metadata: Arbitrary metadata.
        last_seen_at: Time of the worker's last heartbeat.

    Returns:
        Stored worker.
    """
    return await repository.register(
        Worker(
            owner_id=owner_id,
            name=name,
            scope=scope if scope is not None else UNSCOPED_WORKER_SCOPE,
            runtime=runtime if runtime is not None else WorkerRuntime(platform="bare"),
            metadata=metadata if metadata is not None else {},
            last_seen_at=last_seen_at
            if last_seen_at is not None
            else datetime.now(UTC),
        )
    )
