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
"""In-memory fakes for experiments, replays, and imports."""

import uuid
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.replay import BaselineEvaluationMode, ReplayStatus
from kitaru.api_models.v1.tag import TagResourceType
from kitaru.server.application.models.experiment import ExperimentFilter
from kitaru.server.application.models.imports import ImportFilter
from kitaru.server.application.models.replay import ReplayFilter, ReplayStatusCounts
from kitaru.server.domain.experiment import (
    DuplicateExperimentName,
    Experiment,
    ExperimentNotFound,
)
from kitaru.server.domain.imports import Import, ImportNotFound
from kitaru.server.domain.plugin import AnalyzerConfig, EvaluatorConfig
from kitaru.server.domain.replay import (
    DuplicateReplayForBaseline,
    Replay,
    ReplayAlreadyExistsForJob,
    ReplayNotFound,
)
from kitaru.server.domain.replay_config import (
    ReplayConfig,
    ReplayConfigInUse,
    ReplayConfigNotFound,
)
from kitaru.server.filtering import FilterCondition
from kitaru_test_support.common import (
    _evaluate_filter_expression,
    _paginate_fake,
    _renewed_timestamp,
)
from kitaru_test_support.sessions import FakeTagRepository

# Named only in string annotations; a runtime import could be circular.
if TYPE_CHECKING:
    from kitaru_test_support.evaluations import FakeExperimentRunRepository
    from kitaru_test_support.sessions import FakeSessionRepository


class FakeExperimentRepository:
    """In-memory experiment and replay config repository."""

    def __init__(
        self,
        tag_repository: FakeTagRepository | None = None,
        experiment_run_repository: "FakeExperimentRunRepository | None" = None,
        replay_repository: "FakeReplayRepository | None" = None,
    ) -> None:
        """Initialize the repository.

        Args:
            tag_repository: Tag repository, queried for the tag filter,
                mirroring the tag EXISTS join.
            experiment_run_repository: Experiment run repository, restricting
                the delete when the experiment has runs.
            replay_repository: Replay repository, restricting the replay
                config delete when a replay still references it.
        """
        self._experiments: dict[uuid.UUID, Experiment] = {}
        self._configs: dict[uuid.UUID, ReplayConfig] = {}
        self._tag_repository = tag_repository
        self._experiment_run_repository = experiment_run_repository
        self._replay_repository = replay_repository

    def _check_duplicate_name(self, experiment: Experiment) -> None:
        for other in self._experiments.values():
            if (
                other.id != experiment.id
                and other.agent_id == experiment.agent_id
                and other.name == experiment.name
            ):
                raise DuplicateExperimentName(experiment.name)

    async def create(self, experiment: Experiment) -> Experiment:
        """Persist a new experiment.

        Args:
            experiment: Experiment to store.

        Raises:
            DuplicateExperimentName: The experiment name is already
                registered.

        Returns:
            Stored experiment with timestamps set.
        """
        self._check_duplicate_name(experiment)
        now = datetime.now(UTC)
        stored = experiment.model_copy(update={"created": now, "updated": now})
        self._experiments[stored.id] = stored
        return stored.model_copy()

    async def get(
        self, experiment_id: uuid.UUID, exclusive: bool = False
    ) -> Experiment:
        """Load an experiment by id.

        Args:
            experiment_id: Id of the experiment.
            exclusive: Whether to lock the row, a no-op in memory.

        Raises:
            ExperimentNotFound: No experiment has this id.

        Returns:
            Stored experiment.
        """
        _ = exclusive
        experiment = self._experiments.get(experiment_id)
        if experiment is None:
            raise ExperimentNotFound(experiment_id)
        return experiment.model_copy()

    async def query(
        self, experiment_filter: ExperimentFilter
    ) -> tuple[list[Experiment], str | None]:
        """Query experiments matching a filter.

        Args:
            experiment_filter: Filter and pagination parameters.

        Returns:
            Page of matching experiments and the next cursor.
        """
        experiments = list(self._experiments.values())
        if experiment_filter.expression is not None:
            resolvers = {"tag": self._evaluate_tag_condition}
            experiments = [
                experiment
                for experiment in experiments
                if _evaluate_filter_expression(
                    experiment, experiment_filter.expression, resolvers
                )
            ]
        page, next_cursor = _paginate_fake(experiments, experiment_filter)
        return [experiment.model_copy() for experiment in page], next_cursor

    def _evaluate_tag_condition(
        self, experiment: Experiment, condition: FilterCondition
    ) -> bool:
        """Evaluate a tag filter condition against an experiment.

        Args:
            experiment: Experiment to evaluate.
            condition: Validated tag condition.

        Returns:
            Whether the experiment has a matching tag.
        """
        assert self._tag_repository is not None
        names = condition.value if condition.op is FilterOp.IN else (condition.value,)
        return any(
            self._tag_repository.has_link(
                TagResourceType.EXPERIMENT, experiment.id, name
            )
            for name in names
        )

    async def update(self, experiment: Experiment) -> Experiment:
        """Persist changes to an existing experiment.

        Args:
            experiment: Experiment with modified fields.

        Raises:
            ExperimentNotFound: No experiment has this id.
            DuplicateExperimentName: The experiment name is already
                registered.

        Returns:
            Stored experiment with the updated timestamp renewed.
        """
        stored = self._experiments.get(experiment.id)
        if stored is None:
            raise ExperimentNotFound(experiment.id)
        self._check_duplicate_name(experiment)
        now = _renewed_timestamp(stored.updated)
        updated = experiment.model_copy(
            update={"created": stored.created, "updated": now}
        )
        self._experiments[experiment.id] = updated
        return updated.model_copy()

    async def delete(self, experiment_id: uuid.UUID) -> None:
        """Delete an experiment by id, cascading its runs and their replays.

        Args:
            experiment_id: Id of the experiment.

        Raises:
            ExperimentNotFound: No experiment has this id.
        """
        if experiment_id not in self._experiments:
            raise ExperimentNotFound(experiment_id)
        if self._experiment_run_repository is not None:
            run_ids = {
                run.id
                for run in self._experiment_run_repository._runs.values()
                if run.experiment_id == experiment_id
            }
            for run_id in run_ids:
                del self._experiment_run_repository._runs[run_id]
            if self._replay_repository is not None:
                for replay_id, replay in list(self._replay_repository._replays.items()):
                    if replay.experiment_run_id in run_ids:
                        del self._replay_repository._replays[replay_id]
        del self._experiments[experiment_id]

    async def create_replay_config(self, config: ReplayConfig) -> ReplayConfig:
        """Persist a new replay config.

        Args:
            config: Replay config to store.

        Returns:
            Stored replay config with timestamps set.
        """
        now = datetime.now(UTC)
        stored = config.model_copy(update={"created": now, "updated": now})
        self._configs[stored.id] = stored
        return stored.model_copy()

    async def get_replay_config(self, config_id: uuid.UUID) -> ReplayConfig:
        """Load a replay config by id.

        Args:
            config_id: Id of the replay config.

        Raises:
            ReplayConfigNotFound: No replay config has this id.

        Returns:
            Stored replay config.
        """
        config = self._configs.get(config_id)
        if config is None:
            raise ReplayConfigNotFound(config_id)
        return config.model_copy()

    async def get_many_replay_configs(
        self, config_ids: Sequence[uuid.UUID]
    ) -> dict[uuid.UUID, ReplayConfig]:
        """Load replay configs by id, in one bulk fetch.

        Args:
            config_ids: Ids of the replay configs.

        Returns:
            Replay configs keyed by id, missing ids omitted.
        """
        return {
            config_id: self._configs[config_id].model_copy()
            for config_id in config_ids
            if config_id in self._configs
        }

    async def delete_replay_config(self, config_id: uuid.UUID) -> None:
        """Delete a replay config by id.

        Args:
            config_id: Id of the replay config.

        Raises:
            ReplayConfigNotFound: No replay config has this id.
            ReplayConfigInUse: A replay references the replay config.
        """
        if config_id not in self._configs:
            raise ReplayConfigNotFound(config_id)
        if self._replay_repository is not None and any(
            replay.replay_config_id == config_id
            for replay in self._replay_repository._replays.values()
        ):
            raise ReplayConfigInUse(config_id)
        del self._configs[config_id]


async def create_experiment(
    repository: FakeExperimentRepository,
    owner_id: uuid.UUID,
    agent_id: uuid.UUID,
    replay_config_id: uuid.UUID,
    name: str = "smoke-test",
    description: str | None = None,
) -> Experiment:
    """Store an experiment in the fake repository.

    Args:
        repository: Fake experiment repository.
        owner_id: Id of the owning account.
        agent_id: Id of the agent the experiment belongs to.
        replay_config_id: Id of the experiment's replay config.
        name: Experiment name.
        description: Experiment description.

    Returns:
        Stored experiment.
    """
    return await repository.create(
        Experiment(
            owner_id=owner_id,
            name=name,
            description=description,
            agent_id=agent_id,
            replay_config_id=replay_config_id,
        )
    )


class FakeReplayRepository:
    """In-memory replay repository."""

    def __init__(
        self,
        sessions: "FakeSessionRepository | None" = None,
        experiment_runs: "FakeExperimentRunRepository | None" = None,
    ) -> None:
        """Initialize the repository.

        Args:
            sessions: Fake session repository, wired back onto the session
                repository so its delete can check for a replay reference.
            experiment_runs: Fake experiment run repository, wired back onto
                the run repository so its delete cascades these replays.
        """
        self._replays: dict[uuid.UUID, Replay] = {}
        if sessions is not None:
            sessions._replays = self
        if experiment_runs is not None:
            experiment_runs._replays = self

    def _check_duplicate_baseline(self, replay: Replay) -> None:
        for other in self._replays.values():
            if (
                other.id != replay.id
                and other.experiment_run_id is not None
                and other.experiment_run_id == replay.experiment_run_id
                and other.baseline_session_id == replay.baseline_session_id
            ):
                raise DuplicateReplayForBaseline(
                    replay.experiment_run_id, replay.baseline_session_id
                )

    def _check_unique_job(self, replay: Replay) -> None:
        job_id = replay.job_id
        if job_id is None:
            return
        for other in self._replays.values():
            if other.id != replay.id and other.job_id == job_id:
                raise ReplayAlreadyExistsForJob(job_id)

    async def create(self, replay: Replay) -> Replay:
        """Persist a new replay.

        Args:
            replay: Replay to store.

        Raises:
            DuplicateReplayForBaseline: The run already holds a replay for
                this baseline session.
            ReplayAlreadyExistsForJob: The job already has a replay.

        Returns:
            Stored replay with timestamps set.
        """
        self._check_duplicate_baseline(replay)
        self._check_unique_job(replay)
        now = datetime.now(UTC)
        stored = replay.model_copy(update={"created": now, "updated": now})
        self._replays[stored.id] = stored
        return stored.model_copy()

    async def create_many(self, replays: list[Replay]) -> list[Replay]:
        """Persist many new replays in one round trip.

        Args:
            replays: Replays to store.

        Returns:
            Stored replays with timestamps set, in the same order.
        """
        return [await self.create(replay) for replay in replays]

    async def get(self, replay_id: uuid.UUID) -> Replay:
        """Load a replay by id.

        Args:
            replay_id: Id of the replay.

        Raises:
            ReplayNotFound: No replay has this id.

        Returns:
            Stored replay.
        """
        replay = self._replays.get(replay_id)
        if replay is None:
            raise ReplayNotFound(replay_id)
        return replay.model_copy()

    async def get_by_job_id(self, job_id: uuid.UUID) -> Replay | None:
        """Load the replay owning a job, if any.

        Args:
            job_id: Id of the job.

        Returns:
            Stored replay, or ``None`` when the job holds no replay.
        """
        for replay in self._replays.values():
            if replay.job_id == job_id:
                return replay.model_copy()
        return None

    async def get_by_result_session_id(self, session_id: uuid.UUID) -> Replay | None:
        """Load the replay that produced a session, if any.

        Args:
            session_id: Id of the produced session.

        Returns:
            Stored replay, or ``None`` when no replay produced the session.
        """
        for replay in self._replays.values():
            if replay.result_session_id == session_id:
                return replay.model_copy()
        return None

    async def get_many_by_job_ids(
        self, job_ids: Sequence[uuid.UUID]
    ) -> dict[uuid.UUID, Replay]:
        """Bulk-load the replay of each job, keyed by job id.

        Args:
            job_ids: Ids of the jobs.

        Returns:
            Replays keyed by job id, jobs without a replay omitted.
        """
        job_id_set = set(job_ids)
        return {
            replay.job_id: replay.model_copy()
            for replay in self._replays.values()
            if replay.job_id is not None and replay.job_id in job_id_set
        }

    async def query(
        self, replay_filter: ReplayFilter
    ) -> tuple[list[Replay], str | None]:
        """Query replays matching a filter.

        Args:
            replay_filter: Filter and pagination parameters.

        Returns:
            Page of matching replays and the next cursor.
        """
        replays = list(self._replays.values())
        if replay_filter.expression is not None:
            replays = [
                r
                for r in replays
                if _evaluate_filter_expression(r, replay_filter.expression)
            ]
        page, next_cursor = _paginate_fake(replays, replay_filter)
        return [r.model_copy() for r in page], next_cursor

    async def list_by_experiment_run(
        self, experiment_run_id: uuid.UUID
    ) -> list[Replay]:
        """Load every replay of an experiment run.

        Args:
            experiment_run_id: Id of the run.

        Returns:
            Replays of the run, in creation order.
        """
        matches = [
            r
            for r in self._replays.values()
            if r.experiment_run_id == experiment_run_id
        ]
        return [r.model_copy() for r in sorted(matches, key=lambda r: r.id)]

    async def update(self, replay: Replay) -> Replay:
        """Persist changes to an existing replay.

        Args:
            replay: Replay with modified fields.

        Raises:
            ReplayNotFound: No replay has this id.

        Returns:
            Stored replay with the updated timestamp renewed.
        """
        stored = self._replays.get(replay.id)
        if stored is None:
            raise ReplayNotFound(replay.id)
        now = _renewed_timestamp(stored.updated)
        updated = replay.model_copy(update={"created": stored.created, "updated": now})
        self._replays[replay.id] = updated
        return updated.model_copy()

    async def update_many(self, replays: list[Replay]) -> list[Replay]:
        """Persist changes to many existing replays in one round trip.

        Args:
            replays: Replays with modified fields.

        Raises:
            ReplayNotFound: A replay id matches no replay.

        Returns:
            Stored replays with the updated timestamp renewed, in id order.
        """
        return [
            await self.update(replay)
            for replay in sorted(replays, key=lambda replay: replay.id)
        ]

    async def delete(self, replay_id: uuid.UUID) -> None:
        """Delete a replay by id.

        Args:
            replay_id: Id of the replay.

        Raises:
            ReplayNotFound: No replay has this id.
        """
        if replay_id not in self._replays:
            raise ReplayNotFound(replay_id)
        del self._replays[replay_id]

    async def count_by_status(self, experiment_run_id: uuid.UUID) -> ReplayStatusCounts:
        """Count an experiment run's replays by status.

        Args:
            experiment_run_id: Id of the run.

        Returns:
            Replay counts by status.
        """
        counts = await self.count_by_status_many([experiment_run_id])
        return counts.get(experiment_run_id, ReplayStatusCounts())

    async def count_by_status_many(
        self, experiment_run_ids: Sequence[uuid.UUID]
    ) -> dict[uuid.UUID, ReplayStatusCounts]:
        """Bulk-count replays by status for many experiment runs.

        Args:
            experiment_run_ids: Ids of the runs.

        Returns:
            Replay status counts keyed by run id, missing ids holding zero
            counts.
        """
        run_ids = set(experiment_run_ids)
        tallies: dict[uuid.UUID, dict[ReplayStatus, int]] = {}
        for replay in self._replays.values():
            run_id = replay.experiment_run_id
            if run_id is None or run_id not in run_ids:
                continue
            tally = tallies.setdefault(run_id, {})
            tally[replay.status] = tally.get(replay.status, 0) + 1
        return {
            run_id: ReplayStatusCounts(
                pending=tally.get(ReplayStatus.PENDING, 0),
                evaluating=tally.get(ReplayStatus.EVALUATING, 0),
                completed=tally.get(ReplayStatus.COMPLETED, 0),
                failed=tally.get(ReplayStatus.FAILED, 0),
                canceled=tally.get(ReplayStatus.CANCELED, 0),
            )
            for run_id, tally in tallies.items()
        }

    async def exists_for_replay_config(self, replay_config_id: uuid.UUID) -> bool:
        """Report whether any replay references a replay config.

        Args:
            replay_config_id: Id of the replay config.

        Returns:
            Whether a replay references the replay config.
        """
        return any(
            replay.replay_config_id == replay_config_id
            for replay in self._replays.values()
        )


async def create_replay(
    repository: FakeReplayRepository,
    owner_id: uuid.UUID,
    job_id: uuid.UUID,
    replay_config_id: uuid.UUID,
    baseline_session_id: uuid.UUID,
    experiment_run_id: uuid.UUID | None = None,
    baseline_evaluation_mode: BaselineEvaluationMode = BaselineEvaluationMode.NONE,
    status: ReplayStatus = ReplayStatus.PENDING,
) -> Replay:
    """Store a replay in the fake repository.

    Args:
        repository: Fake replay repository.
        owner_id: Id of the owning account.
        job_id: Id of the job running the replay.
        replay_config_id: Id of the replay's config.
        baseline_session_id: Id of the session replayed.
        experiment_run_id: Run this replay belongs to, ``None`` for a
            standalone replay.
        baseline_evaluation_mode: How the baseline session is scored.
        status: Replay status.

    Returns:
        Stored replay.
    """
    return await repository.create(
        Replay(
            owner_id=owner_id,
            job_id=job_id,
            experiment_run_id=experiment_run_id,
            replay_config_id=replay_config_id,
            baseline_session_id=baseline_session_id,
            baseline_evaluation_mode=baseline_evaluation_mode,
            status=status,
        )
    )


def get_replay_job_id(replay: Replay) -> uuid.UUID:
    """Read the job id of a replay that still points at its job.

    Args:
        replay: Replay to read.

    Returns:
        Id of the job that ran the replay.
    """
    assert replay.job_id is not None
    return replay.job_id


class FakeImportRepository:
    """In-memory import repository."""

    def __init__(self) -> None:
        """Initialize the repository."""
        self._imports: dict[uuid.UUID, Import] = {}

    async def create(self, import_: Import) -> Import:
        """Persist a new import.

        Args:
            import_: Import to store.

        Returns:
            Stored import with timestamps set.
        """
        now = datetime.now(UTC)
        stored = import_.model_copy(update={"created": now, "updated": now})
        self._imports[stored.id] = stored
        return stored.model_copy()

    async def get(self, import_id: uuid.UUID) -> Import:
        """Load an import by id.

        Args:
            import_id: Id of the import.

        Raises:
            ImportNotFound: No import has this id.

        Returns:
            Stored import.
        """
        import_ = self._imports.get(import_id)
        if import_ is None:
            raise ImportNotFound(import_id)
        return import_.model_copy()

    async def get_by_job_id(self, job_id: uuid.UUID) -> Import | None:
        """Load the import owning a job, if any.

        Args:
            job_id: Id of the job.

        Returns:
            Stored import, or ``None`` when the job holds no import.
        """
        for import_ in self._imports.values():
            if import_.job_id == job_id:
                return import_.model_copy()
        return None

    async def query(
        self, import_filter: ImportFilter
    ) -> tuple[list[Import], str | None]:
        """Query imports matching a filter.

        Args:
            import_filter: Filter and pagination parameters.

        Returns:
            Page of matching imports and the next cursor.
        """
        imports = list(self._imports.values())
        if import_filter.expression is not None:
            imports = [
                i
                for i in imports
                if _evaluate_filter_expression(i, import_filter.expression)
            ]
        page, next_cursor = _paginate_fake(imports, import_filter)
        return [i.model_copy() for i in page], next_cursor

    async def update(self, import_: Import) -> Import:
        """Persist changes to an existing import.

        Args:
            import_: Import with modified fields.

        Raises:
            ImportNotFound: No import has this id.

        Returns:
            Stored import with the updated timestamp renewed.
        """
        stored = self._imports.get(import_.id)
        if stored is None:
            raise ImportNotFound(import_.id)
        now = _renewed_timestamp(stored.updated)
        updated = import_.model_copy(update={"created": stored.created, "updated": now})
        self._imports[import_.id] = updated
        return updated.model_copy()


async def create_import(
    repository: FakeImportRepository,
    owner_id: uuid.UUID,
    agent_id: uuid.UUID,
    job_id: uuid.UUID | None = None,
    agent_version_id: uuid.UUID | None = None,
    importer_version_id: uuid.UUID | None = None,
    connection_id: uuid.UUID | None = None,
    payload_blob_id: uuid.UUID | None = None,
    params: dict[str, Any] | None = None,
    evaluators: list[EvaluatorConfig] | None = None,
    analyzers: list[AnalyzerConfig] | None = None,
    max_sessions: int | None = None,
) -> Import:
    """Store an import in the fake repository.

    Args:
        repository: Fake import repository.
        owner_id: Id of the owning account.
        agent_id: Agent imported sessions are created under.
        job_id: Id of the job running the import.
        agent_version_id: Agent version recorded on the imported sessions.
        importer_version_id: Importer version the import runs.
        connection_id: Connection the import runs with.
        payload_blob_id: Blob holding the payload.
        params: Parameters passed to the importer.
        evaluators: Evaluators run against every imported session.
        analyzers: Analyzers run against every imported session.
        max_sessions: Maximum number of sessions created by the import.

    Returns:
        Stored import.
    """
    return await repository.create(
        Import(
            owner_id=owner_id,
            job_id=job_id,
            agent_id=agent_id,
            agent_version_id=agent_version_id,
            importer_version_id=(
                importer_version_id if importer_version_id is not None else uuid.uuid4()
            ),
            connection_id=connection_id,
            payload_blob_id=(
                payload_blob_id if payload_blob_id is not None else uuid.uuid4()
            ),
            params=params if params is not None else {},
            evaluators=evaluators if evaluators is not None else [],
            analyzers=analyzers if analyzers is not None else [],
            max_sessions=max_sessions,
        )
    )
