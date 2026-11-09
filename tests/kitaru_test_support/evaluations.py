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
"""In-memory fakes for experiment runs and evaluations."""

import uuid
from collections.abc import Sequence
from datetime import UTC, datetime

from kitaru.api_models.v1.evaluation import EvaluationDataType
from kitaru.api_models.v1.experiment_run import ExperimentRunStatus
from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.replay import BaselineEvaluationMode
from kitaru.api_models.v1.tag import TagResourceType
from kitaru.server.application.interfaces.evaluation_repository import (
    EvaluationWithEvaluator,
)
from kitaru.server.application.models.evaluation import EvaluationFilter
from kitaru.server.application.models.experiment_run import ExperimentRunFilter
from kitaru.server.domain.evaluation import (
    Evaluation,
    EvaluationNameConflict,
    EvaluationNotFound,
)
from kitaru.server.domain.experiment_run import (
    DuplicateExperimentRunNumber,
    ExperimentRun,
    ExperimentRunNotFound,
)
from kitaru.server.domain.plugin import PluginVersionIdNotFound
from kitaru.server.filtering import FilterCondition
from kitaru_test_support.blobs_plugins import FakePluginRepository
from kitaru_test_support.cohorts_workers import FakeCohortVersionRepository
from kitaru_test_support.common import (
    _evaluate_filter_expression,
    _matches_condition,
    _paginate_fake,
    _refuse_unresolvable_fields,
    _renewed_timestamp,
)
from kitaru_test_support.experiments_replays import FakeReplayRepository
from kitaru_test_support.sessions import FakeTagRepository


class FakeExperimentRunRepository:
    """In-memory experiment run repository."""

    def __init__(self, tag_repository: FakeTagRepository | None = None) -> None:
        """Initialize the repository.

        Args:
            tag_repository: Tag repository, queried for the tag filter,
                mirroring the tag EXISTS join.
        """
        self._runs: dict[uuid.UUID, ExperimentRun] = {}
        self._tag_repository = tag_repository
        # Wired back by FakeCohortVersionRepository, to resolve the cohort
        # filter the way the SQL repository resolves it through a subquery.
        self._cohort_versions: FakeCohortVersionRepository | None = None
        # Wired back by FakeReplayRepository, so delete cascades a run's
        # replays the way the SQL foreign key cascades them.
        self._replays: FakeReplayRepository | None = None

    async def create(self, run: ExperimentRun) -> ExperimentRun:
        """Persist a new experiment run.

        Args:
            run: Experiment run to store.

        Raises:
            DuplicateExperimentRunNumber: The experiment already has a run
                with this number.

        Returns:
            Stored experiment run with timestamps set.
        """
        for other in self._runs.values():
            if (
                other.id != run.id
                and other.experiment_id == run.experiment_id
                and other.number == run.number
            ):
                raise DuplicateExperimentRunNumber(run.experiment_id, run.number)
        now = datetime.now(UTC)
        stored = run.model_copy(update={"created": now, "updated": now})
        self._runs[stored.id] = stored
        return stored.model_copy()

    async def get(
        self, experiment_run_id: uuid.UUID, exclusive: bool = False
    ) -> ExperimentRun:
        """Load an experiment run by id.

        Args:
            experiment_run_id: Id of the run.
            exclusive: Whether to lock the row, a no-op in memory.

        Raises:
            ExperimentRunNotFound: No run has this id.

        Returns:
            Stored experiment run.
        """
        _ = exclusive
        run = self._runs.get(experiment_run_id)
        if run is None:
            raise ExperimentRunNotFound(experiment_run_id)
        return run.model_copy()

    async def query(
        self, run_filter: ExperimentRunFilter
    ) -> tuple[list[ExperimentRun], str | None]:
        """Query experiment runs matching a filter.

        Args:
            run_filter: Filter and pagination parameters.

        Returns:
            Page of matching runs and the next cursor.
        """
        _refuse_unresolvable_fields(run_filter.expression, ("agent_id",))
        runs = list(self._runs.values())
        if run_filter.expression is not None:
            resolvers = {
                "tag": self._evaluate_tag_condition,
                "cohort_id": self._evaluate_cohort_condition,
            }
            runs = [
                r
                for r in runs
                if _evaluate_filter_expression(r, run_filter.expression, resolvers)
            ]
        page, next_cursor = _paginate_fake(runs, run_filter)
        return [r.model_copy() for r in page], next_cursor

    def _evaluate_cohort_condition(
        self, run: ExperimentRun, condition: FilterCondition
    ) -> bool:
        """Evaluate a cohort condition against an experiment run.

        Args:
            run: Experiment run to evaluate.
            condition: Validated cohort condition.

        Returns:
            Whether the run pins a version of a matching cohort.
        """
        assert self._cohort_versions is not None
        version = self._cohort_versions._versions.get(run.cohort_version_id)
        return _matches_condition(version.cohort_id if version else None, condition)

    def _evaluate_tag_condition(
        self, run: ExperimentRun, condition: FilterCondition
    ) -> bool:
        """Evaluate a tag filter condition against an experiment run.

        Args:
            run: Experiment run to evaluate.
            condition: Validated tag condition.

        Returns:
            Whether the run has a matching tag.
        """
        assert self._tag_repository is not None
        names = condition.value if condition.op is FilterOp.IN else (condition.value,)
        return any(
            self._tag_repository.has_link(TagResourceType.EXPERIMENT_RUN, run.id, name)
            for name in names
        )

    async def update(self, run: ExperimentRun) -> ExperimentRun:
        """Persist changes to an existing experiment run.

        Args:
            run: Experiment run with modified fields.

        Raises:
            ExperimentRunNotFound: No run has this id.

        Returns:
            Stored experiment run with the updated timestamp renewed.
        """
        stored = self._runs.get(run.id)
        if stored is None:
            raise ExperimentRunNotFound(run.id)
        now = _renewed_timestamp(stored.updated)
        updated = run.model_copy(update={"created": stored.created, "updated": now})
        self._runs[run.id] = updated
        return updated.model_copy()

    async def delete(self, experiment_run_id: uuid.UUID) -> None:
        """Delete an experiment run by id.

        Args:
            experiment_run_id: Id of the run.

        Raises:
            ExperimentRunNotFound: No run has this id.
        """
        if experiment_run_id not in self._runs:
            raise ExperimentRunNotFound(experiment_run_id)
        del self._runs[experiment_run_id]
        if self._replays is not None:
            for replay_id in [
                replay.id
                for replay in self._replays._replays.values()
                if replay.experiment_run_id == experiment_run_id
            ]:
                del self._replays._replays[replay_id]

    async def get_max_number(self, experiment_id: uuid.UUID) -> int:
        """Read the highest run number an experiment has assigned.

        Args:
            experiment_id: Id of the experiment.

        Returns:
            Highest assigned run number, or 0 when the experiment has no runs.
        """
        numbers = [
            run.number
            for run in self._runs.values()
            if run.experiment_id == experiment_id
        ]
        return max(numbers, default=0)

    async def exists_for_experiment(self, experiment_id: uuid.UUID) -> bool:
        """Report whether an experiment has any run.

        Args:
            experiment_id: Id of the experiment.

        Returns:
            Whether the experiment has any run.
        """
        return any(run.experiment_id == experiment_id for run in self._runs.values())

    async def list_by_experiment(self, experiment_id: uuid.UUID) -> list[ExperimentRun]:
        """Load every run of an experiment.

        Args:
            experiment_id: Id of the experiment.

        Returns:
            Runs of the experiment, in creation order.
        """
        runs = [
            run for run in self._runs.values() if run.experiment_id == experiment_id
        ]
        return [run.model_copy() for run in sorted(runs, key=lambda run: run.id)]


async def create_experiment_run(
    repository: FakeExperimentRunRepository,
    owner_id: uuid.UUID,
    experiment_id: uuid.UUID,
    cohort_version_id: uuid.UUID,
    agent_version_id: uuid.UUID,
    number: int = 1,
    baseline_evaluation_mode: BaselineEvaluationMode = BaselineEvaluationMode.NONE,
    status: ExperimentRunStatus = ExperimentRunStatus.RUNNING,
) -> ExperimentRun:
    """Store an experiment run in the fake repository.

    Args:
        repository: Fake experiment run repository.
        owner_id: Id of the owning account.
        experiment_id: Id of the experiment this run belongs to.
        cohort_version_id: Id of the cohort version whose sessions are
            replayed.
        agent_version_id: Id of the agent version to replay with.
        number: Run number within the experiment.
        baseline_evaluation_mode: How baseline sessions are scored.
        status: Run status.

    Returns:
        Stored experiment run.
    """
    return await repository.create(
        ExperimentRun(
            owner_id=owner_id,
            experiment_id=experiment_id,
            number=number,
            cohort_version_id=cohort_version_id,
            agent_version_id=agent_version_id,
            baseline_evaluation_mode=baseline_evaluation_mode,
            status=status,
        )
    )


class FakeEvaluationRepository:
    """In-memory evaluation repository."""

    def __init__(self, plugin_repository: FakePluginRepository | None = None) -> None:
        """Initialize the repository.

        Args:
            plugin_repository: Fake plugin repository, consulted to
                denormalize the evaluator name and version and to check a
                created row's evaluator version exists.
        """
        self._evaluations: dict[uuid.UUID, Evaluation] = {}
        self._replay_links: set[tuple[uuid.UUID, uuid.UUID]] = set()
        self._plugin_repository = plugin_repository

    def _evaluator_info(
        self, evaluator_version_id: uuid.UUID | None
    ) -> tuple[str | None, int | None]:
        """Resolve the evaluator name and version for a plugin version id.

        Args:
            evaluator_version_id: Id of the referenced plugin version.

        Returns:
            Evaluator name and version, both ``None`` when unresolved.
        """
        if evaluator_version_id is None or self._plugin_repository is None:
            return None, None
        version = self._plugin_repository._versions.get(evaluator_version_id)
        if version is None:
            return None, None
        plugin = self._plugin_repository._plugins.get(version.plugin_id)
        if plugin is None:
            return None, None
        return plugin.name, version.version

    async def get(self, evaluation_id: uuid.UUID) -> EvaluationWithEvaluator:
        """Load an evaluation by id, joined with its evaluator name and version.

        Args:
            evaluation_id: Id of the evaluation.

        Raises:
            EvaluationNotFound: No evaluation has this id.

        Returns:
            Stored evaluation paired with its evaluator name and version.
        """
        evaluation = self._evaluations.get(evaluation_id)
        if evaluation is None:
            raise EvaluationNotFound(evaluation_id)
        name, version = self._evaluator_info(evaluation.evaluator_version_id)
        return EvaluationWithEvaluator(evaluation.model_copy(), name, version)

    async def query(
        self, evaluation_filter: EvaluationFilter
    ) -> tuple[list[EvaluationWithEvaluator], str | None]:
        """Query evaluations matching a filter.

        Args:
            evaluation_filter: Filter and pagination parameters.

        Returns:
            Page of matching evaluations and the next cursor.
        """
        _refuse_unresolvable_fields(
            evaluation_filter.expression,
            ("agent_id", "cohort_id", "experiment_run_id", "replay_id"),
        )
        evaluations = list(self._evaluations.values())
        if evaluation_filter.expression is not None:
            evaluations = [
                e
                for e in evaluations
                if _evaluate_filter_expression(e, evaluation_filter.expression)
            ]
        page, next_cursor = _paginate_fake(evaluations, evaluation_filter)
        items = [
            EvaluationWithEvaluator(
                evaluation.model_copy(),
                *self._evaluator_info(evaluation.evaluator_version_id),
            )
            for evaluation in page
        ]
        return items, next_cursor

    async def create_session_evaluations(
        self, session_id: uuid.UUID, evaluations: list[Evaluation]
    ) -> list[Evaluation]:
        """Insert manual evaluations into a session.

        Args:
            session_id: Id of the session the evaluations belong to.
            evaluations: Fully resolved evaluations to store, in request
                order.

        Raises:
            EvaluationNameConflict: A name in the batch already exists for
                the session.

        Returns:
            Stored evaluations in request order.
        """
        seen_names: set[str] = set()
        for evaluation in evaluations:
            exists = any(
                e.session_id == evaluation.session_id
                and e.evaluator_version_id is None
                and e.name == evaluation.name
                for e in self._evaluations.values()
            )
            if exists or evaluation.name in seen_names:
                raise EvaluationNameConflict(evaluation.name, session_id)
            seen_names.add(evaluation.name)
        stored: list[Evaluation] = []
        for evaluation in evaluations:
            now = datetime.now(UTC)
            row = evaluation.model_copy(update={"created": now, "updated": now})
            self._evaluations[row.id] = row
            stored.append(row.model_copy())
        return stored

    def has_evaluation(self, session_id: uuid.UUID) -> bool:
        """Report whether a session has at least one stored evaluation.

        Mirrors the EXISTS probe the SQL repository runs against the
        evaluation table.

        Args:
            session_id: Id of the session.

        Returns:
            Whether the session has at least one stored evaluation.
        """
        return any(
            evaluation.session_id == session_id
            for evaluation in self._evaluations.values()
        )

    async def create_task_evaluations(
        self, evaluations: list[Evaluation], replay_id: uuid.UUID | None
    ) -> list[Evaluation]:
        """Insert evaluation rows produced by a completed evaluator task.

        Args:
            evaluations: Fully resolved evaluations to store, in result order.
            replay_id: Replay to link each stored row to, ``None`` for a
                standalone evaluation batch.

        Raises:
            PluginVersionIdNotFound: No plugin version has the evaluator
                version id.

        Returns:
            Stored evaluations in result order.
        """
        if not evaluations:
            return []
        evaluator_version_id = evaluations[0].evaluator_version_id
        if (
            evaluator_version_id is not None
            and self._plugin_repository is not None
            and evaluator_version_id not in self._plugin_repository._versions
        ):
            raise PluginVersionIdNotFound(evaluator_version_id)
        stored: list[Evaluation] = []
        for evaluation in evaluations:
            now = datetime.now(UTC)
            row = evaluation.model_copy(update={"created": now, "updated": now})
            self._evaluations[row.id] = row
            stored.append(row.model_copy())
        if replay_id is not None:
            self._replay_links.update((replay_id, row.id) for row in stored)
        return stored

    async def get_latest_evaluation_ids_by_identity(
        self, session_ids: Sequence[uuid.UUID]
    ) -> dict[tuple[uuid.UUID, uuid.UUID, str], list[uuid.UUID]]:
        """Read the latest invocation's evaluation ids per identity.

        Args:
            session_ids: Ids of the candidate sessions.

        Returns:
            Evaluation ids of the latest invocation keyed by (session_id,
            evaluator_version_id, params_hash), identities without a match
            omitted.
        """
        session_id_set = set(session_ids)
        rows: list[tuple[tuple[uuid.UUID, uuid.UUID, str], uuid.UUID, uuid.UUID]] = []
        for evaluation in self._evaluations.values():
            if (
                evaluation.session_id not in session_id_set
                or evaluation.evaluator_version_id is None
                or evaluation.params_hash is None
                or evaluation.invocation_id is None
            ):
                continue
            identity = (
                evaluation.session_id,
                evaluation.evaluator_version_id,
                evaluation.params_hash,
            )
            rows.append((identity, evaluation.invocation_id, evaluation.id))
        rows.sort(
            key=lambda row: (
                self._evaluations[row[2]].created,
                self._evaluations[row[2]].id,
            )
        )
        latest_invocation: dict[tuple[uuid.UUID, uuid.UUID, str], uuid.UUID] = {}
        for identity, invocation_id, _ in rows:
            latest_invocation[identity] = invocation_id
        latest: dict[tuple[uuid.UUID, uuid.UUID, str], list[uuid.UUID]] = {}
        for identity, invocation_id, evaluation_id in rows:
            if invocation_id == latest_invocation[identity]:
                latest.setdefault(identity, []).append(evaluation_id)
        return latest

    async def add_replay_links(
        self, links: Sequence[tuple[uuid.UUID, uuid.UUID]]
    ) -> None:
        """Link replays to evaluations they adopted instead of re-running.

        Args:
            links: (replay_id, evaluation_id) pairs to link.
        """
        self._replay_links.update(links)

    async def list_replay_evaluations(
        self, replay_ids: Sequence[uuid.UUID]
    ) -> list[tuple[uuid.UUID, EvaluationWithEvaluator]]:
        """Load the evaluations linked to a set of replays.

        Args:
            replay_ids: Ids of the replays.

        Returns:
            (replay_id, evaluation) pairs ordered by replay id then
            evaluation id, each evaluation paired with its evaluator name
            and version.
        """
        replay_id_set = set(replay_ids)
        links = sorted(
            (replay_id, evaluation_id)
            for replay_id, evaluation_id in self._replay_links
            if replay_id in replay_id_set
        )
        return [
            (
                replay_id,
                EvaluationWithEvaluator(
                    self._evaluations[evaluation_id].model_copy(),
                    *self._evaluator_info(
                        self._evaluations[evaluation_id].evaluator_version_id
                    ),
                ),
            )
            for replay_id, evaluation_id in links
        ]


async def create_evaluation(
    repository: FakeEvaluationRepository,
    owner_id: uuid.UUID,
    session_id: uuid.UUID,
    name: str = "accuracy",
    data_type: EvaluationDataType = EvaluationDataType.FLOAT,
    score: float | bool | None = 0.9,
    value: str | None = None,
    explanation: str | None = None,
    evaluator_version_id: uuid.UUID | None = None,
) -> Evaluation:
    """Store an evaluation in the fake repository through its create insert.

    Args:
        repository: Fake evaluation repository.
        owner_id: Id of the owning account.
        session_id: Id of the session being scored.
        name: Evaluation name.
        data_type: Data type of the result.
        score: Numeric or boolean score.
        value: Label or string value.
        explanation: Free-form explanation.
        evaluator_version_id: Evaluator version that produced the result.

    Returns:
        Stored evaluation.
    """
    evaluation = Evaluation(
        owner_id=owner_id,
        evaluator_version_id=evaluator_version_id,
        session_id=session_id,
        name=name,
        data_type=data_type,
        score=score,
        value=value,
        explanation=explanation,
    )
    stored = await repository.create_session_evaluations(session_id, [evaluation])
    return stored[0]
