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
"""Experiment run evaluation aggregation use cases."""

import uuid
from collections import defaultdict

from kitaru.api_models.v1.evaluation import EvaluationDataType
from kitaru.server.application.interfaces.evaluation_repository import (
    EvaluationRepository,
)
from kitaru.server.application.interfaces.experiment_run_repository import (
    ExperimentRunRepository,
)
from kitaru.server.application.interfaces.replay_repository import ReplayRepository
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.domain.evaluation import Evaluation
from kitaru.server.domain.evaluation_aggregate import (
    EvaluationAggregate,
    EvaluationStats,
    ReplayEvaluations,
)

MAX_VALUE_REPLAYS = 50

GroupKey = tuple[str, uuid.UUID | None, EvaluationDataType]


class ExperimentRunEvaluationService:
    """Aggregate pinned evaluation evidence for an experiment run."""

    def __init__(
        self,
        repository: ExperimentRunRepository,
        replay_repository: ReplayRepository,
        evaluation_repository: EvaluationRepository,
    ) -> None:
        """Initialize the service.

        Args:
            repository: Experiment run repository.
            replay_repository: Replay repository.
            evaluation_repository: Evaluation repository.
        """
        self._repository = repository
        self._replays = replay_repository
        self._evaluations = evaluation_repository

    async def list_evaluation_aggregates(
        self, experiment_run_id: uuid.UUID, actor: AuthContext
    ) -> list[EvaluationAggregate]:
        """Aggregate evaluations linked to every replay in a run.

        Args:
            experiment_run_id: Id of the experiment run.
            actor: Caller context.

        Raises:
            ExperimentRunNotFound: No run has this id.

        Returns:
            Groups sorted by name, data type, and evaluator version id, each
            with whole-run statistics and the 50 most recent replay pairs.
        """
        _ = actor
        await self._repository.get(experiment_run_id)
        replays = await self._replays.list_by_experiment_run(experiment_run_id)
        replays.sort(key=lambda replay: replay.id)
        baseline_session_ids = list(
            dict.fromkeys(replay.baseline_session_id for replay in replays)
        )
        result_session_ids = [
            replay.result_session_id
            for replay in replays
            if replay.result_session_id is not None
        ]
        linked = await self._evaluations.list_replay_evaluations(
            [replay.id for replay in replays]
        )

        # One evaluator config per run and pinned adoption yield at most one
        # evaluation per session and group. Task writes reject duplicate names.
        session_evaluations: dict[uuid.UUID, dict[GroupKey, Evaluation]] = defaultdict(
            dict
        )
        evaluator_info: dict[uuid.UUID | None, tuple[str | None, int | None]] = {}
        for _, item in linked:
            evaluation = item.evaluation
            key = (
                evaluation.name,
                evaluation.evaluator_version_id,
                evaluation.data_type,
            )
            session_evaluations[evaluation.session_id][key] = evaluation
            evaluator_info[evaluation.evaluator_version_id] = (
                item.evaluator_name,
                item.evaluator_version,
            )

        baseline_groups: dict[GroupKey, list[Evaluation]] = defaultdict(list)
        for session_id in baseline_session_ids:
            for key, evaluation in session_evaluations.get(session_id, {}).items():
                baseline_groups[key].append(evaluation)
        result_groups: dict[GroupKey, list[Evaluation]] = defaultdict(list)
        for session_id in result_session_ids:
            for key, evaluation in session_evaluations.get(session_id, {}).items():
                result_groups[key].append(evaluation)
        recent = replays[-MAX_VALUE_REPLAYS:]
        aggregates: list[EvaluationAggregate] = []
        for key in sorted(
            set(baseline_groups) | set(result_groups),
            key=lambda group_key: (
                group_key[0],
                group_key[2],
                str(group_key[1]) if group_key[1] is not None else "",
            ),
        ):
            name, evaluator_version_id, data_type = key
            evaluator_name, evaluator_version = evaluator_info.get(
                evaluator_version_id, (None, None)
            )
            aggregates.append(
                EvaluationAggregate(
                    name=name,
                    evaluator_version_id=evaluator_version_id,
                    evaluator_name=evaluator_name,
                    evaluator_version=evaluator_version,
                    data_type=data_type,
                    baseline=EvaluationStats.aggregate(
                        data_type, baseline_groups.get(key, [])
                    ),
                    result=EvaluationStats.aggregate(
                        data_type, result_groups.get(key, [])
                    ),
                    replays=[
                        ReplayEvaluations(
                            replay_id=replay.id,
                            baseline=session_evaluations.get(
                                replay.baseline_session_id, {}
                            ).get(key),
                            result=session_evaluations.get(
                                replay.result_session_id, {}
                            ).get(key)
                            if replay.result_session_id is not None
                            else None,
                        )
                        for replay in recent
                    ],
                )
            )
        return aggregates
