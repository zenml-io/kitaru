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
"""Experiment run DTO conversions."""

from kitaru.api_models.v1.experiment_runs import (
    ExperimentRunProgress as ExperimentRunProgressModel,
)
from kitaru.api_models.v1.experiment_runs import (
    ExperimentRunResponse,
)
from kitaru.api_models.v1.experiment_runs import (
    ExperimentRunStatus as ExperimentRunStatusModel,
)
from kitaru.server.domain.experiment_run import (
    ExperimentRun,
    ExperimentRunProgress,
    ExperimentRunStatus,
)


def run_status_to_domain(
    status: ExperimentRunStatusModel | None,
) -> ExperimentRunStatus | None:
    """Convert an optional run status DTO to its domain enum.

    Args:
        status: Run status DTO.

    Returns:
        Domain run status, ``None`` for ``None``.
    """
    if status is None:
        return None
    return ExperimentRunStatus(status.value)


def progress_to_response(
    progress: ExperimentRunProgress,
) -> ExperimentRunProgressModel:
    """Convert a domain progress to its DTO.

    Args:
        progress: Domain progress.

    Returns:
        Progress DTO.
    """
    return ExperimentRunProgressModel(
        pending=progress.pending,
        claimed=progress.claimed,
        running=progress.running,
        completed=progress.completed,
        failed=progress.failed,
        timed_out=progress.timed_out,
        canceled=progress.canceled,
        total=progress.total,
    )


def experiment_run_to_response(
    run: ExperimentRun, progress: ExperimentRunProgress
) -> ExperimentRunResponse:
    """Convert an experiment run entity to its response DTO.

    Args:
        run: Stored experiment run.
        progress: Computed replay counts of the run.

    Returns:
        Experiment run response.
    """
    assert run.created is not None
    assert run.updated is not None
    return ExperimentRunResponse(
        id=run.id,
        owner_id=run.owner_id,
        experiment_id=run.experiment_id,
        number=run.number,
        status=ExperimentRunStatusModel(run.status.value),
        agent_version_id=run.agent_version_id,
        score_baselines=run.score_baselines,
        started_at=run.started_at,
        ended_at=run.ended_at,
        summary=run.summary,
        error=run.error,
        progress=progress_to_response(progress),
        created=run.created,
        updated=run.updated,
    )
