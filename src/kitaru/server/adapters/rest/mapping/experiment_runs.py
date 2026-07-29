"""Experiment run DTO conversions."""

import uuid

from kitaru.api_models.v1.experiment_run import (
    ExperimentRunJobsListParams,
    ExperimentRunListParams,
    ExperimentRunResponse,
)
from kitaru.api_models.v1.experiment_run import (
    ExperimentRunProgress as ExperimentRunProgressDTO,
)
from kitaru.server.application.models.experiment_run import (
    ExperimentRunFilter,
    ExperimentRunJobsFilter,
)
from kitaru.server.domain.experiment_run import (
    ExperimentRun,
    ExperimentRunProgress,
)


def experiment_run_to_response(
    run: ExperimentRun, progress: ExperimentRunProgress
) -> ExperimentRunResponse:
    """Convert an experiment run and progress to a response."""
    assert run.created is not None
    assert run.updated is not None
    return ExperimentRunResponse(
        id=run.id,
        owner_id=run.owner_id,
        experiment_id=run.experiment_id,
        number=run.number,
        status=run.status,
        cohort_id=run.cohort_id,
        agent_version_id=run.agent_version_id,
        evaluate_baselines=run.evaluate_baselines,
        started_at=run.started_at,
        ended_at=run.ended_at,
        error=run.error,
        progress=ExperimentRunProgressDTO.model_validate(progress.model_dump()),
        created=run.created,
        updated=run.updated,
    )


def experiment_run_list_params_to_filter(
    params: ExperimentRunListParams,
) -> ExperimentRunFilter:
    """Convert experiment run list query parameters."""
    return ExperimentRunFilter(**params.model_dump(mode="python"))


def experiment_run_jobs_params_to_filter(
    run_id: uuid.UUID, params: ExperimentRunJobsListParams
) -> ExperimentRunJobsFilter:
    """Convert run job list query parameters."""
    return ExperimentRunJobsFilter(
        experiment_run_id=run_id,
        **params.model_dump(mode="python"),
    )
