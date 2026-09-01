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

from kitaru.api_models.v1.experiment_run import (
    ExperimentRunCreateRequest,
    ExperimentRunJobsListParams,
    ExperimentRunListParams,
    ExperimentRunProgress,
    ExperimentRunResponse,
)
from kitaru.api_models.v1.replay import BaselineEvaluationMode
from kitaru.server.adapters.rest.mapping.filtering import filter_to_expression
from kitaru.server.adapters.rest.mapping.replays import resolve_baseline_evaluation_mode
from kitaru.server.application.models.experiment_run import (
    ExperimentRunCreate,
    ExperimentRunFilter,
    ExperimentRunJobsFilter,
)
from kitaru.server.application.models.replay import ReplayStatusCounts
from kitaru.server.domain.experiment_run import ExperimentRun


def experiment_run_create_to_command(
    body: ExperimentRunCreateRequest,
) -> ExperimentRunCreate:
    """Convert an experiment run create request to its application command.

    Args:
        body: Experiment run create request.

    Returns:
        Create command.
    """
    return ExperimentRunCreate(
        cohort_version_id=body.cohort_version_id,
        agent_version_id=body.agent_version_id,
        baseline_evaluation_mode=resolve_baseline_evaluation_mode(body),
    )


def _progress_to_wire(counts: ReplayStatusCounts) -> ExperimentRunProgress:
    """Convert application replay status counts to the wire progress DTO.

    Args:
        counts: Replay counts by status.

    Returns:
        Wire progress.
    """
    return ExperimentRunProgress(
        pending=counts.pending,
        evaluating=counts.evaluating,
        completed=counts.completed,
        failed=counts.failed,
        canceled=counts.canceled,
        total=counts.total,
    )


def experiment_run_to_response(
    run: ExperimentRun, counts: ReplayStatusCounts
) -> ExperimentRunResponse:
    """Convert an experiment run and its replay counts to the response DTO.

    Args:
        run: Stored experiment run.
        counts: The run's replay counts by status.

    Returns:
        Experiment run response, inlining progress.
    """
    assert run.created is not None
    assert run.updated is not None
    return ExperimentRunResponse(
        id=run.id,
        owner_id=run.owner_id,
        experiment_id=run.experiment_id,
        number=run.number,
        status=run.status,
        cohort_version_id=run.cohort_version_id,
        agent_version_id=run.agent_version_id,
        evaluate_baselines=run.baseline_evaluation_mode
        is not BaselineEvaluationMode.NONE,
        baseline_evaluation_mode=run.baseline_evaluation_mode,
        started_at=run.started_at,
        ended_at=run.ended_at,
        error=run.error,
        progress=_progress_to_wire(counts),
        created=run.created,
        updated=run.updated,
    )


def experiment_run_list_params_to_filter(
    params: ExperimentRunListParams,
) -> ExperimentRunFilter:
    """Convert experiment run list params to the application filter.

    Args:
        params: Experiment run list params.

    Returns:
        Experiment run filter.
    """
    return ExperimentRunFilter(
        expression=filter_to_expression(params.filter)
        if params.filter is not None
        else None,
        cursor=params.cursor,
        size=params.size,
        sort=params.sort,
    )


def experiment_run_jobs_list_params_to_filter(
    params: ExperimentRunJobsListParams,
) -> ExperimentRunJobsFilter:
    """Convert experiment run jobs list params to the application filter.

    Args:
        params: Experiment run jobs list params.

    Returns:
        Experiment run jobs filter.
    """
    return ExperimentRunJobsFilter(
        expression=filter_to_expression(params.filter)
        if params.filter is not None
        else None,
        cursor=params.cursor,
        size=params.size,
        sort=params.sort,
    )
