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
"""Job routes."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query, status

from kitaru.api_models.v1.agent_versions import ExecutionTarget
from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.jobs import (
    ClaimedJobResponse,
    JobClaimRequest,
    JobClaimResponse,
    JobKind,
    JobResponse,
    JobSpecResponse,
    JobStatus,
    JobUpdateRequest,
    ToolLookupRequest,
    ToolLookupResponse,
)
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_job_service,
)
from kitaru.server.adapters.rest.mapping.experiment_runs import (
    execution_target_to_domain,
)
from kitaru.server.adapters.rest.mapping.jobs import (
    job_kind_to_domain,
    job_spec_to_response,
    job_status_to_domain,
    job_to_response,
    job_update_to_command,
    tool_lookup_to_response,
    worker_scope_to_domain,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.jobs import JobFilter
from kitaru.server.application.services.job_service import JobService

router = APIRouter()


@router.get("")
async def list_jobs(
    service: Annotated[JobService, Depends(get_job_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    experiment_run_id: uuid.UUID | None = None,
    input_session_id: uuid.UUID | None = None,
    kind: JobKind | None = None,
    job_status: Annotated[JobStatus | None, Query(alias="status")] = None,
    standalone: bool | None = None,
    worker_id: uuid.UUID | None = None,
    execution_target: ExecutionTarget | None = None,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=1000)] = 20,
) -> Page[JobResponse]:
    """List jobs.

    Clients observe HTTP 200 on success and 422 on invalid pagination
    parameters.

    Args:
        service: Job service.
        actor: Caller context.
        experiment_run_id: Filter on experiment run id.
        input_session_id: Filter on the session the job reads.
        kind: Filter on job kind.
        job_status: Filter on job status.
        standalone: Filter on standalone jobs.
        worker_id: Filter on the claiming worker id.
        execution_target: Filter on execution target.
        page: Page number.
        page_size: Page size.

    Returns:
        Page of jobs.
    """
    job_filter = JobFilter(
        experiment_run_id=experiment_run_id,
        input_session_id=input_session_id,
        kind=job_kind_to_domain(kind),
        status=job_status_to_domain(job_status),
        standalone=standalone,
        worker_id=worker_id,
        execution_target=execution_target_to_domain(execution_target),
        page=page,
        page_size=page_size,
    )
    jobs, total = await service.list_jobs(job_filter, actor=actor)
    return Page[JobResponse](
        items=[job_to_response(job) for job in jobs],
        total=total,
        page=page,
        page_size=page_size,
    )


@router.post("/claim")
async def claim_jobs(
    body: JobClaimRequest,
    service: Annotated[JobService, Depends(get_job_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> JobClaimResponse:
    """Atomically claim pending jobs within a scope for a worker.

    Stale claimed or running jobs in scope are requeued or timed out
    first, and the claim bumps the worker's last seen time. An unpinned
    scope yields only pool-target work. With an experiment run id the
    first claim moves a pending run to running, and canceling and
    terminal runs yield no jobs. Every claimed job ships with the spec the
    runner executes it with, and a job whose spec does not resolve fails
    and drops out of the response.

    Clients observe HTTP 200 on success, 404 when no worker or scoped
    experiment run has the referenced id, and 422 on invalid input.

    Args:
        body: Job claim request.
        service: Job service.
        actor: Caller context.

    Returns:
        Claimed jobs with their specs.
    """
    jobs = await service.claim_jobs(
        worker_id=body.worker_id,
        max_jobs=body.max_jobs,
        scope=worker_scope_to_domain(body.scope),
        actor=actor,
    )
    return JobClaimResponse(
        jobs=[
            ClaimedJobResponse(
                job=job_to_response(job), spec=job_spec_to_response(spec)
            )
            for job, spec in jobs
        ]
    )


@router.get("/{job_id}")
async def get_job(
    job_id: uuid.UUID,
    service: Annotated[JobService, Depends(get_job_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> JobResponse:
    """Get a job by id.

    Clients observe HTTP 200 on success and 404 when no job has this
    id.

    Args:
        job_id: Id of the job.
        service: Job service.
        actor: Caller context.

    Returns:
        Stored job.
    """
    job = await service.get_job(job_id, actor=actor)
    return job_to_response(job)


@router.get("/{job_id}/spec")
async def get_job_spec(
    job_id: uuid.UUID,
    service: Annotated[JobService, Depends(get_job_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> JobSpecResponse:
    """Resolve the spec a runner executes a job with.

    The spec includes the resolved secret environment of the run spec's
    secrets.

    Clients observe HTTP 200 on success, 404 when no job has this id,
    and 409 when the stamped agent version has no run spec.

    Args:
        job_id: Id of the job.
        service: Job service.
        actor: Caller context.

    Returns:
        Resolved job spec.
    """
    spec = await service.get_spec(job_id, actor=actor)
    return job_spec_to_response(spec)


@router.patch("/{job_id}")
async def update_job(
    job_id: uuid.UUID,
    body: JobUpdateRequest,
    service: Annotated[JobService, Depends(get_job_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> JobResponse:
    """Transition a job through the runner status updates.

    A completing replay job fans out its score jobs within the same
    request. The transition that makes the last job of a run terminal also
    finalizes the run.

    Clients observe HTTP 200 on success, 404 when no job has this id,
    409 when the transition is illegal or the completion carries no
    result the kind requires, and 422 when failing without an error.

    Args:
        job_id: Id of the job.
        body: Job update request.
        service: Job service.
        actor: Caller context.

    Returns:
        Updated job.
    """
    job = await service.update_job(job_id, job_update_to_command(body), actor=actor)
    return job_to_response(job)


@router.delete("/{job_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_job(
    job_id: uuid.UUID,
    service: Annotated[JobService, Depends(get_job_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> None:
    """Delete a standalone job.

    Deletes the replay config of a replay job when nothing else references
    it.

    Clients observe HTTP 204 on success, 404 when no job has this id,
    and 409 when the job belongs to an experiment run or is claimed or
    running.

    Args:
        job_id: Id of the job.
        service: Job service.
        actor: Caller context.
    """
    await service.delete_job(job_id, actor=actor)


@router.post("/{job_id}/release")
async def release_job(
    job_id: uuid.UUID,
    service: Annotated[JobService, Depends(get_job_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> JobResponse:
    """Requeue a claimed or running job for another attempt.

    Clients observe HTTP 200 on success, 404 when no job has this id,
    and 409 when the job is not claimed or running.

    Args:
        job_id: Id of the job.
        service: Job service.
        actor: Caller context.

    Returns:
        Requeued job.
    """
    job = await service.release_job(job_id, actor=actor)
    return job_to_response(job)


@router.post("/{job_id}/retry")
async def retry_job(
    job_id: uuid.UUID,
    service: Annotated[JobService, Depends(get_job_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> JobResponse:
    """Requeue a finished standalone job for another attempt.

    A replay job drops its score jobs, so the next completion fans out
    again.

    Clients observe HTTP 200 on success, 404 when no job has this id,
    and 409 when the job belongs to an experiment run or is not
    failed, timed out, or canceled.

    Args:
        job_id: Id of the job.
        service: Job service.
        actor: Caller context.

    Returns:
        Requeued job.
    """
    job = await service.retry_job(job_id, actor=actor)
    return job_to_response(job)


@router.post("/{job_id}/tool-lookup")
async def tool_lookup(
    job_id: uuid.UUID,
    body: ToolLookupRequest,
    service: Annotated[JobService, Depends(get_job_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ToolLookupResponse:
    """Resolve a history tool policy lookup within its scope.

    Clients observe HTTP 200 on success, including misses, 404 when no
    job has this id, and 422 when the cache key does not match the tool
    name and inputs, the tool resolves to no history policy, or a
    standalone job scopes to a cohort.

    Args:
        job_id: Id of the job.
        body: Tool lookup request.
        service: Job service.
        actor: Caller context.

    Returns:
        Tool lookup response.
    """
    node = await service.tool_lookup(
        job_id,
        tool_name=body.tool_name,
        inputs=body.inputs,
        cache_key=body.cache_key,
        actor=actor,
    )
    return tool_lookup_to_response(node)
