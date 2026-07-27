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
"""Experiment run finalization helpers."""

import uuid

from kitaru.server.application.interfaces.experiment_run_repository import (
    ExperimentRunRepository,
)
from kitaru.server.application.interfaces.job_repository import (
    JobRepository,
)
from kitaru.server.application.interfaces.session_repository import (
    SessionRepository,
)
from kitaru.server.application.models.jobs import JobFilter
from kitaru.server.domain.experiment_run import TERMINAL_RUN_STATUSES
from kitaru.server.domain.job import TERMINAL_JOB_STATUSES, Replay
from kitaru.server.domain.replay_diff import compute_run_summary
from kitaru.server.domain.session import Session

# Page size for resolving every job of an experiment run.
_JOB_RESOLUTION_PAGE_SIZE = 1000


async def load_run_jobs(
    job_repository: JobRepository, run_id: uuid.UUID
) -> list[Replay]:
    """Load every job of an experiment run across all pages.

    Args:
        job_repository: Job repository.
        run_id: Id of the experiment run.

    Returns:
        Jobs of the run.
    """
    jobs: list[Replay] = []
    page = 1
    while True:
        batch, total = await job_repository.query(
            JobFilter(
                experiment_run_id=run_id,
                page=page,
                page_size=_JOB_RESOLUTION_PAGE_SIZE,
            )
        )
        jobs.extend(job for job in batch if isinstance(job, Replay))
        if len(jobs) >= total or not batch:
            return jobs
        page += 1


async def finalize_run_if_drained(
    run_repository: ExperimentRunRepository,
    job_repository: JobRepository,
    session_repository: SessionRepository,
    run_id: uuid.UUID,
) -> None:
    """Finalize an experiment run once its last job went terminal.

    A canceling run lands on canceled, a run with failed or timed out
    jobs on failed, any other run on completed, with the aggregate
    summary computed from the jobs and their sessions. Runs with
    non-terminal jobs stay untouched.

    Args:
        run_repository: Experiment run repository.
        job_repository: Job repository.
        session_repository: Session repository.
        run_id: Id of the experiment run.
    """
    run = await run_repository.get(run_id)
    if run.status in TERMINAL_RUN_STATUSES:
        return
    jobs = await load_run_jobs(job_repository, run_id)
    if any(job.status not in TERMINAL_JOB_STATUSES for job in jobs):
        return
    sessions: dict[uuid.UUID, Session] = {}
    for job in jobs:
        for session_id in (job.input_session_id, job.result_session_id):
            if session_id is not None and session_id not in sessions:
                sessions[session_id] = await session_repository.get(session_id)
    run.finalize(
        compute_run_summary(jobs, sessions),
        [job.status for job in jobs],
    )
    await run_repository.update(run)
