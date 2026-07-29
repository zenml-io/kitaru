"""Experiment run filters."""

import uuid

from kitaru.server.base import ListFilter
from kitaru.server.domain.experiment_run import ExperimentRunStatus
from kitaru.server.domain.job import JobStatus


class ExperimentRunFilter(ListFilter):
    """Experiment run list filter."""

    experiment_id: uuid.UUID | None = None
    status: ExperimentRunStatus | None = None
    tag: str | None = None


class ExperimentRunJobsFilter(ListFilter):
    """Run job list filter."""

    experiment_run_id: uuid.UUID
    status: JobStatus | None = None
