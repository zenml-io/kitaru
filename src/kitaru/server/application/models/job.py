"""Job filters and creation commands."""

import uuid
from typing import Any

from kitaru.server.base import FrozenModel, ListFilter
from kitaru.server.domain.job import JobStatus
from kitaru.server.domain.replay_config import EvaluatorConfig
from kitaru.server.domain.task import TaskKind, TaskStatus


class JobFilter(ListFilter):
    """Job list filter."""

    status: JobStatus | None = None


class JobTasksFilter(ListFilter):
    """Job task list filter."""

    job_id: uuid.UUID
    kind: TaskKind | None = None
    status: TaskStatus | None = None


class SessionRunCreate(FrozenModel):
    """Session-run job command."""

    agent_version_id: uuid.UUID
    inputs: Any = None
    name: str | None = None


class ImportCreate(FrozenModel):
    """Import job command."""

    importer: str
    agent_id: uuid.UUID
    version: int | None = None
    payload_blob_id: uuid.UUID
    params: dict[str, Any]


class EvaluationBatchCreate(FrozenModel):
    """Evaluation job command."""

    input_session_ids: list[uuid.UUID]
    evaluators: list[EvaluatorConfig]
