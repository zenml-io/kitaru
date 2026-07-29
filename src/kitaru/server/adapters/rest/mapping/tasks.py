"""Task DTO conversions."""

from kitaru.api_models.v1.task import (
    TaskListParams,
    TaskResponse,
    TaskSpecResponse,
    TaskUpdateRequest,
)
from kitaru.server.adapters.rest.mapping.partial import to_partial
from kitaru.server.application.models.task import TaskFilter, TaskUpdate
from kitaru.server.domain.task import (
    AgentTask,
    EvaluationTask,
    ImportTask,
    Task,
    TaskSpec,
)


def task_to_response(task: Task) -> TaskResponse:
    """Convert a concrete task entity to its response."""
    assert task.created is not None
    assert task.updated is not None
    return TaskResponse(
        id=task.id,
        job_id=task.job_id,
        kind=task.kind,
        status=task.status,
        on_failure=task.on_failure,
        attempt=task.attempt,
        labels=task.labels,
        agent_version_id=(
            task.agent_version_id if isinstance(task, AgentTask) else None
        ),
        plugin_version_id=(
            task.plugin_version_id
            if isinstance(task, EvaluationTask | ImportTask)
            else None
        ),
        payload_blob_id=(
            task.payload_blob_id if isinstance(task, ImportTask) else None
        ),
        input_session_id=(
            task.input_session_id if isinstance(task, EvaluationTask) else None
        ),
        agent_id=task.agent_id if isinstance(task, ImportTask) else None,
        worker_id=task.worker_id,
        result_session_id=task.result_session_id,
        claimed_at=task.claimed_at,
        heartbeat_at=task.heartbeat_at,
        cancel_requested_at=task.cancel_requested_at,
        started_at=task.started_at,
        ended_at=task.ended_at,
        error=task.error,
        result=task.result,
        created=task.created,
        updated=task.updated,
    )


def task_spec_to_response(spec: TaskSpec) -> TaskSpecResponse:
    """Convert a resolved task specification to its response."""
    return TaskSpecResponse.model_validate(spec.model_dump(mode="python"))


def task_list_params_to_filter(params: TaskListParams) -> TaskFilter:
    """Convert task list query parameters."""
    return TaskFilter(**params.model_dump(mode="python"))


def task_update_to_command(body: TaskUpdateRequest) -> TaskUpdate:
    """Convert a task PATCH body while preserving unset fields."""
    return to_partial(TaskUpdate, body)
