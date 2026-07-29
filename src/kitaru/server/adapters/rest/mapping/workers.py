"""Worker DTO conversions."""

from kitaru.api_models.v1.worker import (
    WorkerListParams,
    WorkerResponse,
)
from kitaru.api_models.v1.worker import (
    WorkerRuntime as WorkerRuntimeDTO,
)
from kitaru.server.application.models.worker import WorkerFilter
from kitaru.server.domain.worker import Worker, WorkerRuntime


def worker_to_response(worker: Worker, live: bool) -> WorkerResponse:
    """Convert a worker entity and computed liveness to a response."""
    assert worker.created is not None
    assert worker.updated is not None
    return WorkerResponse(
        id=worker.id,
        owner_id=worker.owner_id,
        name=worker.name,
        scope=worker.scope,
        runtime=WorkerRuntimeDTO.model_validate(worker.runtime.model_dump()),
        last_seen_at=worker.last_seen_at,
        live=live,
        metadata=worker.metadata,
        created=worker.created,
        updated=worker.updated,
    )


def worker_runtime_to_domain(runtime: WorkerRuntimeDTO) -> WorkerRuntime:
    """Convert an API worker runtime to its domain counterpart."""
    return WorkerRuntime.model_validate(runtime.model_dump(mode="python"))


def worker_list_params_to_filter(params: WorkerListParams) -> WorkerFilter:
    """Convert worker list query parameters."""
    return WorkerFilter(**params.model_dump(mode="python"))
