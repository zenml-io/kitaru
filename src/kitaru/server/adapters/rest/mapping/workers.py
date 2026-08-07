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
"""Worker DTO conversions."""

from datetime import datetime

from kitaru.api_models.v1.worker import WorkerListParams, WorkerResponse
from kitaru.server.adapters.rest.mapping.filtering import filter_to_expression
from kitaru.server.application.models.worker import WorkerFilter
from kitaru.server.domain.worker import Worker


def worker_to_response(
    worker: Worker, now: datetime, liveness_timeout_seconds: int
) -> WorkerResponse:
    """Convert a worker entity to its response DTO.

    Args:
        worker: Stored worker.
        now: Current time, against which liveness is derived.
        liveness_timeout_seconds: Liveness window in seconds.

    Returns:
        Worker response.
    """
    assert worker.created is not None
    assert worker.updated is not None
    return WorkerResponse(
        id=worker.id,
        owner_id=worker.owner_id,
        name=worker.name,
        pool_id=worker.pool_id,
        scope=worker.scope,
        runtime=worker.runtime,
        concurrency=worker.concurrency,
        last_seen_at=worker.last_seen_at,
        live=worker.is_live(now, liveness_timeout_seconds),
        metadata=worker.metadata,
        created=worker.created,
        updated=worker.updated,
    )


def worker_list_params_to_filter(params: WorkerListParams) -> WorkerFilter:
    """Convert worker list params to the application filter.

    Args:
        params: Worker list params.

    Returns:
        Worker filter.
    """
    return WorkerFilter(
        expression=filter_to_expression(params.filter)
        if params.filter is not None
        else None,
        cursor=params.cursor,
        size=params.size,
        sort=params.sort,
    )
