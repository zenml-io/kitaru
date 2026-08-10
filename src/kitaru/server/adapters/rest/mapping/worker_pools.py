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
"""Worker pool DTO conversions."""

from kitaru.api_models.v1.worker_pool import (
    WorkerPoolListParams,
    WorkerPoolResponse,
    WorkerPoolStatsResponse,
    WorkerPoolUpdateRequest,
)
from kitaru.server.adapters.rest.mapping.filtering import filter_to_expression
from kitaru.server.application.models.worker_pool import (
    WorkerPoolFilter,
    WorkerPoolUpdate,
)
from kitaru.server.domain.worker_pool import WorkerPool, WorkerPoolStats


def worker_pool_to_response(worker_pool: WorkerPool) -> WorkerPoolResponse:
    """Convert a worker pool entity to its response DTO.

    Args:
        worker_pool: Stored worker pool.

    Returns:
        Worker pool response.
    """
    assert worker_pool.created is not None
    assert worker_pool.updated is not None
    return WorkerPoolResponse(
        id=worker_pool.id,
        owner_id=worker_pool.owner_id,
        name=worker_pool.name,
        scope=worker_pool.scope,
        created=worker_pool.created,
        updated=worker_pool.updated,
    )


def worker_pool_stats_to_response(stats: WorkerPoolStats) -> WorkerPoolStatsResponse:
    """Convert worker pool stats to their response DTO.

    Args:
        stats: Computed worker pool stats.

    Returns:
        Worker pool stats response.
    """
    return WorkerPoolStatsResponse(
        pending_tasks=stats.pending_tasks,
        in_flight_tasks=stats.in_flight_tasks,
        oldest_pending_seconds=stats.oldest_pending_seconds,
        live_workers=stats.live_workers,
        capacity=stats.capacity,
    )


def worker_pool_list_params_to_filter(
    params: WorkerPoolListParams,
) -> WorkerPoolFilter:
    """Convert worker pool list params to the application filter.

    Args:
        params: Worker pool list params.

    Returns:
        Worker pool filter.
    """
    return WorkerPoolFilter(
        expression=filter_to_expression(params.filter)
        if params.filter is not None
        else None,
        cursor=params.cursor,
        size=params.size,
        sort=params.sort,
    )


def worker_pool_update_to_command(body: WorkerPoolUpdateRequest) -> WorkerPoolUpdate:
    """Convert a worker pool update request to its application command.

    Args:
        body: Worker pool update request.

    Returns:
        Update command carrying only the fields the request set.
    """
    return WorkerPoolUpdate(**body.model_dump(exclude_unset=True))
