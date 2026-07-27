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

from kitaru.api_models.v1.workers import WorkerResponse
from kitaru.server.adapters.rest.mapping.jobs import worker_scope_to_response
from kitaru.server.domain.worker import Worker


def worker_to_response(worker: Worker, liveness_timeout_seconds: int) -> WorkerResponse:
    """Convert a worker entity to its response DTO.

    Args:
        worker: Stored worker.
        liveness_timeout_seconds: Seconds after which a worker counts as
            dead.

    Returns:
        Worker response.
    """
    assert worker.created is not None
    assert worker.updated is not None
    return WorkerResponse(
        id=worker.id,
        owner_id=worker.owner_id,
        name=worker.name,
        scope=worker_scope_to_response(worker.scope),
        last_seen_at=worker.last_seen_at,
        live=worker.is_live(liveness_timeout_seconds),
        metadata=worker.metadata,
        created=worker.created,
        updated=worker.updated,
    )
