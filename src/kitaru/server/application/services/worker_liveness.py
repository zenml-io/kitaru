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
"""Worker liveness helpers."""

import logging
import uuid
from datetime import UTC, datetime, timedelta

from kitaru.server.application.interfaces.worker_repository import (
    WorkerRepository,
)
from kitaru.server.application.models.workers import WorkerFilter

logger = logging.getLogger(__name__)


async def warn_if_no_live_worker(
    repository: WorkerRepository,
    agent_version_id: uuid.UUID,
    liveness_timeout_seconds: int,
) -> None:
    """Log a warning when no live worker serves an agent version.

    Args:
        repository: Worker repository.
        agent_version_id: Id of the agent version.
        liveness_timeout_seconds: Seconds after which a worker counts as
            dead.
    """
    seen_after = datetime.now(UTC) - timedelta(seconds=liveness_timeout_seconds)
    _, total = await repository.query(
        WorkerFilter(
            agent_version_id=agent_version_id, seen_after=seen_after, page_size=1
        )
    )
    if total == 0:
        logger.warning("No live worker serves agent version %s", agent_version_id)
