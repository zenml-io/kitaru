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
"""Job settlement queue interface."""

import uuid
from typing import Protocol


class JobSettlementQueue(Protocol):
    """Job settlement check queue operations."""

    async def enqueue(self, job_id: uuid.UUID) -> None:
        """Queue a settlement check for a job.

        Args:
            job_id: Id of the job.
        """
        ...

    async def claim(self, limit: int) -> list[uuid.UUID]:
        """Claim queued settlement checks and drop them from the queue.

        A check another transaction holds is skipped and stays queued for it.

        Args:
            limit: Maximum number of queued checks to claim.

        Returns:
            Distinct job ids of the claimed checks, oldest first.
        """
        ...
