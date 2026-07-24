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
"""Import job repository interface."""

import uuid
from typing import Protocol

from kitaru.server.domain.import_job import ImportJob


class ImportJobRepository(Protocol):
    """Import job persistence operations."""

    async def create(self, job: ImportJob) -> ImportJob:
        """Persist a new job."""
        ...

    async def get(self, job_id: uuid.UUID) -> ImportJob:
        """Load a job by id."""
        ...

    async def update(self, job: ImportJob) -> ImportJob:
        """Persist job changes."""
        ...

    async def claim_next(self, worker_id: str) -> ImportJob | None:
        """Claim the oldest pending job."""
        ...
