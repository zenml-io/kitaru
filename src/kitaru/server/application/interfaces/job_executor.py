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
"""Job executor interface."""

from enum import StrEnum
from typing import Protocol

from pydantic import Field

from kitaru.server.base import FrozenModel


class ExecutorJobStatus(StrEnum):
    """Executor job status."""

    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    UNKNOWN = "unknown"


class ExecutorLaunchRequest(FrozenModel):
    """Executor launch request."""

    image: str
    env: dict[str, str] = Field(default_factory=dict)


class JobExecutor(Protocol):
    """Runner launching operations."""

    async def launch(self, request: ExecutorLaunchRequest) -> str:
        """Launch a runner for pending work.

        Args:
            request: Launch request.

        Returns:
            Opaque executor handle.
        """
        ...

    async def status(self, handle: str) -> ExecutorJobStatus:
        """Report the status of a launched job.

        Args:
            handle: Executor handle.

        Returns:
            Job status.
        """
        ...

    async def cancel(self, handle: str) -> None:
        """Cancel a launched job.

        Args:
            handle: Executor handle.
        """
        ...
