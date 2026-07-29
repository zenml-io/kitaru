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
"""Tasks resource."""

import uuid
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.task import (
    TaskClaimRequest,
    TaskClaimResponse,
    TaskListParams,
    TaskResponse,
    TaskSpecResponse,
    TaskUpdateRequest,
)

if TYPE_CHECKING:
    from kitaru.client.api_client import KitaruAPIClient


class TasksResource:
    """Task API methods."""

    def __init__(self, client: "KitaruAPIClient") -> None:
        """Initialize the resource.

        Args:
            client: API client used to send requests.
        """
        self._client = client

    async def claim(self, request: TaskClaimRequest) -> TaskClaimResponse:
        """Claim pending tasks matching the worker's stored scope.

        Args:
            request: Task claim request.

        Raises:
            APIError: The request failed, including 404 for a missing worker.

        Returns:
            Claimed tasks with their execution specs.
        """
        response = await self._client.request(
            "POST",
            "/v1/tasks/claim",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return TaskClaimResponse.model_validate(response.json())

    async def get(self, task_id: uuid.UUID) -> TaskResponse:
        """Get a task by id.

        Args:
            task_id: Id of the task.

        Raises:
            APIError: The request failed, including 404 for a missing task.

        Returns:
            Stored task.
        """
        response = await self._client.request("GET", f"/v1/tasks/{task_id}")
        return TaskResponse.model_validate(response.json())

    async def get_spec(self, task_id: uuid.UUID) -> TaskSpecResponse:
        """Get the execution spec of a task.

        Args:
            task_id: Id of the task.

        Raises:
            APIError: The request failed, including 404 for a missing task.

        Returns:
            Execution spec.
        """
        response = await self._client.request("GET", f"/v1/tasks/{task_id}/spec")
        return TaskSpecResponse.model_validate(response.json())

    async def list(self, params: TaskListParams | None = None) -> Page[TaskResponse]:
        """List tasks.

        Args:
            params: Task list params.

        Raises:
            APIError: The request failed.

        Returns:
            Page of tasks.
        """
        params = params or TaskListParams()
        response = await self._client.request(
            "GET",
            "/v1/tasks",
            params=params.model_dump(mode="json", exclude_unset=True),
        )
        return Page[TaskResponse].model_validate(response.json())

    async def iter(
        self, params: TaskListParams | None = None
    ) -> AsyncIterator[TaskResponse]:
        """Iterate over all tasks.

        Args:
            params: Task list params.

        Raises:
            APIError: The request failed.

        Returns:
            Async iterator over every task.
        """
        params = params or TaskListParams()
        while True:
            page = await self.list(params)
            for item in page.items:
                yield item
            if page.next_cursor is None:
                break
            params = params.model_copy(update={"cursor": page.next_cursor})

    async def update(
        self, task_id: uuid.UUID, request: TaskUpdateRequest
    ) -> TaskResponse:
        """Apply an executor transition to a task.

        Args:
            task_id: Id of the task.
            request: Task update request.

        Raises:
            APIError: The request failed, including 409 when the attempt does
                not match or the transition is illegal.

        Returns:
            Task carrying its new status.
        """
        response = await self._client.request(
            "PATCH",
            f"/v1/tasks/{task_id}",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return TaskResponse.model_validate(response.json())
