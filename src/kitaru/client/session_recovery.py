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
"""Result session recovery for a task principal's retried session create."""

import uuid

import httpx

from kitaru.api_models.v1.session import SessionCreateRequest, SessionResponse
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError


async def create_or_get_result_session(
    client: KitaruAPIClient,
    request: SessionCreateRequest,
    task_id: uuid.UUID | None,
) -> SessionResponse:
    """Create a session, recovering from a task's already-linked result session.

    A retry of a task's session create can 409 when the first attempt
    committed and linked the task's result session but its response was
    lost. When task_id is set, that 409 is resolved by reading the task's
    result session instead of failing the retry.

    Args:
        client: API client used to send requests.
        request: Session create request.
        task_id: Id of the task principal creating the session, None outside
            a task.

    Raises:
        APIError: The create failed for a reason other than a linked result
            session, or the task has no result session to recover.

    Returns:
        Created or recovered session.
    """
    try:
        return await client.sessions.create(request)
    except APIError as error:
        if task_id is None or error.status_code != httpx.codes.CONFLICT:
            raise
        task = await client.tasks.get(task_id)
        if task.result_session_id is None:
            raise
        return await client.sessions.get(task.result_session_id)
