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
"""Trigger plugin contract and the trigger flow."""

import uuid
from collections.abc import Callable
from typing import Any

from kitaru.api_models.v1.session import (
    SessionCreateRequest,
    SessionOrigin,
    SessionStatus,
)
from kitaru.api_models.v1.task import TriggerTaskDetails
from kitaru.client.api_client import KitaruAPIClient
from kitaru.task.plugins import PluginLoadError, load_source_ref
from kitaru.task.task_io import write_task_result

__all__ = [
    "Trigger",
    "TriggerError",
    "call_trigger",
    "run",
]

_LABEL = "Trigger"


class TriggerError(Exception):
    """Raised when loading or invoking a trigger function fails."""


Trigger = Callable[[Any, str | None], str]


def call_trigger(
    name: str, trigger: Trigger, inputs: Any, replay_id: str | None
) -> str:
    """Invoke a trigger function and validate its result.

    Args:
        name: Trigger entrypoint, named in error messages.
        trigger: Trigger callable.
        inputs: Inputs passed to the trigger function.
        replay_id: Replay id passed to the trigger function.

    Raises:
        TriggerError: The trigger raised, or did not return a non-empty
            string.

    Returns:
        External id of the resulting session.
    """
    try:
        result = trigger(inputs, replay_id)
    except Exception as exc:
        raise TriggerError(f"Trigger '{name}' raised an error: {exc}") from exc
    if not isinstance(result, str) or not result:
        raise TriggerError(f"Trigger '{name}' did not return a non-empty string")
    return result


async def run(client: KitaruAPIClient, task_id: str) -> None:
    """Run the trigger flow: run the trigger function and create the placeholder.

    Args:
        client: API client.
        task_id: Id of the trigger task.

    Raises:
        TriggerError: The task is not a trigger task, the entrypoint fails
            to load, or the trigger invocation fails validation.
    """
    spec = await client.tasks.get_spec(uuid.UUID(task_id))
    details = spec.details
    if not isinstance(details, TriggerTaskDetails):
        raise TriggerError(f"Task {task_id} is not a trigger task")
    try:
        trigger = load_source_ref(details.entrypoint, _LABEL)
    except PluginLoadError as exc:
        raise TriggerError(str(exc)) from exc

    replay_id = str(details.replay_id) if details.replay_id is not None else None
    external_id = call_trigger(details.entrypoint, trigger, details.inputs, replay_id)

    await client.sessions.create(
        SessionCreateRequest(
            origin=SessionOrigin.REPLAY,
            status=SessionStatus.PENDING_IMPORT,
            external_id=external_id,
            inputs=None,
            outputs=None,
        )
    )
    write_task_result(external_id)
