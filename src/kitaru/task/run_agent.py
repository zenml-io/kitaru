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
"""Run function contract and the run-agent flow."""

import uuid
from collections.abc import Callable
from typing import Any

from kitaru.api_models.v1.session import (
    SessionCreateRequest,
    SessionOrigin,
    SessionStatus,
)
from kitaru.api_models.v1.task import FunctionAgentTaskDetails
from kitaru.client.api_client import KitaruAPIClient
from kitaru.task.plugins import PluginLoadError, load_source_ref
from kitaru.task.task_io import write_task_result

__all__ = [
    "AgentRunError",
    "RunFunction",
    "call_run_function",
    "run",
]

_LABEL = "Run function"


class AgentRunError(Exception):
    """Raised when loading or invoking the run function fails."""


RunFunction = Callable[[Any, str | None], str]


def call_run_function(
    name: str, run_function: RunFunction, inputs: Any, replay_id: str | None
) -> str:
    """Invoke the run function and validate its result.

    Args:
        name: Run function entrypoint, named in error messages.
        run_function: Run function callable.
        inputs: Inputs passed to the run function.
        replay_id: Replay id passed to the run function.

    Raises:
        AgentRunError: The run function raised, or did not return a
            non-empty string.

    Returns:
        External id of the resulting session.
    """
    try:
        result = run_function(inputs, replay_id)
    except Exception as exc:
        raise AgentRunError(f"Run function '{name}' raised an error: {exc}") from exc
    if not isinstance(result, str) or not result:
        raise AgentRunError(f"Run function '{name}' did not return a non-empty string")
    return result


async def run(client: KitaruAPIClient, task_id: str) -> None:
    """Run the run-agent flow: call the run function and create the placeholder.

    Args:
        client: API client.
        task_id: Id of the agent task.

    Raises:
        AgentRunError: The task is not a function agent task, the entrypoint
            fails to load, or the run function invocation fails validation.
    """
    spec = await client.tasks.get_spec(uuid.UUID(task_id))
    details = spec.details
    if not isinstance(details, FunctionAgentTaskDetails):
        raise AgentRunError(f"Task {task_id} is not a function agent task")
    try:
        run_function = load_source_ref(details.entrypoint, _LABEL)
    except PluginLoadError as exc:
        raise AgentRunError(str(exc)) from exc

    replay_id = str(details.replay_id) if details.replay_id is not None else None
    external_id = call_run_function(
        details.entrypoint, run_function, details.inputs, replay_id
    )

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
