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
"""Agent-facing accessors for code running inside a task process."""

import json
import os
from typing import Any

import httpx

from kitaru.api_models.v1.task import AgentTaskDetails, TaskSpecResponse
from kitaru.task.task_io import get_required_env


def get_task_id() -> str | None:
    """Get the id of the running task.

    Returns:
        Task id, None outside task mode.
    """
    return os.environ.get("KITARU_TASK_ID")


def get_task_inputs() -> Any:
    """Get the inputs of the running agent task.

    Reads KITARU_TASK_INPUTS when set. Otherwise fetches the task spec with a
    single synchronous request, since this accessor must stay callable from
    inside a running event loop.

    Raises:
        RuntimeError: KITARU_API_URL is not set and the spec fetch fallback
            is needed, or the task is not an agent task.

    Returns:
        Task inputs, None outside task mode.
    """
    task_id = os.environ.get("KITARU_TASK_ID")
    if task_id is None:
        return None
    inputs_env = os.environ.get("KITARU_TASK_INPUTS")
    if inputs_env is not None:
        return json.loads(inputs_env)

    base_url = get_required_env("KITARU_API_URL")
    headers = {}
    token = os.environ.get("KITARU_TASK_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    response = httpx.get(f"{base_url}/v1/tasks/{task_id}/spec", headers=headers)
    response.raise_for_status()
    spec = TaskSpecResponse.model_validate(response.json())
    if not isinstance(spec.details, AgentTaskDetails):
        raise RuntimeError(f"Task {task_id} is not an agent task")
    return spec.details.inputs
