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
"""Agent-facing task process accessors."""

import json
import os
from typing import Any

import httpx


def get_task_id() -> str | None:
    """Get the current task id.

    Returns:
        The task id, or ``None`` outside a task process.
    """
    return os.environ.get("KITARU_TASK_ID") or None


def get_task_inputs() -> Any:
    """Get the inputs for the current task.

    Small inputs are supplied directly through the process environment. Larger
    inputs are fetched synchronously so this accessor also works while the
    caller has a running event loop.

    Raises:
        RuntimeError: The API URL is missing when the spec fallback is needed.
        httpx.HTTPStatusError: The task spec request failed.

    Returns:
        The task inputs, or ``None`` outside a task process.
    """
    task_id = get_task_id()
    if task_id is None:
        return None

    encoded_inputs = os.environ.get("KITARU_TASK_INPUTS")
    if encoded_inputs is not None:
        return json.loads(encoded_inputs)

    api_url = os.environ.get("KITARU_API_URL")
    if not api_url:
        raise RuntimeError("KITARU_API_URL is not set")

    headers: dict[str, str] = {}
    api_key = os.environ.get("KITARU_API_KEY")
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    response = httpx.get(
        f"{api_url.rstrip('/')}/v1/tasks/{task_id}/spec",
        headers=headers,
    )
    response.raise_for_status()
    payload = response.json()
    return payload["details"]["inputs"]
