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
"""Job environment accessors for agent code."""

import json
import os
from typing import Any

import httpx


def job_id() -> str | None:
    """Return the id of the job this process executes.

    Returns:
        Job id from ``KITARU_JOB_ID``, ``None`` outside job mode.
    """
    return os.environ.get("KITARU_JOB_ID")


def job_inputs() -> Any:
    """Return the inputs of the job this process executes.

    Reads ``KITARU_JOB_INPUTS`` when the runner set it and fetches the job
    spec from the server otherwise.

    Raises:
        RuntimeError: ``KITARU_API_URL`` is not set while the inputs
            require a spec fetch.
        httpx.HTTPStatusError: The spec fetch failed.

    Returns:
        Job inputs, ``None`` outside job mode.
    """
    current_job_id = os.environ.get("KITARU_JOB_ID")
    if current_job_id is None:
        return None
    encoded = os.environ.get("KITARU_JOB_INPUTS")
    if encoded is not None:
        return json.loads(encoded)
    api_url = os.environ.get("KITARU_API_URL")
    if not api_url:
        raise RuntimeError("KITARU_API_URL is not set, cannot fetch the job inputs")
    api_key = os.environ.get("KITARU_API_KEY")
    headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
    response = httpx.get(
        f"{api_url.rstrip('/')}/v1/jobs/{current_job_id}/spec",
        headers=headers,
    )
    response.raise_for_status()
    return response.json()["inputs"]
