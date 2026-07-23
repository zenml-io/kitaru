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
"""Replay environment accessors for agent code."""

import json
import os
from typing import Any

import httpx


def replay_id() -> str | None:
    """Return the id of the replay this process executes.

    Returns:
        Replay id from ``KITARU_REPLAY_ID``, ``None`` outside replay mode.
    """
    return os.environ.get("KITARU_REPLAY_ID")


def replay_inputs() -> Any:
    """Return the inputs of the replay this process executes.

    Reads ``KITARU_INPUTS`` when the runner set it and fetches the replay
    spec from the server otherwise.

    Raises:
        RuntimeError: ``KITARU_API_URL`` is not set while the inputs
            require a spec fetch.
        httpx.HTTPStatusError: The spec fetch failed.

    Returns:
        Replay inputs, ``None`` outside replay mode.
    """
    current_replay_id = os.environ.get("KITARU_REPLAY_ID")
    if current_replay_id is None:
        return None
    encoded = os.environ.get("KITARU_INPUTS")
    if encoded is not None:
        return json.loads(encoded)
    api_url = os.environ.get("KITARU_API_URL")
    if not api_url:
        raise RuntimeError("KITARU_API_URL is not set, cannot fetch the replay inputs")
    api_key = os.environ.get("KITARU_API_KEY")
    headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
    response = httpx.get(
        f"{api_url.rstrip('/')}/v1/replays/{current_replay_id}/spec",
        headers=headers,
    )
    response.raise_for_status()
    return response.json()["inputs"]
