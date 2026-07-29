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
"""Stable cache keys shared by recording and replay lookup."""

import hashlib
import json
from typing import Any


def compute_tool_cache_key(tool_name: str, inputs: Any) -> str:
    """Compute the cache key for a tool call.

    Args:
        tool_name: Tool name.
        inputs: JSON-compatible tool inputs.

    Returns:
        SHA-256 hex digest over the tool name and canonical inputs.
    """
    payload = json.dumps(
        [tool_name, inputs],
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
