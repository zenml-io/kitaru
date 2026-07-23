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
"""Canonical JSON hashing shared by server and client."""

import hashlib
import json
from typing import Any


def canonical_json(value: Any) -> bytes:
    """Serialize a value as canonical JSON.

    Canonical JSON uses sorted keys, compact separators, and UTF-8 encoding.

    Args:
        value: JSON-serializable value.

    Returns:
        Canonical JSON bytes.
    """
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def tool_call_cache_key(tool_name: str | None, inputs: Any) -> str:
    """Compute the cache key of a tool call from its name and inputs.

    Args:
        tool_name: Name of the tool.
        inputs: Tool call inputs.

    Returns:
        SHA-256 hex digest of the canonical JSON payload.
    """
    payload = {"inputs": inputs, "tool": tool_name}
    return hashlib.sha256(canonical_json(payload)).hexdigest()
