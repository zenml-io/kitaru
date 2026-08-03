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
"""Tool call cache key derivation."""

import hashlib
import json
from typing import Any


def compute_tool_cache_key(tool_name: str, inputs: Any) -> str | None:
    """Compute the cache key for a tool call.

    Args:
        tool_name: Tool name.
        inputs: Tool call inputs.

    Returns:
        SHA-256 hex digest over the tool name and canonical JSON inputs, or
        None when the inputs cannot be canonicalized.
    """
    if inputs is None:
        # We cannot compute a useful cache key in this case
        return None
    try:
        canonical = json.dumps(
            inputs, sort_keys=True, separators=(",", ":"), allow_nan=False
        )
    except (TypeError, ValueError):
        return None
    digest = hashlib.sha256()
    digest.update(tool_name.encode("utf-8"))
    digest.update(b"\x00")
    digest.update(canonical.encode("utf-8"))
    return digest.hexdigest()
