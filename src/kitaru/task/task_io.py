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
"""Task process environment and result-file helpers."""

import json
import os
from pathlib import Path
from typing import Any

from pydantic import BaseModel


def get_required_env(name: str) -> str:
    """Read a required task process environment variable.

    Args:
        name: Environment variable name.

    Raises:
        RuntimeError: The variable is missing or empty.

    Returns:
        Environment variable value.
    """
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} is not set")
    return value


def _json_value(value: Any) -> Any:
    """Convert task result models to JSON-compatible values."""
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, list):
        return [_json_value(item) for item in value]
    return value


def write_task_result(value: Any) -> None:
    """Write the task result as JSON.

    Args:
        value: Pydantic model, list of models, or plain JSON value.
    """
    path = Path(get_required_env("KITARU_TASK_RESULT_PATH"))
    path.write_text(json.dumps(_json_value(value)), encoding="utf-8")
