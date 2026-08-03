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
"""Task process env reading and result-file writing."""

import json
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from kitaru.env import get_required_env


def write_task_result(value: Any) -> None:
    """Write a task's result as JSON to the path in KITARU_TASK_RESULT_PATH.

    Args:
        value: A BaseModel, a list of BaseModels, or a plain JSON value.
    """
    if isinstance(value, BaseModel):
        encoded: Any = value.model_dump(mode="json")
    elif isinstance(value, list):
        encoded = [
            item.model_dump(mode="json") if isinstance(item, BaseModel) else item
            for item in value
        ]
    else:
        encoded = value
    path = Path(get_required_env("KITARU_TASK_RESULT_PATH"))
    path.write_text(json.dumps(encoded))
