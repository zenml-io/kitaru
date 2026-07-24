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
"""Partial update request helpers."""

from typing import Any

from pydantic import BaseModel


def set_fields(body: BaseModel) -> dict[str, Any]:
    """Collect a request's explicitly set fields by name.

    Args:
        body: Parsed request model.

    Returns:
        Set field values by name, explicit nulls included.
    """
    return {field: getattr(body, field) for field in body.model_fields_set}
