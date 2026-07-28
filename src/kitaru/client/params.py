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
"""Query parameter building."""

import uuid
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any


def _serialize(value: Any) -> Any:
    """Serialize a parameter value for the query string.

    Args:
        value: Parameter value.

    Returns:
        Query-string-compatible value.
    """
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return value


def build_params(**params: Any) -> dict[str, Any]:
    """Build query parameters, dropping None values.

    Args:
        **params: Parameter values.

    Returns:
        Query parameters.
    """
    return {
        key: _serialize(value) for key, value in params.items() if value is not None
    }
