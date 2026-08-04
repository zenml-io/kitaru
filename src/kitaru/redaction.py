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
"""Base-safe redaction of recognizable credentials and secret fields."""

import re
from datetime import date, datetime
from enum import Enum
from pathlib import Path
from typing import Any
from uuid import UUID

from pydantic import BaseModel

_SECRET_KEY = re.compile(
    r"^(?:authorization|credential|password|api[_-]?key|access[_-]?token|"
    r"refresh[_-]?token|device[_-]?code|secret)$",
    re.IGNORECASE,
)
_SECRET_VALUE = re.compile(r"(?i)(bearer\s+|KITKEY_|ZENPROKEY_)[^\s,;\]\}\"']+")


def redact(value: str) -> str:
    """Mask recognizable credentials in a string.

    Args:
        value: Potentially sensitive string.

    Returns:
        Redacted text.
    """
    return _SECRET_VALUE.sub(lambda match: f"{match.group(1)}***", value)


def redact_data(value: Any, *, key: str | None = None) -> Any:
    """Recursively mask secret-like fields.

    Args:
        value: Value to sanitize.
        key: Field name containing the value.

    Returns:
        JSON-compatible sanitized value.
    """
    if key is not None and _SECRET_KEY.search(key):
        return "***"
    if isinstance(value, BaseModel):
        return redact_data(value.model_dump(mode="json"))
    if isinstance(value, dict):
        return {
            str(item_key): redact_data(item, key=str(item_key))
            for item_key, item in value.items()
        }
    if isinstance(value, (list, tuple, set)):
        return [redact_data(item) for item in value]
    if isinstance(value, str):
        return redact(value)
    return _get_json_value(value)


def _get_json_value(value: Any) -> Any:
    """Convert common model values into JSON-compatible values."""
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, (UUID, Path)):
        return str(value)
    return value
