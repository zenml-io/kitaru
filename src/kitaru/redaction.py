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
"""Redact recognizable credentials from structured output and diagnostics."""

import re
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any
from uuid import UUID

from pydantic import BaseModel

_SECRET_KEY = re.compile(
    r"(?:^|[_-])(?:api[_-]?keys?|access[_-]?tokens?|refresh[_-]?tokens?|tokens?|"
    r"passwords?|secrets?)$|^(?:authorization|credentials?|device[_-]?codes?|"
    r"client[_-]?secrets?|private[_-]?keys?|secret[_-]?env)$",
    re.IGNORECASE,
)
_SECRET_VALUE = re.compile(r"(?i)(bearer\s+|KITKEY_|ZENPROKEY_)[^\s,;\]\}\"']+")

_TOKEN_USAGE_KEYS = frozenset(
    {"input_tokens", "output_tokens", "cached_input_tokens", "reasoning_tokens"}
)
_MAX_DEPTH = 64


def redact(value: str) -> str:
    """Mask recognizable credentials in a string."""
    return _SECRET_VALUE.sub(lambda match: f"{match.group(1)}***", value)


def redact_data(value: Any, *, key: str | None = None) -> Any:
    """Return bounded JSON-safe output with recognizable credentials masked."""
    try:
        return _redact_data(value, key=key, depth=0, active_ids=set())
    except Exception:
        # Normalization errors can include credentials in their messages.
        return "[unavailable]"


def _redact_data(
    value: Any, *, key: str | None, depth: int, active_ids: set[int]
) -> Any:
    """Normalize one value while retaining key context and active ancestors."""
    if (
        key is not None
        and _SECRET_KEY.search(key)
        and not _is_token_usage(value, key=key)
    ):
        return "***"
    if depth >= _MAX_DEPTH:
        return "[depth limit]"
    if isinstance(value, BaseModel):
        # JSON-mode dumping collapses mixed keys before we can disambiguate them.
        value = value.model_dump(mode="python")
    if isinstance(value, (dict, list, tuple, set)):
        identity = id(value)
        if identity in active_ids:
            return "[cycle]"
        active_ids.add(identity)
        try:
            if isinstance(value, dict):
                reserved = {item_key for item_key in value if isinstance(item_key, str)}
                result: dict[str, Any] = {}
                for item_key, item in value.items():
                    original_key = str(item_key)
                    output_key = original_key
                    if not isinstance(item_key, str):
                        suffix = 2
                        while output_key in reserved or output_key in result:
                            output_key = f"{original_key} [{suffix}]"
                            suffix += 1
                    result[output_key] = _redact_data(
                        item, key=original_key, depth=depth + 1, active_ids=active_ids
                    )
                return result
            return [
                _redact_data(item, key=None, depth=depth + 1, active_ids=active_ids)
                for item in value
            ]
        finally:
            active_ids.remove(identity)
    if isinstance(value, Enum):
        return _redact_data(
            value.value, key=key, depth=depth + 1, active_ids=active_ids
        )
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, (UUID, Path)):
        return redact(str(value))
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, str):
        return redact(value)
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return "[unavailable]"


def _is_token_usage(value: Any, *, key: str) -> bool:
    """Recognize only the numeric token usage fields emitted by the API."""
    if key not in _TOKEN_USAGE_KEYS and key != "tokens":
        return False
    if value is None or (isinstance(value, int) and not isinstance(value, bool)):
        return True
    return (
        key == "tokens"
        and isinstance(value, dict)
        and all(
            field in _TOKEN_USAGE_KEYS
            and (
                count is None
                or (isinstance(count, int) and not isinstance(count, bool))
            )
            for field, count in value.items()
        )
    )
