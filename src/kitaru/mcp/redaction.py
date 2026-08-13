#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""MCP-local redaction for protocol results and diagnostics."""

import re
from datetime import date, datetime
from enum import Enum
from pathlib import Path
from typing import Any
from uuid import UUID

from pydantic import BaseModel

_SECRET_KEY = re.compile(
    r"(?:^|_)(?:api_key|access_token|refresh_token|token|password|secret)$|"
    r"^(?:authorization|credential|device_code|client_secret|private_key|"
    r"secret_env)$",
    re.IGNORECASE,
)
_SECRET_VALUE = re.compile(r"(?i)(bearer\s+|KITKEY_|ZENPROKEY_)[^\s,;\]\}\"']+")


def redact(value: str) -> str:
    """Mask recognizable credential values in text."""
    return _SECRET_VALUE.sub(lambda match: f"{match.group(1)}***", value)


def redact_data(value: Any, *, key: str | None = None) -> Any:
    """Recursively mask secret fields and normalize JSON-compatible values."""
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
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, (UUID, Path)):
        return str(value)
    return value
