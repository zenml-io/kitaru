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
"""Read fields from Langfuse records: JSON-encoded values and metadata layers."""

import json
from typing import Any


class InvalidImport(ValueError):
    """Raised when a Langfuse payload cannot be parsed."""


def get_first(record: dict[str, Any], *names: str) -> Any:
    """Return the first present field."""
    for name in names:
        if name in record:
            return record[name]
    return None


def decode_json(value: Any) -> Any:
    """Decode a JSON string while preserving ordinary strings."""
    if not isinstance(value, str):
        return value
    stripped = value.strip()
    if not stripped or stripped[0] not in '[{"':
        return value
    try:
        return json.loads(stripped)
    except RecursionError as exc:
        raise InvalidImport("Nested JSON exceeds the decoding depth limit") from exc
    except json.JSONDecodeError:
        return value
    except ValueError as exc:
        raise InvalidImport("Embedded JSON contains an invalid value") from exc


def decode_dict(value: Any) -> dict[str, Any]:
    """Return a decoded dictionary or an empty one."""
    decoded = decode_json(value)
    return decoded if isinstance(decoded, dict) else {}


def get_metadata_value(record: dict[str, Any], *keys: str) -> Any:
    """Return the first non-empty value from Langfuse metadata layers."""
    metadata = decode_dict(record.get("metadata"))
    attributes = decode_dict(metadata.get("attributes"))
    resource_attributes = decode_dict(metadata.get("resourceAttributes"))
    for key in keys:
        for source in (attributes, metadata, resource_attributes):
            value = source.get(key)
            if value not in (None, ""):
                return value
        for flattened_key in (f"attributes.{key}", f"resourceAttributes.{key}"):
            value = metadata.get(flattened_key)
            if value not in (None, ""):
                return value
    return None
