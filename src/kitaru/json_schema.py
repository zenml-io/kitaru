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
"""JSON Schema property helpers."""

from typing import Any


def is_write_only(definition: dict[str, Any]) -> bool:
    """Report whether a JSON Schema property is marked `writeOnly`.

    Args:
        definition: Property definition.

    Returns:
        Whether the property or one of its union branches is `writeOnly`.
    """
    # Pydantic marks an optional `SecretStr | None` field inside its non-null
    # branch rather than on the property itself.
    branches = [
        branch
        for key in ("anyOf", "oneOf")
        if isinstance(options := definition.get(key), list)
        for branch in options
        if isinstance(branch, dict)
    ]
    return any(item.get("writeOnly") is True for item in (definition, *branches))


def get_write_only_keys(schema: dict[str, Any] | None) -> set[str]:
    """Get the properties of a JSON Schema object marked `writeOnly`.

    Args:
        schema: Object schema, None for no schema.

    Returns:
        Names of the `writeOnly` properties.
    """
    properties = (schema or {}).get("properties")
    if not isinstance(properties, dict):
        return set()
    return {
        key
        for key, definition in properties.items()
        if isinstance(definition, dict) and is_write_only(definition)
    }
