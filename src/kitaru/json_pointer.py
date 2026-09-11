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
"""RFC 6901 JSON Pointer resolution."""

import re
from typing import Any

_INVALID_ESCAPE = re.compile(r"~(?:[^01]|$)")


def _decode_reference_token(token: str) -> str | None:
    """Decode one RFC 6901 reference token, None when its escapes are invalid."""
    if _INVALID_ESCAPE.search(token):
        return None
    return token.replace("~1", "/").replace("~0", "~")


def resolve_json_pointer(document: Any, pointer: str) -> tuple[bool, Any]:
    """Resolve an RFC 6901 JSON Pointer against a decoded JSON document.

    Args:
        document: Document to resolve against.
        pointer: JSON Pointer to resolve.

    Returns:
        Whether the pointer resolved, and the value it selected.
    """
    if pointer == "":
        return True, document
    if not pointer.startswith("/"):
        return False, None
    current = document
    for raw_token in pointer[1:].split("/"):
        token = _decode_reference_token(raw_token)
        if token is None:
            return False, None
        if isinstance(current, dict) and token in current:
            current = current[token]
        elif (
            isinstance(current, list)
            and token.isascii()
            and token.isdigit()
            and (token == "0" or not token.startswith("0"))
            and int(token) < len(current)
        ):
            current = current[int(token)]
        else:
            return False, None
    return True, current
