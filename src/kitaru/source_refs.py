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
"""Module attribute reference parsing."""

import re

_MODULE_RE = re.compile(r"^[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*$")
_ATTRIBUTE_RE = re.compile(r"^[A-Za-z_]\w*$")


def parse_source_ref(ref: str) -> tuple[str, str]:
    """Parse a module:attribute reference.

    Args:
        ref: Reference string.

    Raises:
        ValueError: The reference is not exactly one non-empty module and
            one non-empty attribute separated by a single colon.

    Returns:
        Module and attribute.
    """
    module, separator, attribute = ref.partition(":")
    if not separator or not module or not attribute or ":" in attribute:
        raise ValueError(
            f"Invalid source reference '{ref}', expected 'module:attribute'"
        )
    return module, attribute


def parse_python_source_ref(ref: str) -> tuple[str, str]:
    """Parse an importable top-level Python callable reference.

    Args:
        ref: Reference string.

    Raises:
        ValueError: The reference is malformed or contains invalid identifiers.

    Returns:
        Importable module and top-level attribute names.
    """
    module, attribute = parse_source_ref(ref)
    if not _MODULE_RE.fullmatch(module) or not _ATTRIBUTE_RE.fullmatch(attribute):
        raise ValueError(
            f"Invalid source reference '{ref}', expected 'module:attribute'"
        )
    return module, attribute
