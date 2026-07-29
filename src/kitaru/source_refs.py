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
"""Utilities for package plugin source references."""


def parse_source_ref(ref: str) -> tuple[str, str]:
    """Parse a ``module:attribute`` source reference.

    Args:
        ref: Source reference to parse.

    Raises:
        ValueError: The reference is not in ``module:attribute`` form.

    Returns:
        Module and attribute names.
    """
    if ref.count(":") != 1:
        raise ValueError(
            f"Invalid source reference {ref!r}; expected 'module:attribute'."
        )
    module, attribute = ref.split(":")
    if not module or not attribute:
        raise ValueError(
            f"Invalid source reference {ref!r}; expected 'module:attribute'."
        )
    return module, attribute
