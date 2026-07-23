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
"""Restricted entity name validation."""

import re
from typing import Annotated

from pydantic import AfterValidator

from kitaru.server.domain.base import ValidationError

# Separators reserved for system use. No entity name may ever allow them.
RESERVED_SEPARATORS = frozenset({".", "/", ":", "@"})

DEFAULT_ALLOWED_SEPARATORS = frozenset({"-", "_"})

MAX_NAME_LENGTH = 255


class InvalidName(ValidationError):
    """Raised when a name violates the restricted character rules."""


def validate_name(
    value: str,
    allowed_separators: frozenset[str] = DEFAULT_ALLOWED_SEPARATORS,
    max_length: int = MAX_NAME_LENGTH,
) -> str:
    """Validate an entity name against the restricted character set.

    Names contain ASCII letters, digits, and the allowed separators, start
    and end with a letter or digit, and are at most ``max_length`` characters.

    Args:
        value: Name to validate.
        allowed_separators: Separator characters permitted inside the name.
        max_length: Maximum name length.

    Raises:
        ValueError: ``allowed_separators`` contains a reserved separator.
        InvalidName: ``value`` violates the name rules.

    Returns:
        Validated name.
    """
    reserved = allowed_separators & RESERVED_SEPARATORS
    if reserved:
        raise ValueError(f"Separators {sorted(reserved)} are reserved")
    if not value:
        raise InvalidName("Name must not be empty")
    if len(value) > max_length:
        raise InvalidName(f"Name exceeds {max_length} characters")
    separators = re.escape("".join(sorted(allowed_separators)))
    pattern = rf"^[A-Za-z0-9](?:[A-Za-z0-9{separators}]*[A-Za-z0-9])?$"
    if not re.fullmatch(pattern, value):
        allowed = " ".join(sorted(allowed_separators))
        raise InvalidName(
            f"Name '{value}' must contain only letters, digits, and "
            f"{allowed}, and must start and end with a letter or digit"
        )
    return value


Name = Annotated[str, AfterValidator(validate_name)]
