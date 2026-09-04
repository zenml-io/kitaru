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

# An account mirrors an external identity, whose username is an email address
# under an OAuth2 control plane, so account names carry the email separators.
ACCOUNT_ALLOWED_SEPARATORS = frozenset({"-", "_", ".", "+", "@"})

# An evaluator may emit a qualified display form like "accuracy.v2", so
# evaluation names carry the dot separator alongside the default set.
EVALUATION_ALLOWED_SEPARATORS = frozenset({"-", "_", "."})

# A display version may be a semver string carrying build metadata or a
# branch-style label, so version names carry the dot, plus, and slash
# separators alongside the default set.
VERSION_ALLOWED_SEPARATORS = frozenset({"-", "_", ".", "+", "/"})

# Built-in entities ship under this namespace, reserved so no user-created
# name can collide with one.
RESERVED_NAMESPACE = "kitaru"

NAMESPACE_SEPARATOR = "/"

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
    return _validate(value, allowed_separators, max_length)


def validate_account_name(value: str, max_length: int = MAX_NAME_LENGTH) -> str:
    """Validate an account name, which may be an email address.

    Args:
        value: Name to validate.
        max_length: Maximum name length.

    Raises:
        InvalidName: ``value`` violates the name rules.

    Returns:
        Validated name.
    """
    return _validate(value, ACCOUNT_ALLOWED_SEPARATORS, max_length)


def validate_evaluation_name(value: str, max_length: int = MAX_NAME_LENGTH) -> str:
    """Validate an evaluation name, which allows the dot separator.

    Args:
        value: Name to validate.
        max_length: Maximum name length.

    Raises:
        InvalidName: ``value`` violates the name rules.

    Returns:
        Validated name.
    """
    return _validate(value, EVALUATION_ALLOWED_SEPARATORS, max_length)


def validate_version_name(value: str, max_length: int = MAX_NAME_LENGTH) -> str:
    """Validate a version name, which allows semver and branch-style separators.

    Args:
        value: Name to validate.
        max_length: Maximum name length.

    Raises:
        InvalidName: ``value`` violates the name rules.

    Returns:
        Validated name.
    """
    return _validate(value, VERSION_ALLOWED_SEPARATORS, max_length)


def get_namespace(value: str) -> str | None:
    """Get the namespace of a namespaced name, None when it has none.

    Args:
        value: Namespaced name.

    Returns:
        Namespace.
    """
    namespace, separator, _ = value.partition(NAMESPACE_SEPARATOR)
    return namespace if separator else None


def validate_namespaced_name(value: str, max_length: int = MAX_NAME_LENGTH) -> str:
    """Validate a name with an optional namespace, allowing only the reserved one.

    Args:
        value: Name to validate.
        max_length: Maximum name length.

    Raises:
        InvalidName: ``value`` violates the name rules.

    Returns:
        Validated name.
    """
    namespace, separator, name = value.partition(NAMESPACE_SEPARATOR)
    if not separator:
        return _validate(value, DEFAULT_ALLOWED_SEPARATORS, max_length)
    if namespace != RESERVED_NAMESPACE:
        raise InvalidName(f"Unknown namespace '{namespace}'")
    _validate(name, DEFAULT_ALLOWED_SEPARATORS, max_length - len(namespace) - 1)
    return value


def _validate(value: str, allowed_separators: frozenset[str], max_length: int) -> str:
    """Check a name against a character set.

    Args:
        value: Name to validate.
        allowed_separators: Separator characters permitted inside the name.
        max_length: Maximum name length.

    Raises:
        InvalidName: ``value`` violates the name rules.

    Returns:
        Validated name.
    """
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
AccountName = Annotated[str, AfterValidator(validate_account_name)]
EvaluationName = Annotated[str, AfterValidator(validate_evaluation_name)]
VersionName = Annotated[str, AfterValidator(validate_version_name)]
NamespacedName = Annotated[str, AfterValidator(validate_namespaced_name)]
