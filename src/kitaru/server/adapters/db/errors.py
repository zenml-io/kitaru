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
"""Database error introspection."""

import asyncpg
from sqlalchemy.exc import IntegrityError


def violated_constraint(exc: IntegrityError) -> str | None:
    """Return the name of the constraint an integrity error violated.

    Args:
        exc: Integrity error raised by a flush.

    Returns:
        Constraint name when the driver reports one, otherwise ``None``.
    """
    cause = exc.orig.__cause__ if exc.orig is not None else None
    if isinstance(cause, asyncpg.exceptions.IntegrityConstraintViolationError):
        # The driver generates this attribute dynamically, invisible to ty.
        return cause.constraint_name  # ty: ignore[unresolved-attribute]
    return None
