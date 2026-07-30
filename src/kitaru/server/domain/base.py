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
"""Shared domain primitives."""

from pydantic import BaseModel, ConfigDict


class DomainModel(BaseModel):
    """Base type for mutable domain entities and value objects."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)


class DomainError(Exception):
    """Base exception for domain rule violations visible to callers."""


class NotFoundError(DomainError):
    """Raised when a lookup does not resolve in the caller's scope."""


class ConflictError(DomainError):
    """Raised when an operation conflicts with existing state."""


class PayloadTooLargeError(DomainError):
    """Raised when a request payload exceeds a size limit."""


class ValidationError(DomainError):
    """Raised when input fails domain shape or invariant checks."""
