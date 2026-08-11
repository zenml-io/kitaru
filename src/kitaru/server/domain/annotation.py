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
"""Annotation entity and errors."""

import uuid
from datetime import datetime
from typing import Any

from pydantic import Field

from kitaru.api_models.v1.annotation import AnnotationSelector
from kitaru.server.domain.base import DomainModel, NotFoundError
from kitaru.server.domain.ids import uuid7


class AnnotationNotFound(NotFoundError):
    """Raised when an annotation lookup does not resolve."""

    def __init__(self, annotation_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            annotation_id: Id of the missing annotation.
        """
        super().__init__(f"Annotation {annotation_id} was not found")


class Annotation(DomainModel):
    """Annotation."""

    id: uuid.UUID = Field(default_factory=uuid7)
    owner_id: uuid.UUID
    session_id: uuid.UUID
    investigation_session_id: uuid.UUID | None = None
    selector: AnnotationSelector | None = None
    value: Any
    created: datetime | None = None
    updated: datetime | None = None

    def update_value(self, value: Any) -> None:
        """Set a new annotation value.

        Args:
            value: New value.
        """
        self.value = value
